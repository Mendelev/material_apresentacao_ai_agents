import os
import psycopg2
from psycopg2 import sql
import pandas as pd
from loguru import logger
from dotenv import load_dotenv

# load_dotenv()  # se precisar

PG_HOST = os.getenv("PG_HOST", "192.168.1.7") # Mantive o seu IP
PG_PORT = os.getenv("PG_PORT", "5433")
PG_DATABASE = os.getenv("PG_DATABASE", "database-teste2")
PG_USER = os.getenv("PG_USER", "myuser")
PG_PASSWORD = os.getenv("PG_PASSWORD", "mypassword")


def infer_column_type(series: pd.Series) -> str:
    """
    Infere se os valores da coluna são text, numeric (para floats) ou bigint (para inteiros).
    """
    # Esta função não precisa de alterações
    sample = series.dropna().astype(str)
    if len(sample) == 0:
        return "TEXT"

    def can_be_int(x: str) -> bool:
        x2 = x.strip().replace(",", "")
        try:
            int(x2)
            return True
        except ValueError:
            return False

    def can_be_float(x: str) -> bool:
        x2 = x.strip().replace(",", "")
        try:
            float(x2)
            return True
        except ValueError:
            return False

    all_int = all(can_be_int(val) for val in sample)
    if all_int:
        # Alterado para INT para ser mais compatível que BIGINT em alguns casos.
        return "INT" 

    all_float = all(can_be_float(val) for val in sample)
    if all_float:
        return "NUMERIC"

    return "TEXT"


# --- FUNÇÃO ALTERADA PARA SER MAIS GENÉRICA ---
def create_table_if_not_exists(cur, table_name, df, primary_key_col=None, foreign_key_info=None):
    """
    Cria a tabela no Postgres com tipos inferidos.
    Adiciona a constraint de chave primária na coluna especificada em 'primary_key_col'.
    Adiciona a foreign key se 'foreign_key_info' for especificado.
    'foreign_key_info' deve ser um dict: {'col': 'nome_col', 'ref_table': 'tabela_ref'}
    """
    columns_def = []
    for col in df.columns:
        col_clean = col.replace(".", "_").replace(" ", "_")
        # A inferência de tipo agora se aplica a todas as colunas igualmente
        col_type = infer_column_type(df[col])
        
        column_definition = f'"{col_clean}" {col_type}'
        
        # Se esta for a coluna de chave primária, adiciona a constraint
        if primary_key_col and col == primary_key_col:
            column_definition += " PRIMARY KEY"
        
        columns_def.append(column_definition)
    
    columns_def_str = ", ".join(columns_def)
    
    query = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns_def_str}'
    
    # Adiciona a constraint de chave estrangeira se especificado
    if foreign_key_info:
        fk_col = foreign_key_info['col']
        ref_table = foreign_key_info['ref_table']
        # Assume que a coluna de referência na outra tabela tem o mesmo nome
        query += f', CONSTRAINT fk_{fk_col.lower()} FOREIGN KEY ("{fk_col}") REFERENCES "{ref_table}" ("{fk_col}")'
    
    query += ');'
    
    cur.execute(query)


def clean_numeric(value):
    """
    Remove vírgulas de valores numéricos e os converte em float, se aplicável.
    """
    # Esta função não precisa de alterações
    if isinstance(value, str):
        value = value.replace(",", "")
    try:
        return float(value) if value else None
    except ValueError:
        return None


def insert_data(cur, table_name, df, chunk_size=100):
    """
    Insere linha a linha e lida com erros de violação de chaves.
    """
    # Esta função já lida com ForeignKeyViolation, então não precisa de alterações.
    col_names = [col.replace(".", "_").replace(" ", "_") for col in df.columns]
    placeholders = ", ".join(["%s"] * len(col_names))
    columns_str = ", ".join(f'"{c}"' for c in col_names)
    insert_sql = f'INSERT INTO "{table_name}" ({columns_str}) VALUES ({placeholders})'

    records = []
    for _, row in df.iterrows():
        row_values = []
        for val in row:
            if pd.isna(val):
                row_values.append(None)
            else:
                if isinstance(val, str) and ',' in val:
                    row_values.append(clean_numeric(val))
                else:
                    row_values.append(val)
        records.append(tuple(row_values))

    conn = cur.connection
    old_autocommit = conn.autocommit
    conn.autocommit = True

    for record in records:
        try:
            cur.execute(insert_sql, record)
        except psycopg2.errors.UniqueViolation as e:
            logger.warning(f"Descartando linha {record} por violação de chave primária (duplicada): {e}")
            continue
        except psycopg2.errors.NumericValueOutOfRange as e:
            logger.warning(f"Descartando linha {record} por exceder range numérico: {e}")
            continue
        except psycopg2.errors.InvalidTextRepresentation as e:
            logger.warning(f"Erro de formato de texto na linha {record}: {e}")
            continue
        except psycopg2.errors.ForeignKeyViolation as e:
            logger.warning(f"Descartando linha {record} por violação de chave estrangeira: {e}")
            continue

    conn.autocommit = old_autocommit


# --- FUNÇÃO MAIN ALTERADA PARA A NOVA ESTRUTURA ---
def main():
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        options="-c client_encoding=UTF8"
    )
    cur = conn.cursor()

    # Estrutura de configuração mais clara e flexível
    csvs_to_process = [
        {
            "path": "csv_output/us_precipitation.csv",
            "primary_key": "id",  # A coluna 'id' será a chave primária
            "foreign_key": None   # Esta tabela não tem chave estrangeira
        },
    ]

    for config in csvs_to_process:
        file_path = config["path"]
        primary_key = config.get("primary_key")
        foreign_key = config.get("foreign_key")

        table_name = os.path.basename(file_path).replace(".csv", "")
        logger.info(f"Processando CSV {file_path} -> Tabela {table_name}")
        df = pd.read_csv(file_path)

        # Cria a tabela (se não existir) usando a nova configuração
        create_table_if_not_exists(cur, table_name, df, primary_key_col=primary_key, foreign_key_info=foreign_key)
        conn.commit()

        # Insere dados
        insert_data(cur, table_name, df)
        logger.info(f"Inserção concluída para {table_name}.")

    cur.close()
    conn.close()
    logger.success(
        "Todos os CSVs foram processados com sucesso."
    )


if __name__ == "__main__":
    main()