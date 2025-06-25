import os
import psycopg2
from psycopg2 import sql
import pandas as pd
from loguru import logger
from dotenv import load_dotenv

# load_dotenv()  # se precisar

PG_HOST = os.getenv("PG_HOST", "192.168.1.7")
PG_PORT = os.getenv("PG_PORT", "5433")
PG_DATABASE = os.getenv("PG_DATABASE", "database-teste")
PG_USER = os.getenv("PG_USER", "myuser")
PG_PASSWORD = os.getenv("PG_PASSWORD", "mypassword")

max_lines = 100

def infer_column_type(series: pd.Series) -> str:
    """
    Infere se os valores da coluna são text, numeric (para floats) ou bigint (para inteiros).
    """
    sample = series.dropna().astype(str)
    if len(sample) == 0:
        return "TEXT"

    def can_be_int(x: str) -> bool:
        x2 = x.strip().replace(",", "")
        try:
            val = int(x2)
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

    # Se todos podem ser int, retornamos BIGINT
    all_int = all(can_be_int(val) for val in sample)
    if all_int:
        return "INT"

    # Caso contrário, se todos podem ser float, retornamos NUMERIC
    all_float = all(can_be_float(val) for val in sample)
    if all_float:
        return "NUMERIC"

    # Senão, TEXT
    return "TEXT"

def create_table_if_not_exists(cur, table_name, df):
    """
    Cria a tabela no Postgres com tipos inferidos nas colunas (TEXT, NUMERIC ou BIGINT).
    """
    columns_def = []
    for col in df.columns:
        col_clean = col.replace(".", "_").replace(" ", "_")
        col_type = "TEXT" if col == "SKU" else infer_column_type(df[col])        
        columns_def.append(f'"{col_clean}" {col_type}')

    columns_def_str = ", ".join(columns_def)
    query = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns_def_str});'
    cur.execute(query)

def clean_numeric(value):
    """
    Remove vírgulas de valores numéricos e os converte em float, se aplicável.
    """
    if isinstance(value, str):
        value = value.replace(",", "")
    try:
        return float(value) if value else None
    except ValueError:
        return None

def insert_data(cur, table_name, df, chunk_size=100):
    """
    Insere linha a linha (em autocommit) e descarta qualquer linha que causar NumericValueOutOfRange.
    """
    col_names = [col.replace(".", "_").replace(" ", "_") for col in df.columns]
    placeholders = ", ".join(["%s"] * len(col_names))
    columns_str = ", ".join(f'"{c}"' for c in col_names)
    insert_sql = f'INSERT INTO "{table_name}" ({columns_str}) VALUES ({placeholders})'

    # Converte o DataFrame em lista de tuplas
    records = []
    for _, row in df.iterrows():
        row_values = []
        for val in row:
            if pd.isna(val):
                row_values.append(None)
            else:
                if isinstance(val, str) and ',' in val:  # Tratamento para valores numéricos com vírgulas
                    row_values.append(clean_numeric(val))
                else:
                    row_values.append(val)
        records.append(tuple(row_values))

    # Habilita autocommit para que cada linha seja uma transação isolada
    conn = cur.connection
    old_autocommit = conn.autocommit
    conn.autocommit = True

    # Faz a inserção linha a linha
    for record in records:
        try:
            cur.execute(insert_sql, record)
        except psycopg2.errors.NumericValueOutOfRange as e:
            logger.warning(f"Descartando linha {record} por exceder range numérico: {e}")
            continue
        except psycopg2.errors.InvalidTextRepresentation as e:
            logger.warning(f"Erro de formato de texto na linha {record}: {e}")
            continue

    # Restaura o valor antigo do autocommit
    conn.autocommit = old_autocommit

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

    # Lista de CSVs a processar
    csvs = ["csv_output/us_precipitation.csv"] 

    for file_path in csvs:
        table_name = os.path.basename(file_path).replace(".csv", "")
        logger.info(f"Processando CSV {file_path} -> Tabela {table_name}")
        df = pd.read_csv(file_path)

        if max_lines:
            df = df.head(max_lines)

        # Cria tabela (se não existir)
        create_table_if_not_exists(cur, table_name, df)
        conn.commit()

        # Insere dados, descartando linhas que derem NumericValueOutOfRange
        insert_data(cur, table_name, df)
        logger.info(f"Inserção concluída para {table_name}.")

    cur.close()
    conn.close()
    logger.success("Todos os CSVs foram processados com sucesso, descartando linhas fora do range quando necessário.")

if __name__ == "__main__":
    main()
