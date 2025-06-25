import os
import logging
import json
import re
import pandas as pd
from decimal import Decimal
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from utils import clean_llm_output

from langchain_community.utilities.sql_database import SQLDatabase
from langchain.chains import create_sql_query_chain
from langchain_openai import AzureChatOpenAI 
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser

from table_descriptions import TABLE_DESCRIPTIONS

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- LÓGICA DE SELEÇÃO DO PROVEDOR DE LLM ---
provider = os.getenv("LLM_PROVIDER", "openai").lower().strip()
llm = None

if provider == "gemini":
    logger.info("Usando o provedor: Gemini")
    llm = ChatGoogleGenerativeAI(
        model="gemini-2.0-flash",  # Ou outro modelo, como gemini-pro
        google_api_key=os.getenv("GOOGLE_API_KEY", "").strip(),
        temperature=0,
        # O parâmetro abaixo ajuda na compatibilidade com alguns prompts do LangChain
        convert_system_message_to_human=True 
    )
elif provider == "openai":
    logger.info("Usando o provedor: Azure OpenAI")
    azure_deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "").strip()
    if not azure_deployment:
        logger.error("A variável AZURE_OPENAI_DEPLOYMENT_NAME não foi definida no arquivo .env")
    
    llm = AzureChatOpenAI(
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT", "").strip(),
        api_key=os.getenv("OPENAI_API_KEY", "").strip(),
        api_version=os.getenv("OPENAI_API_VERSION", "").strip(),
        azure_deployment=azure_deployment,
        temperature=0,
    )
else:
    raise ValueError(f"Provedor de LLM '{provider}' desconhecido. Escolha 'openai' ou 'gemini' no arquivo .env.")

# Adicione uma verificação para garantir que o LLM foi inicializado
if not llm:
    raise ConnectionError("Falha ao inicializar o cliente LLM. Verifique suas variáveis de ambiente e o LLM_PROVIDER.")

db_connection_string = os.getenv("DB_CONNECTION_STRING", "")
if not db_connection_string:
    logger.error("DB_CONNECTION_STRING está vazio ou não definido.")
engine = create_engine(db_connection_string)
Session = sessionmaker(bind=engine)


def sample_table_data(table_name: str, limit: int = 5) -> list:
    """
    Retorna até 'limit' linhas do table_name em forma de dicionários.
    """
    with Session() as session:
        query_str = f'SELECT * FROM "{table_name}" LIMIT {limit}'
        res = session.execute(text(query_str))
        rows = res.fetchall()
        col_names = res.keys()
    # Converte cada row em dict {col: value, ...}
    data = []
    for row in rows:
        row_dict = {}
        for col, val in zip(col_names, row):
            # se val for Decimal, converta
            if isinstance(val, Decimal):
                val = float(val)
            row_dict[col] = val
        data.append(row_dict)
    return data

def sample_all_tables(limit=5) -> dict:
    """
    Para cada tabela do schema public,
    pega uma amostra (até 'limit' linhas),
    retorna dict {table_name: [ {col: val, ...}, ... ]}.
    """
    with Session() as session:
        result = session.execute(text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='public'
            ORDER BY table_name
        """))
        table_rows = result.fetchall()
    table_samples = {}
    for (tbl_name,) in table_rows:
        data = sample_table_data(tbl_name, limit)
        table_samples[tbl_name] = data
    return table_samples

def gather_relevant_table_samples(reflection_data: dict, limit: int = 5) -> dict:
    """
    Lê reflection_data["columns_to_check"] e extrai 
    os nomes de tabela. Para cada tabela relevante,
    gera até 'limit' amostras (linhas).
    Retorna dict {table_name: [ {col: val, ...}, ... ]}.
    """
    relevant_tables = set()
    for col_str in reflection_data.get("columns_to_check", []):
        if "." in col_str:
            tname, _ = col_str.split(".", 1)
            relevant_tables.add(tname)

    table_samples = {}
    with Session() as session:
        for tname in relevant_tables:
            data = sample_table_data(tname, limit)  # reusa sua func sample_table_data
            table_samples[tname] = data

    return table_samples




##############################################################################
# 1) Reflection
##############################################################################
reflection_prompt = PromptTemplate.from_template(
    """
    Você é um assistente de dados. 
    O usuário perguntou: {question}

    Aqui está o schema real do banco:
    {schema}

    (Possíveis descrições de tabelas):
    {extra_desc}

    Abaixo, um pequeno subconjunto de dados das tabelas (limitado a 10 linhas cada):
    {samples}


    - Liste colunas que você gostaria de conhecer DISTINCT (LIMIT 30) 
      para entender melhor os valores disponíveis.
    - Se o usuário mencionar algo como molhado e houver uma coluna que mencione
      precipitação, você pode inferir que chuva se refere a molhado e verificá-la.
    - Use as descrições da tabela para tentar associar quais colunas são pertinentes
      a perguntas do usuário.
    - Se a pergunta mencionar colunas de mais de uma tabela, planeje usar um JOIN.
    - Se não precisar de nenhuma, retorne vazio.

    Se você encontrar nomes diferentes no table_descriptions (ex.: "País de Origen") 
    e no schema real (ex.: "Pais_de_Origen"), entenda que eles se referem à mesma coluna, 
    mas use exatamente "Pais_de_Origen" ao gerar a query.

    Responda no formato JSON (use chaves duplas):
    {{
       "columns_to_check": ["tabela.coluna", "outra.coluna", ...],
       "justification": "Explique..."
    }}
    """
)

def get_db_schema():
    with Session() as session:
        result = session.execute(text("""
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema='public'
            ORDER BY table_name, ordinal_position
        """))
        rows = result.fetchall()
    table_dict = {}
    for (tbl, col) in rows:
        if tbl not in table_dict:
            table_dict[tbl] = []
        table_dict[tbl].append(col)
    schema_str = ""
    for tbl, cols in table_dict.items():
        schema_str += f"- Tabela: {tbl}\n  Colunas: {', '.join(cols)}\n"
    return schema_str

def get_table_descriptions():
    desc_text = "\n\n".join(
        f"[{name}]\n{desc}" for name, desc in TABLE_DESCRIPTIONS.items()
    )
    return desc_text

def reflect_before_query(question: str):
    samples = sample_all_tables(limit=3)
    samples_json = json.dumps(samples, ensure_ascii=False)
    schema = get_db_schema()
    extras = get_table_descriptions()
    prompt_text = reflection_prompt.format(
        question=question,
        schema=schema,
        extra_desc=extras,
        samples=samples_json
    )
    logger.info("[Reflection Step] => %s", prompt_text)

    response = llm.invoke(prompt_text).content.strip()
    logger.info("[Reflection Step] LLM raw => %s", response)

    cleaned_response = clean_llm_output(response)

    try:
        data = json.loads(response)
        return data
    except json.JSONDecodeError:
        logger.warning("Falha ao parsear reflection JSON.")
        return {"columns_to_check": [], "justification": response}

##############################################################################
# 2) fetch_distinct_values
##############################################################################
def fetch_distinct_values(cols_to_check):
    logger.info("[Fetch Distinct] => %s", cols_to_check)
    distinct_dict = {}
    with Session() as session:
        for tc in cols_to_check:
            if "." not in tc:
                continue
            table_name, col_name = tc.split(".", 1)
            query_str = f'SELECT DISTINCT "{col_name}" FROM "{table_name}" LIMIT 30'
            logger.info("[Fetch Distinct] Query => %s", query_str)
            try:
                res = session.execute(text(query_str))
                rows = res.fetchall()
                converted = []
                for row in rows:
                    v = row[0]
                    if isinstance(v, Decimal):
                        v = float(v)
                    converted.append(v)
                distinct_dict[tc] = converted
            except SQLAlchemyError as e:
                logger.warning("Erro DISTINCT p/ %s => %s", tc, e)
                distinct_dict[tc] = []
    return distinct_dict

##############################################################################
# 2.1) aggregator_hint
##############################################################################
def aggregator_hint(reflection_data):
    if reflection_data.get("need_aggregation"):
        return (
            "Observação: o usuário perguntou 'quantos' ou algo similar, "
            "logo prefira usar funções de agregação (SUM(...) ou COUNT(...)). "
            "Ao falar de 'estoque', considere que a tabela ZMM038 define 'Disponible excluyendo pedidos' "
            "como a coluna de quantidade real de estoque. Não apenas conte SKUs."
        )
    return ""

##############################################################################
# 3) generate_final_sql
##############################################################################
final_prompt = PromptTemplate.from_template(
    """
    Você é um assistente de dados.
    Pergunta do usuário: {question}

    Análise prévia (colunas distinct etc.):
    {reflection_data}

    Distinct values (LIMIT 30) obtidos:
    {distinct_info}

    Eis o schema real (com colunas exatas):
    {schema}

    {aggregator_hint}

    Aqui estão algumas linhas amostradas de cada tabela:
    {samples}

    **Instruções**:
    1) Sempre use aspas duplas separadas p/ tabela e coluna (ex: "Tabela"."Coluna"), 
       jamais "Tabela.Coluna".
    2) Não invente colunas que não existam.
    3) Se for estoque, some a coluna 'Disponible excluyendo pedidos' se for o total.
    4) Somente retorne a SQL, sem explicações ou ```sql```.
    5) Se usar LIKE, prefira ILIKE e se precisar, coloque o texto entre %.
    6) Selecione apenas as colunas necessárias.
    7) Se houver discrepância entre 'table_descriptions' e 'schema', use o 'schema' real.
    8) Se a pergunta refere colunas de **mais de uma tabela** ou se a 
       "table_descriptions" diz que X é relacionado a Y, **você DEVE** usar JOIN 
       explicitamente. NÃO coloque todas as colunas numa mesma tabela se não existirem nela.
    9) Se existirem relacionamentos mencionados (ex: "SKU" em Tabela A é igual a "SKU" em Tabela B),
       use "TabelaA"."SKU" = "TabelaB"."SKU" como condição de JOIN.
    10) Jamais retorne colunas que não existem na tabela. Se col X está em Tabela A e col Y em Tabela B,
        faça JOIN, usando as colunas e nomes exatos do schema real.

    ATENÇÃO:
    - Se o nome de uma coluna no “table_descriptions” não bater com o real no "schema real" que você estudou,
      **sempre** use o nome que aparece no schema real do banco.
    - Quando houver conflito, o schema real é a fonte de verdade.
    """
)


def generate_final_sql(question, reflection_data, distinct_data):
    schema = get_db_schema()
    reflection_json = json.dumps(reflection_data, indent=2, ensure_ascii=False)
    distinct_json = json.dumps(distinct_data, indent=2, ensure_ascii=False)
    hint = aggregator_hint(reflection_data)
    samples = sample_all_tables(limit=3)
    samples_json = json.dumps(samples, ensure_ascii=False)

    prompt_text = final_prompt.format(
        question=question,
        reflection_data=reflection_json,
        distinct_info=distinct_json,
        schema=schema,
        aggregator_hint=hint,
        samples=samples_json
    )
    logger.info("[Final Prompt] => %s", prompt_text)

    resp = llm.invoke(prompt_text).content.strip()
    logger.info("[Final Query LLM raw] => %s", resp)
    cleaned_resp = clean_llm_output(resp)
    return cleaned_resp

##############################################################################
# 4) Execução + Retry
##############################################################################
def execute_sql_query(query: str):
    logger.info("[Execute SQL] => %s", query)
    with Session() as s:
        try:
            res = s.execute(text(query))
            rows = res.fetchall()
            cols = res.keys()
            return rows, cols, ""  # sem erro
        except SQLAlchemyError as e:
            logger.error("Erro no SQL => %s", e)
            return [], [], str(e)  # retorna a msg de erro

def generate_alternative_sql_query(
    question: str,
    original_query: str,
    reflection_data: dict,
    distinct_data: dict,
    schema: str,
    error_msg: str  # agora passamos a msg de erro
):
    reflection_str = json.dumps(reflection_data, indent=2, ensure_ascii=False)
    distinct_str = json.dumps(distinct_data, indent=2, ensure_ascii=False)
    hint = aggregator_hint(reflection_data)

    prompt_alt = f"""
A query a seguir falhou ou não retornou nada:

Pergunta: {question}
Query: {original_query}
Erro: {error_msg}

Schema:
{schema}
Distinct Info:
{distinct_str}

{hint}

Regras:
1) Sempre use aspas duplas ("Tabela"."Coluna").
   Jamais use "Tabela.Coluna" ou 'Tabela.Coluna', pois você receberá erros.
2) Se for estoque total, some 'Disponible excluyendo pedidos', não conte SKUs.
3) Se for erro de sintaxe, corrija com base na msg acima.
4) Retorne SOMENTE a nova consulta SQL, nada de ```sql``` etc.
"""
    alt_resp = llm.invoke(prompt_alt).content.strip()
    logger.info("[Alternative Query raw] => %s", alt_resp)
    return alt_resp


def is_all_null(rows):
    if not rows:
        return True
    return all(all(v is None for v in r) for r in rows)

def execute_sql_query_with_retry(
    question: str,
    query: str,
    reflection_data: dict,
    distinct_data: dict,
    schema: str,
    max_retries=2
):
    current_q = query
    attempts = 0
    while attempts < max_retries:
        rows, cols, error_msg = execute_sql_query(current_q)
        
        # 1) Se houve erro de sintaxe ou outra falha
        if error_msg:
            logger.info("SQL falhou com erro. Tentando query alternativa.")
            alt_q = generate_alternative_sql_query(
                question, 
                current_q, 
                reflection_data, 
                distinct_data, 
                schema, 
                f"Erro de sintaxe ou similar: {error_msg}"
            )
            if alt_q:
                current_q = alt_q
                attempts += 1
            else:
                return [], []
            continue  # tenta novamente

        # 2) Se rodou sem erro mas 0 rows ou all null
        if not rows or is_all_null(rows):
            logger.info("No results. Tentando query alternativa.")
            alt_q = generate_alternative_sql_query(
                question, 
                current_q, 
                reflection_data, 
                distinct_data, 
                schema, 
                "No results or all null"
            )
            if alt_q:
                current_q = alt_q
                attempts += 1
            else:
                return [], []
        else:
            # sucesso
            return rows, cols
    # se chegou aqui, falhou
    return [], []


##############################################################################
# 5) Resposta curta
##############################################################################
short_answer_prompt = PromptTemplate.from_template(
    """
    Pergunta: {question}
    Query: {query}
    Resultado (linhas):
    {result_str}

    Regras:
    - Se multiplas linhas/colunas, liste enumerado
    - Se 1 linha e 1 col, retorne direto
    - Sem explicações
    """
)

def rephrase_result_short(question, query, rows, cols):
    if not rows:
        result_str = "Nenhum resultado."
    else:
        lines = []
        for i, row in enumerate(rows, start=1):
            if len(cols) > 1:
                row_txt = ", ".join(f"{col}={row[idx]}" for idx, col in enumerate(cols))
                lines.append(f"{i}) {row_txt}")
            else:
                lines.append(f"{i}) {row[0]}")
        result_str = "\n".join(lines)
    ptxt = short_answer_prompt.format(
        question=question, query=query, result_str=result_str
    )
    chain = llm | StrOutputParser()
    ans = chain.invoke(ptxt)
    return ans

##############################################################################
# 6) Justificativa no Log
##############################################################################
explanation_prompt = PromptTemplate.from_template(
    """
    Pergunta: {question}
    Query: {query}
    Resultado: {result_str}

    Explique em no máximo 2 parágrafos como chegou a essa resposta.
    Cite colunas ou filtros usados.
    """
)

def log_explanation(question, query, rows, cols):
    if not rows:
        explanation = "Nenhum resultado."
    else:
        lines = []
        for i, row in enumerate(rows):
            line = ", ".join(str(x) for x in row)
            lines.append(line)
        result_str = "\n".join(lines)
        ptxt = explanation_prompt.format(question=question, query=query, result_str=result_str)
        chain = llm | StrOutputParser()
        explanation = chain.invoke(ptxt)
    logger.info("[Justificativa no Log] => %s", explanation)

##############################################################################
# 7) ReAct-style verificação
##############################################################################
verifier_prompt = PromptTemplate.from_template(
    """
    O usuário perguntou: {question}
    A query usada foi: {query}
    Aqui está a explicação que você deu sobre como chegou a resposta:
    {explanation}

    Resposta obtida: {answer_text}

    Reflita:
    - Isso realmente atende a intenção do usuário?
    - Se o usuário pediu quantidade total e você só contou SKUs, isso está incorreto?
    - Se sim, gere uma NOVA query corrigida. Caso contrário, diga "aceito".

    Responda em JSON sem usar ```json:
    {{
      "verdict": "aceito" ou "corrigir",
      "new_query": "se for para corrigir, coloque aqui a SQL"
    }}
    """
)

def verify_and_correct_query(question, final_sql, explanation_text, answer_text):
    """
    LLM re-checa se a query/explicação atende a pergunta.
    Se não, gera uma nova query.
    """
    ptxt = verifier_prompt.format(
        question=question,
        query=final_sql,
        explanation=explanation_text,
        answer_text=answer_text
    )
    resp = llm.invoke(ptxt).content.strip()
    logger.info("[Verifier RAW] => %s", resp)
    try:
        data = json.loads(resp)
        return data
    except json.JSONDecodeError:
        return {"verdict": "aceito", "new_query": ""}

##############################################################################
# 8) run_query_and_summarize
##############################################################################
def run_query_and_summarize(question: str) -> tuple[str, pd.DataFrame]:
    logger.info("[run_query_and_summarize] => %s", question)

    # (1) Reflection
    reflection_data = reflect_before_query(question)

    # se a pergunta for 'quantos', aggregator
    if any(k in question.lower() for k in ["quantos", "quanto estoque", "how many"]):
        reflection_data["need_aggregation"] = True

    cols_to_check = reflection_data.get("columns_to_check", [])
    # (2) Distinct
    distinct_data = {}
    if cols_to_check:
        distinct_data = fetch_distinct_values(cols_to_check)

    # (3) Gera SQL
    final_sql = generate_final_sql(question, reflection_data, distinct_data)

    # (4) Exec c/ retry
    rows, columns = execute_sql_query_with_retry(
        question, final_sql, reflection_data, distinct_data, get_db_schema()
    )
    if not rows:
        return "Nenhum resultado encontrado.", pd.DataFrame()

    df = pd.DataFrame(rows, columns=columns)

    # (5) Log e obter explicacao
    # Em vez de so logar, pegamos a explicacao para ver se LLM fez count SKUs
    lines = []
    for r in rows:
        lines.append(", ".join(str(x) for x in r))
    result_str = "\n".join(lines)

    # gera explicacao
    ptxt = explanation_prompt.format(
        question=question, query=final_sql, result_str=result_str
    )
    chain = llm | StrOutputParser()
    explanation = chain.invoke(ptxt)
    logger.info("[Justificativa no Log] => %s", explanation)

    # (6) Gera resposta curta
    answer_text = rephrase_result_short(question, final_sql, rows, columns)
    logger.info("[Resposta final] => %s", answer_text)

    # (7) Fase ReAct: verificador
    # max 2 loops
    new_sql = final_sql
    for i in range(2):
        verdict_data = verify_and_correct_query(
            question, new_sql, explanation, answer_text
        )
        if verdict_data.get("verdict") == "corrigir" and verdict_data.get("new_query"):
            # executa nova query
            new_sql = verdict_data["new_query"].strip()
            logger.info("[Verifier] Nova Query => %s", new_sql)
            # executa
            rows2, cols2, error_msg2  = execute_sql_query(new_sql)
            if not rows2:
                logger.info("Nova query nao retornou nada, paramos.")
                break
            # geramos nova explicacao
            lines2 = []
            for r2 in rows2:
                lines2.append(", ".join(str(x) for x in r2))
            res2_str = "\n".join(lines2)
            # explicacao
            ptxt2 = explanation_prompt.format(
                question=question, query=new_sql, result_str=res2_str
            )
            explanation2 = chain.invoke(ptxt2)
            # nova answer
            answer_text2 = rephrase_result_short(question, new_sql, rows2, cols2)
            logger.info("[Resposta ReAct Final] => %s", answer_text2)
            # substitui final
            explanation = explanation2
            answer_text = answer_text2
            df = pd.DataFrame(rows2, columns=cols2)
        else:
            logger.info("[Verifier] => aceito. Nao precisa corrigir.")
            break

    return answer_text, df
