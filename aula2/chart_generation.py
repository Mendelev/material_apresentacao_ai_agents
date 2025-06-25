# chart_generation.py

import os
import json
import pandas as pd
from openai import AzureOpenAI
import google.generativeai as genai
from textwrap import dedent
from dotenv import load_dotenv
import logging
from utils import clean_llm_output

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


load_dotenv()


def generate_auto_chart(df: pd.DataFrame, question: str) -> dict:
    """
    Usa a LLM para sugerir um gráfico Plotly (JSON) a partir do DataFrame e da pergunta do usuário.
    Retorna um dicionário Python no formato figure Plotly.
    """

    # Para não mandar um DF gigantesco para a LLM, você pode limitar a:
    sample_df_str = df.head(50).to_json(orient="records")  # Ex: 50 linhas no máximo

    # Prompt que instrui o modelo a responder *apenas* em JSON
    # com a estrutura 'figure' para Plotly. Ajuste conforme sua necessidade.
    prompt = f"""
    Você é um assistente de dados especialista em gerar visuais para o Dash Chart Editor (Plotly).
    Recebe uma pergunta do usuário e um subconjunto de dados (em formato JSON).
    Sua tarefa é:
    1. Identificar qual tipo de gráfico é mais apropriado (ex: bar, line, scatter, pie, etc).
    2. Montar um dicionário JSON válido que represente o `figure` no estilo Plotly (sem o 'layout' se quiser simplificar).
       - Use a seguinte estrutura mínima:
            {{
                "data": [
                    {{
                        "type": "<tipo_de_grafico>",
                        "x": [...],
                        "y": [...],
                        "name": "Series Name",
                        "marker": {{"color": "blue"}}
                    }}
                ],
                "layout": {{
                    "title": "<Título do gráfico>",
                    "xaxis": {{"title": "<nome eixo X>"}},
                    "yaxis": {{"title": "<nome eixo Y>"}}
                }}
            }}
    
    3. Não inclua nada fora do JSON, nem explicações, só o objeto JSON!

    Pergunta do usuário: {question}
    Abaixo, um subconjunto do DataFrame em JSON:
    {sample_df_str}

    IMPORTANTÍSSIMO:
    - Sua resposta deve ser APENAS o JSON do dicionário Plotly, sem markdown ou texto adicional.
    - Não use crases, nem aspas simples fora do lugar. Responda unicamente com JSON válido.
    - não use ```json se fizer isso, vai dar erro
    """

    # --- LÓGICA DE SELEÇÃO DO PROVEDOR ---
    provider = os.getenv("LLM_PROVIDER", "openai").lower().strip()
    content = "" # Inicializa a variável de conteúdo

    try:
        if provider == "gemini":
            logger.info("Gerando gráfico com Gemini")
            genai.configure(api_key=os.getenv("GOOGLE_API_KEY", "").strip())
            model = genai.GenerativeModel('gemini-2.0-flash')
            response = model.generate_content(dedent(prompt))
            # A resposta do Gemini é extraída diretamente do atributo .text
            content = response.text
            
        elif provider == "openai":
            logger.info("Gerando gráfico com Azure OpenAI")
            client = AzureOpenAI(
                azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT", "").strip(),
                api_key=os.getenv("OPENAI_API_KEY", "").strip(),
                api_version=os.getenv("OPENAI_API_VERSION", "").strip(),
            )
            response = client.chat.completions.create(
                model=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "").strip(),
                messages=[{"role": "user", "content": dedent(prompt)}],
                temperature=0.3,
            )
            # A resposta do OpenAI está aninhada dentro de choices
            content = response.choices[0].message.content
        else:
            raise ValueError(f"Provedor de LLM '{provider}' inválido.")

    except Exception as e:
        print("Erro na chamada à API da LLM:", e)
        return {}
    cleaned_content = clean_llm_output(content)
    # O resto da função para processar o JSON 'content' continua igual
    try:
        figure_dict = json.loads(cleaned_content)
        return figure_dict
    except json.JSONDecodeError:
        print("A LLM não retornou JSON válido. Resposta foi:", cleaned_content)
        return {}

