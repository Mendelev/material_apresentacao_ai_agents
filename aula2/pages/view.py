import json
import pickle

import dash
import dash_bootstrap_components as dbc
import dash_mantine_components as dmc
import os
from dash import dcc, html
from dotenv import load_dotenv

from constants import redis_instance
from openai import AzureOpenAI
import google.generativeai as genai
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

dash.register_page(__name__)


def layout(layout=None):
    layout = redis_instance.get(layout)
    layout = pickle.loads(layout)

    figures = [i["props"]["children"][0]["props"]["figure"] for i in layout[1:]]

    question = (
        "The following is a Plotly Dash layout with several charts. Summarize "
        "the charts for me and provide some maximums, mimumuns, trends, "
        "notable outliers, etc. Describe the data and content as the user doesn't know it's a layout."
        "The data may be truncated to comply with a max character count. "
        f"There should be {len(figures)} charts to follow:\n\n\n"
    )

    # --- LÓGICA DE SELEÇÃO DO PROVEDOR ---
    provider = os.getenv("LLM_PROVIDER", "openai").lower().strip()
    llm_response_content = ""

    if provider == "gemini":
        logger.info("Resumindo gráfico com Gemini")
        genai.configure(api_key=os.getenv("GOOGLE_API_KEY", "").strip())
        model = genai.GenerativeModel('gemini-2.0-flash')
        completion = model.generate_content(question + json.dumps(figures)[0:3900])
        llm_response_content = completion.text

    elif provider == "openai":
        logger.info("Resumindo gráfico com Azure OpenAI")
        client = AzureOpenAI(
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT", "").strip(),
            api_key=os.getenv("OPENAI_API_KEY", "").strip(),
            api_version=os.getenv("OPENAI_API_VERSION", "").strip(),
        )
        completion = client.chat.completions.create(
            model=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "").strip(),
            messages=[{"role": "user", "content": question + json.dumps(figures)[0:3900]}],
        )
        llm_response_content = completion.choices[0].message.content
    else:
        raise ValueError(f"Provedor de LLM '{provider}' inválido.")

    response = dcc.Markdown(llm_response_content)

    return dmc.LoadingOverlay(
        [
            dbc.Button(
                children="Home",
                href="/",
                style={"background-color": "#238BE6", "margin": "10px"},
            ),
            html.Div([response, html.Div(layout[1:])], style={"padding": "40px"}),
        ]
    )