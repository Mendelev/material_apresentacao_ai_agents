# pages/custom_visual.py

import random
import dash_bootstrap_components as dbc
import dash_mantine_components as dmc
import pandas as pd
from dash import dcc, html, Input, Output, State, callback, register_page, no_update

from db_utils import run_query_and_summarize
from chart_generation import generate_auto_chart

register_page(__name__, path="/custom-visual")

layout = dbc.Container(
    [
        html.H2("Visualização Personalizada via LLM", className="mt-4"),
        html.P("Digite sua pergunta sobre os dados:", className="lead"),
        dmc.Textarea(
            id="custom-question",
            placeholder=random.choice(
                [
                    '"Quais são os 5 maiores clientes em faturamento?"',
                    '"Quantos registros existem na tabela X?"',
                    '"Quais meses apresentam maior taxa de churn?"',
                ]
            ),
            autosize=True,
            minRows=2,
            style={"width": "70%"},
        ),
        dmc.Group(
            [
                dmc.Button("Submeter", id="custom-submit", disabled=True),
            ],
            position="right",
        ),
        html.Hr(),
        # Área para texto de resposta (chat)
        html.Div(id="custom-chat-output", style={"margin-top": "20px"}),
        html.Hr(),
        # Exibição do gráfico automático
        dcc.Graph(id="auto-graph", figure={}),
    ],
    fluid=True,
)

@callback(
    Output("custom-submit", "disabled"),
    Input("custom-question", "value"),
)
def toggle_submit_button(value):
    """
    Habilita/desabilita o botão caso haja texto
    """
    return not bool(value)

@callback(
    Output("custom-chat-output", "children"),  # conversa/resposta
    Output("custom-question", "value"),        # limpa a pergunta
    Output("auto-graph", "figure"),            # exibe o gráfico
    Input("custom-submit", "n_clicks"),
    State("custom-question", "value"),
    State("custom-chat-output", "children"),
    prevent_initial_call=True,
)
def run_query_and_plot(n_clicks, question, current_chat):
    """
    1. Faz a consulta ao DB via run_query_and_summarize()
    2. Traz o DataFrame
    3. Gera a figure via generate_auto_chart()
    4. Exibe a resposta textual e o gráfico
    """
    if not question:
        return no_update, no_update, no_update

    # 1) Executa a query e obtem a resposta textual e o DF
    response_text, df_result = run_query_and_summarize(question)

    # 2) Monta um "chat" simples
    new_items = [
        dbc.Card(
            [
                dbc.CardHeader("Você"),
                dbc.CardBody(question),
            ],
            style={"margin-bottom": "10px"},
        ),
        dbc.Card(
            [
                dbc.CardHeader("LLM"),
                dbc.CardBody(response_text),
            ],
            style={"margin-bottom": "10px", "border-color": "gray"},
        ),
    ]

    updated_chat = (new_items + current_chat) if current_chat else new_items

    # 3) Se DF vier vazio, não plotamos nada
    if df_result.empty:
        empty_fig = {
            "data": [],
            "layout": {"title": "Nenhum dado retornado ou DF vazio"}
        }
        return updated_chat, "", empty_fig

    # 4) Gera a figura com a LLM
    figure_dict = generate_auto_chart(df_result, question)
    print("Figura gerada pela LLM:", figure_dict)

    return updated_chat, "", figure_dict
