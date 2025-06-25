# pages/home.py

import random

import dash_bootstrap_components as dbc
import dash_mantine_components as dmc
from dash import (
    dcc,
    html,
    Input,
    Output,
    State,
    callback,
    register_page,
    no_update,
)
import pandas as pd

import utils
from db_utils import run_query_and_summarize
from chart_generation import generate_auto_chart

# Define esta página como a principal (path="/")
register_page(__name__, path="/")

layout = dbc.Container(
    [
        # --- Adição do logo ---
        dbc.Row(
            [
                dbc.Col(
                    html.Img(
                        src="assets/image.jpg",  # Caminho para a imagem
                        height="100px",  # Ajuste a altura conforme necessário
                    ),
                    width="auto",  # Largura automática para manter a proporção
                    className="mb-3", # Adicionado margin-bottom
                ),
                dbc.Col(
                    #titulo principal da pagina
                    html.H1("Agente Smarthis", #Coloque aqui o título principal da sua página
                            className="text-center text-primary"), #centraliza e deixa texto azul.
                    width=True,
                    className="mt-2 mb-2",
                )
            ],
            justify="start",  # Alinha a linha à esquerda
            align="start",
            style={'display': 'flex'} #Adicionado display flex para alinhar horizontalmente o logo e o titulo.
        ),

        # Linha contendo a Textarea e o Botão, lado a lado
        dbc.Row(
            [
                dbc.Col(
                    dmc.Textarea(
                        id="user-question",
                        placeholder=random.choice(
                            [
                                "Qual é a precipitação média anual em Seattle?",
                                "Quais estados têm mais de 100 polegadas de precipitação?",
                                "Mostre os dados de precipitação de 2019 para todas as cidades.",
                                "Qual cidade tem a maior precipitação média?",
                                "Como a precipitação varia entre os estados dos EUA?",
                            ]
                        ),
                        autosize=True,
                        minRows=2,
                        style={"width": "100%"},
                    ),
                    width=10,  # ou ajuste conforme desejar
                ),
                dbc.Col(
                    dmc.Button("Enviar", id="submit-btn", disabled=True),
                    width=2,
                    style={"display": "flex", "align-items": "end"},  # para alinhar o botão
                ),
            ],
            className="mt-4",
        ),

        html.Hr(),

        # dcc.Loading mostrará um spinner durante o callback
        dcc.Loading(
            id="loading-state",
            type="circle",  # pode ser "default", "dot", etc.
            children=html.Div(id="chat-container"),
        ),
    ],
    fluid=True,
)


@callback(
    Output("submit-btn", "disabled"),
    Input("user-question", "value"),
)
def toggle_button_disabled(value):
    """
    Habilita/desabilita o botão de envio
    se o usuário não digitou nada
    """
    return not bool(value)


@callback(
    Output("chat-container", "children"),  # Atualiza a lista de cards
    Output("user-question", "value"),      # Limpa o input
    Input("submit-btn", "n_clicks"),
    State("user-question", "value"),
    State("chat-container", "children"),
    prevent_initial_call=True,
)
def handle_question(n, question, current_children):
    """
    1. Usuario envia uma pergunta
    2. Chamamos run_query_and_summarize -> obtemos (resposta_texto, df_result)
    3. Se df_result não vazio, chamamos generate_auto_chart -> obtemos figure
    4. Criamos um "bloco" (Pergunta, Resposta, Graph) e inserimos no topo
    """
    if not question:
        return no_update, no_update

    # 1) Consulta no DB + reescrita
    response_text, df_result = run_query_and_summarize(question)
    formatted_response = response_text.replace("\n", "  \n")
    # o "  \n" (dois espaços e depois newline) força uma quebra de linha em Markdown

    # 2) Se df_result vier vazio, não gera gráfico
    if df_result.empty:
        new_block = html.Div(
            [
                dbc.CardHeader(f"Pergunta: {question}"),
                dbc.CardBody(
                    [
                        html.Strong("Resposta:"),
                        dcc.Markdown(formatted_response),
                        html.Hr(),
                        html.Small("Nenhum dado retornado para exibir gráfico."),
                    ]
                ),
            ],
            style={"margin-bottom": "15px", "border": "1px solid #ddd", "padding": "10px"},
        )
    else:
        # Gera figure com a LLM
        figure_dict = generate_auto_chart(df_result, question)
        print("Figura gerada pela LLM:", figure_dict)

        new_block = html.Div(
            [
                dbc.CardHeader(f"Pergunta: {question}"),
                dbc.CardBody(
                    [
                        html.Strong("Resposta:"),
                        dcc.Markdown(formatted_response),
                        html.Hr(),
                        dcc.Graph(figure=figure_dict, style={"height": "600px"})


                    ]
                ),
            ],
            style={"margin-bottom": "15px", "border": "1px solid #ddd", "padding": "10px"},
        )

    # 3) Inserimos este novo bloco no topo do "chat-container"
    if not current_children:
        updated_children = [new_block]
    else:
        updated_children = [new_block] + current_children

    # 4) Retornamos updated_children e limpamos o input
    return updated_children, ""