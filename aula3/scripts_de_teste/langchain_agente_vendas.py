# langchain_agente_vendas.py

import pandas as pd
from langchain.agents import initialize_agent, AgentType
from langchain_openai import ChatOpenAI
from langchain.tools import tool
from langchain.prompts import ChatPromptTemplate
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()



from datetime import datetime

def processar_cadencia(cadencia_str, data_negociacao_str):
    meses = {
        "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
        "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12
    }
    
    partes = cadencia_str.lower().split()
    resultado = []
    
    # Inicializa com base na data de negociação fornecida
    data_negociacao = datetime.strptime(data_negociacao_str, "%d/%m/%Y")
    ano_negociacao = data_negociacao.year
    mes_negociacao = data_negociacao.month
    
    ano_atual = ano_negociacao
    mes_anterior = mes_negociacao

    for i in range(0, len(partes), 2):
        qtd = partes[i]
        mes_nome = partes[i+1][:3]  # garantir 3 letras
        mes = meses.get(mes_nome)
        
        if mes is None:
            continue  # pula se o mês for inválido
        
        # Ajusta o ano somente se houver passagem de dezembro para janeiro
        if mes_anterior == 12 and mes == 1:
            ano_atual += 1
        elif mes_anterior < mes:
            pass  # mantém o mesmo ano
        elif mes_anterior > mes:
            # Somente volta ao ano anterior se estiver na transição ano novo para dezembro anterior
            if mes_anterior == 1 and mes == 12:
                ano_atual -= 1

        resultado.append(f"{mes:02}.{ano_atual}:{qtd} ton")
        mes_anterior = mes

    return "\n".join(resultado)


# Carregar CSVs
clientes_df = pd.read_csv('precofixo-de-para.csv', dtype=str)
material_df = pd.read_csv('material.csv', dtype=str)
cond_pag_df = pd.read_csv('condicao-de-pagamento.csv', dtype=str)
forma_pag_df = pd.read_csv('forma-de-pagamento.csv', dtype=str)

# Tools para o agente

@tool("busca_cliente")
def busca_cliente(codigo_ou_nome_ou_cpf:str):
    '''Busca cliente pelo CPF/CNPJ, código ou nome e retorna nome e código.'''
    cliente = clientes_df[(clientes_df['CNPJ/CPF'].str.replace('[^0-9]','',regex=True)==codigo_ou_nome_ou_cpf.replace('.','').replace('-','').replace('/','')) |
                          (clientes_df['Cliente']==codigo_ou_nome_ou_cpf) |
                          (clientes_df['Nome Cliente'].str.upper()==codigo_ou_nome_ou_cpf.upper())]
    if not cliente.empty:
        return cliente[['Nome Cliente','Cliente']].iloc[0].to_dict()
    return "Cliente não encontrado"

@tool("busca_material")
def busca_material(nome_material:str):
    '''Busca código do material dado seu nome.'''
    material = material_df[material_df['Produto'].str.upper() == nome_material.upper()]
    if not material.empty:
        return material['Cód'].iloc[0]
    return "Material não encontrado"

@tool("busca_cond_pag")
def busca_cond_pag(cond_pag:str):
    '''Busca código da condição de pagamento dado seu significado.'''
    cond = cond_pag_df[cond_pag_df['Significado'].str.contains(cond_pag, case=False)]
    if not cond.empty:
        return cond['Cond Pagamento'].iloc[0]
    return "Condição de pagamento não encontrada"

@tool("busca_forma_pag")
def busca_forma_pag(forma_pag:str):
    '''Busca código da forma de pagamento dado seu significado.'''
    forma = forma_pag_df[forma_pag_df['Significado'].str.contains(forma_pag, case=False)]
    if not forma.empty:
        return forma['MP'].iloc[0]
    return "Forma de pagamento não encontrada"

# Setup LangChain
llm = ChatOpenAI(model='gpt-3.5-turbo')

agent = initialize_agent(
    tools=[busca_cliente, busca_material, busca_cond_pag, busca_forma_pag],
    llm=llm,
    agent=AgentType.OPENAI_FUNCTIONS,
    verbose=False
)

# Prompt exemplo
prompt_template = """
Formate a seguinte solicitação em uma saída organizada conforme modelo abaixo.
Pergunte ao usuário caso algum dado esteja faltando.

Saída modelo:
Data da solicitação:{data_solicitacao}
Número do Vendedor:{vendedor}
Cidade:{cidade}
Planta:{planta}
Nome do cliente:{nome_cliente}
Código do cliente:{codigo_cliente}
Campanha:SEM REF
Data da negociação:{data_negociacao}
Condição de pagamento:{cond_pag}
Forma de pagamento:{forma_pag}
Incoterms:{incoterms}
{frete}
Preço:{preco}
Código do material:{codigo_material}
-- Cadência --
{cadencia}

Solicitação do usuário:
{solicitacao}
"""

def gerar_saida(solicitacao_usuario:str):
    # Extrai a cadência e a data negociação diretamente para pré-processar
    cadencia = ""
    data_negociacao = ""

    for linha in solicitacao_usuario.lower().splitlines():
        if "cadência:" in linha:
            cadencia = linha.split("cadência:")[1].strip()
        if "data de negociação:" in linha:
            data_negociacao = linha.split("data de negociação:")[1].strip()

    cadencia_processada = processar_cadencia(cadencia, data_negociacao)

    prompt_template = f"""
    Você receberá uma solicitação com informações desorganizadas. 

    1. Extraia claramente os dados informados.
    2. Use as ferramentas fornecidas para realizar os seguintes "De-Para":
       - Código e nome do cliente a partir do CPF/CNPJ, Código ou Nome fornecido.
       - Código do Material a partir do nome informado.
       - Código da Condição de pagamento.
       - Código da Forma de pagamento.

    3. Caso falte qualquer informação obrigatória para o formato de saída, pergunte explicitamente ao usuário.
    
    Formate rigorosamente a saída final exatamente conforme o modelo abaixo, preenchendo todos os campos corretamente.

    Modelo de Saída:
    Data da solicitação: {datetime.now().strftime("%d/%m/%Y")}
    Número do Vendedor: <Informado pelo usuário>
    Cidade: <Informado pelo usuário>
    Planta: <Informado pelo usuário>
    Nome do cliente: <Nome obtido pela ferramenta busca_cliente>
    Código do cliente: <Código obtido pela ferramenta busca_cliente>
    Campanha: SEM REF
    Data da negociação: <Informado pelo usuário>
    Condição de pagamento: <Código obtido por busca_cond_pag>
    Forma de pagamento: <Código obtido por busca_forma_pag>
    Incoterms: <Informado pelo usuário>
    Preço frete: <Informado pelo usuário, incluir só se Incoterms diferente de FOB, caso seja FOB, omitir essa linha>
    Preço: <Informado pelo usuário>
    Código do material: <Código obtido por busca_material>
    -- Cadência --
    {cadencia_processada}

    Solicitação do usuário:
    {solicitacao_usuario}
    """

    resposta = agent.invoke(prompt_template)
    return resposta['output']  # retorna apenas o output



# Exemplo de uso
entrada_usuario = '''
CNPJ/CPF: 040.074.561-51
Planta: PDL
Condição de Pagamento: 15 dias
Forma de Pagamento: boleto
Código do Material: FS Ouro
Cadência: 40 fev 20 mar 58 abr
Vendedor: 5522999540302
Cidade: Lucas do Rio Verde
Data de Negociação: 10/03/2025
Incoterms: CIF
Preço Frete: 170
Valor: 2200
'''

print(gerar_saida(entrada_usuario))
