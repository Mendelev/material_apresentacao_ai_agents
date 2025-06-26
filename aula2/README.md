# Agente de IA para Análise de Dados com LLMs

![Python](https://img.shields.io/badge/Python-3.12%2B-blue.svg)
![Dash](https://img.shields.io/badge/Dash-2.9-orange.svg)
![LangChain](https://img.shields.io/badge/LangChain-0.1-green.svg)
![License](https://img.shields.io/badge/License-MIT-lightgrey.svg)

Este projeto demonstra a construção de um agente de IA avançado que permite aos usuários interagir com um banco de dados relacional (PostgreSQL) usando linguagem natural. A aplicação web, construída com Dash, traduz as perguntas dos usuários em consultas SQL, executa-as, e gera tanto respostas textuais quanto visualizações de dados automáticas.

O agente é projetado para ser resiliente e inteligente, utilizando uma arquitetura **ReAct (Reason + Act)** para planejar, agir, observar os resultados e corrigir seus próprios erros em tempo real.

## ✨ Principais Funcionalidades

- **Consulta de Dados com Linguagem Natural (Text-to-SQL):** Permite que usuários não-técnicos façam perguntas complexas aos dados.
- **Geração Automática de Gráficos:** Cria visualizações com Plotly de forma dinâmica com base nos dados retornados e na pergunta do usuário.
- **Suporte a Múltiplos Provedores de LLM:** Flexibilidade para alternar entre **Azure OpenAI** e **Google Gemini** através de uma simples variável de ambiente.
- **Arquitetura ReAct:** O agente analisa seus próprios resultados e erros de SQL para refinar e corrigir suas ações, aumentando a precisão das respostas.
- **Engenharia de Prompts Avançada:** Utiliza contexto rico (schema do banco, descrições de tabelas, exemplos de dados) para guiar a LLM a gerar consultas mais precisas e eficientes.

## 🏛️ Arquitetura

O sistema é orquestrado por um agente Python que segue um fluxo lógico para responder a cada pergunta. A arquitetura pode ser visualizada da seguinte forma:

```mermaid
graph TD
    A[👨‍💻 Interface Web - Dash] --> B{🤖 Orquestrador do Agente};
    B -- Pergunta --> C[🧠 LLM - O Cérebro - OpenAI/Gemini];
    C -- Plano SQL/JSON --> B;
    B -- Ação --> D[🛠️ Ferramentas Executor de SQL];
    D -- Executa Query --> F[🗄️ Banco de Dados - PostgreSQL];
    F -- Resultado --> D;
    D -- Observação (Dados/Erro) --> B;
    subgraph Contexto
        E[📚 Base de Conhecimento - table_descriptions.py]
    end
    E --> C;
```

## 🛠️ Tecnologias Utilizadas
Backend: Python 3.9+
Interface Web: Plotly Dash, Dash Bootstrap Components
Inteligência Artificial: LangChain, API OpenAI (via Azure), API Google Gemini
Banco de Dados: PostgreSQL
Cache/Estado (Opcional): Redis
Manipulação de Dados: Pandas, SQLAlchemy
🚀 Guia de Instalação e Execução
Siga estes passos para configurar e executar o projeto em seu ambiente local.

1. Pré-requisitos
Python 3.9 ou superior
Um servidor PostgreSQL em execução
(Opcional) Um servidor Redis em execução, se for usar a funcionalidade de "salvar visualização".
Acesso à API do Azure OpenAI ou do Google Gemini.
2. Instalação
a. Clone o repositório:

```Bash

git clone <URL_DO_SEU_REPOSITORIO>
cd <NOME_DO_DIRETORIO>
b. Crie e ative um ambiente virtual (Recomendado):
```

```Bash

python -m venv venv
# No Windows:
venv\Scripts\activate
# No macOS/Linux:
source venv/bin/activate
c. Instale as dependências:
```

```Bash
pip install -r requirements.txt
```

3. Configuração do Ambiente
a. Crie um arquivo .env na raiz do projeto, copiando o exemplo abaixo.

b. Preencha o arquivo .env com suas credenciais:

Ini, TOML

### .env - Arquivo de Configuração

### --- SELEÇÃO DO PROVEDOR DE LLM ---
### Escolha entre "openai" ou "gemini"
```dotenv
LLM_PROVIDER="openai"
```

### --- CREDENCIAIS DO GOOGLE GEMINI ---
```dotenv
GOOGLE_API_KEY="SUA_CHAVE_DE_API_DO_GEMINI"
```

### --- CREDENCIAIS DO AZURE OPENAI ---
```dotenv
OPENAI_API_KEY="SUA_CHAVE_DE_API_DO_AZURE"
AZURE_OPENAI_ENDPOINT="https://SEU_[RECURSO.openai.azure.com/](https://RECURSO.openai.azure.com/)"
OPENAI_API_VERSION="2024-02-01"
AZURE_OPENAI_DEPLOYMENT_NAME="SEU_NOME_DE_DEPLOYMENT"
```

### --- CONEXÃO COM O BANCO DE DADOS POSTGRESQL ---
```dotenv
PG_HOST="localhost"
PG_PORT="5432"
PG_DATABASE="database-teste"
PG_USER="myuser"
PG_PASSWORD="mypassword"
```
### String de conexão completa para SQLAlchemy (usada por db_utils.py)
```dotenv
DB_CONNECTION_STRING="postgresql://myuser:mypassword@localhost:5432/database-teste"
```
### --- CONEXÃO REDIS (Opcional) ---
```dotenv
REDIS_URL="redis://127.0.0.1:6379"
```
4. Carga de Dados no Banco
Antes de iniciar a aplicação, você precisa popular o banco de dados com os dados dos seus arquivos CSV. Utilize o script de ingestão fornecido.

a. Coloque seus arquivos CSV na pasta csv_output/.

b. Configure o script csv_to_postgres_...py para apontar para seus arquivos e definir chaves, se necessário (conforme nossa conversa anterior).

c. Execute o script de carga:

```Bash
python csv_to_postgres_with_max_lines.py
```
### ou o outro script, dependendo do seu caso de uso
5. Executando a Aplicação
Com o ambiente configurado e os dados no banco, inicie a aplicação web:

```Bash

python app.py
```
Acesse a aplicação em seu navegador no endereço http://127.0.0.1:8050.

## 📂 Estrutura do Projeto
```Bash
.
├── pages/                # Contém as páginas da aplicação Dash (home, etc.)
│   ├── home.py
│   └── ...
├── csv_output/           # Pasta para armazenar os arquivos CSV de entrada
├── app.py                # Ponto de entrada principal da aplicação Dash
├── db_utils.py           # Coração do agente: lógica Text-to-SQL, ReAct, e interação com o DB
├── chart_generation.py   # Lógica para gerar gráficos dinamicamente com a LLM
├── csv_to_postgres...py  # Scripts para ingestão de dados dos CSVs para o PostgreSQL
├── table_descriptions.py # Arquivo CRÍTICO com as descrições em texto das tabelas
├── requirements.txt      # Lista de dependências Python
├── .env                  # Arquivo local para armazenar segredos e configurações
└── README.md             # Este arquivo
```

## 🌱 Futuras Melhorias
Adicionar mais Ferramentas: Permitir ao agente salvar resultados em arquivos ou buscar informações na web para enriquecer as respostas.
Memória Conversacional: Implementar memória para que o agente entenda o contexto de perguntas anteriores.
Interface de Edição: Permitir que o usuário edite o gráfico ou a consulta SQL gerada pela IA.