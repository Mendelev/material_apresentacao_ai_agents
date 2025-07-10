# src/config.py
import os
from dotenv import load_dotenv

load_dotenv() # Carrega variáveis do arquivo .env

# Configurações Gerais
ARTIFACTS_DIR = os.getenv("ARTIFACTS_DIR", "../artifacts") # Diretório onde estão os CSVs

# --- Configurações LLM ---
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "openai").lower() # "openai" ou "google_genai"
LLM_TEMPERATURE = float(os.getenv("LLM_TEMPERATURE", 0.0))
LLM_MAX_TOKENS = int(os.getenv("LLM_MAX_TOKENS", 1024)) # Max tokens para a resposta do LLM

# OpenAI
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL_NAME = os.getenv("OPENAI_MODEL_NAME", "gpt-3.5-turbo-instruct") # Modelo de completions

# Google Gemini
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GEMINI_MODEL_NAME = os.getenv("GEMINI_MODEL_NAME", "gemini-2.5-flash-preview-04-17") # Modelo Gemini (Flash é bom para velocidade/custo)
                                                                         # Poderia ser "gemini-pro" se preferir

AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY") # Sua API Key do Azure OpenAI
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT") # Ex: "https://your-resource-name.openai.azure.com/"
AZURE_OPENAI_DEPLOYMENT_NAME = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME") # O nome da SUA implantação do gpt-3.5-turbo-instruct
AZURE_MODEL_NAME = os.getenv("AZURE_MODEL_NAME") # O nome do modelo a ser usado no Azure OpenAI
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION") # A API version correta, ex: "2024-02-15-preview"

MONGO_CONNECTION_STRING = os.getenv("MONGO_CONNECTION_STRING", "mongodb://localhost:27017/") # Conexão padrão do MongoDB