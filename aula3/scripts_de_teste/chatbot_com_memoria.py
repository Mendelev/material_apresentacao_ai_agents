# chatbot_com_memoria.py

from langchain_openai import ChatOpenAI
from langchain.chains import ConversationChain
from langchain.memory import ConversationBufferWindowMemory
from dotenv import load_dotenv

load_dotenv()

# Configuração do modelo OpenAI
llm = ChatOpenAI(model='gpt-3.5-turbo')

# Memória de contexto para até as últimas 5 mensagens
memory = ConversationBufferWindowMemory(k=5)

# Criação do chatbot com LangChain
chatbot = ConversationChain(
    llm=llm,
    memory=memory,
    verbose=False
)

# Exemplo de interação contínua
while True:
    pergunta = input("Usuário: ")
    if pergunta.lower() in ['sair', 'exit', 'quit']:
        break

    resposta = chatbot.invoke(pergunta)
    print(f"Chatbot: {resposta['response']}")
