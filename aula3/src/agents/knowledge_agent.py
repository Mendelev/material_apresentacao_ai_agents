# src/agents/knowledge_agent.py
import json
import logging
from langchain_core.language_models.base import BaseLanguageModel
from memory.memory_manager import MemoryManager

logger = logging.getLogger(__name__)

class KnowledgeAgent:
    def __init__(self, llm: BaseLanguageModel, memory_manager: MemoryManager):
        self.llm = llm
        self.memory_manager = memory_manager

    def _answer_general_question(self, question: str) -> str:
        """Gera uma resposta para uma pergunta de conhecimento geral."""
        prompt = f"""
        Você é um assistente de IA prestativo e conversacional. Responda à pergunta do usuário da melhor forma possível.

        Pergunta do Usuário: {question}

        Resposta:
        """
        try:
            response = self.llm.invoke(prompt)
            return response.content if hasattr(response, 'content') else str(response)
        except Exception as e:
            logger.error(f"Erro ao invocar LLM para pergunta geral: {e}")
            return "Desculpe, tive um problema ao processar sua pergunta."


    def _answer_from_context(self, question: str, context: str, context_name: str) -> str:
        """Gera uma resposta baseada em um contexto fornecido (histórico ou dados)."""
        prompt = f"""
        Você é um assistente prestativo. Sua tarefa é responder à pergunta do usuário com base estritamente no {context_name} fornecido.
        Se a informação não estiver no contexto, diga que não encontrou a informação.

        {context_name}:
        ---
        {context}
        ---

        Pergunta do Usuário: {question}

        Resposta:
        """
        try:
            response = self.llm.invoke(prompt)
            # A resposta do LLM pode vir em um objeto ou como string, dependendo da classe
            return response.content if hasattr(response, 'content') else str(response)
        except Exception as e:
            logger.error(f"Erro ao invocar LLM para responder pergunta: {e}")
            return "Desculpe, tive um problema ao processar sua pergunta."

    def _generate_mongo_query_from_question(self, question: str) -> dict:
        """Usa o LLM para traduzir uma pergunta em linguagem natural para uma query MongoDB."""
        prompt = f"""
        Sua tarefa é converter a pergunta do usuário em um filtro de query para MongoDB.
        O filtro deve estar em um formato JSON válido.
        Mapeie os nomes dos campos para as chaves do banco de dados. As chaves mais comuns são: 'Nome do cliente', 'Valor', 'Preço Frete', 'CNPJ/CPF'.
        Se o usuário perguntar sobre o "último pedido", não adicione nada à query, pois a busca já é ordenada por data.
        Se o usuário usar um nome de cliente, use uma expressão regular case-insensitive. Ex: {{ "Nome do cliente": {{ "$regex": "nome do cliente", "$options": "i" }} }}

        Pergunta do Usuário: "{question}"

        JSON da Query MongoDB (apenas o filtro):
        """
        try:
            response = self.llm.invoke(prompt)
            response_text = response.content if hasattr(response, 'content') else str(response)
            
            # Limpa qualquer texto extra que o LLM possa ter retornado
            json_match = response_text[response_text.find('{'):response_text.rfind('}')+1]
            
            logger.debug(f"LLM gerou a query JSON: {json_match}")
            return json.loads(json_match)
        except Exception as e:
            logger.error(f"Falha ao gerar ou parsear a query MongoDB do LLM: {e}")
            return {}

    def answer_question(self, user_id: str, question: str, intent: str, chat_history: str) -> str:
        """Ponto de entrada principal para responder a uma pergunta."""
        
        if intent == 'PERGUNTA_SOBRE_A_CONVERSA':
            logger.info(f"Respondendo pergunta sobre a conversa atual para o usuário {user_id}.")
            return self._answer_from_context(question, chat_history, "Histórico da Conversa Atual")
            
        elif intent == 'PERGUNTA_SOBRE_O_HISTORICO':
            logger.info(f"Respondendo pergunta sobre o histórico de tickets para o usuário {user_id}.")
            
            # 1. Gerar a query
            mongo_query = self._generate_mongo_query_from_question(question)
            
            if not mongo_query:
                return "Não consegui entender os filtros da sua pergunta sobre o histórico."
                
            # 2. Executar a busca
            results = self.memory_manager.query_ticket_history(user_id, mongo_query)
            
            if not results:
                return "Não encontrei nenhum ticket no seu histórico que corresponda a essa busca."
            
            # 3. Gerar a resposta final a partir dos resultados
            # Usamos apenas o resultado mais recente para simplificar
            context_data = json.dumps(results[0], indent=2, ensure_ascii=False)
            return self._answer_from_context(question, context_data, "Dados do Último Ticket Encontrado")
        
        elif intent == 'CONVERSA_GERAL':
            logger.info(f"Respondendo pergunta geral para o usuário {user_id}.")
            return self._answer_general_question(question)
            
        else:
            return "Não sei como responder a esse tipo de pergunta."