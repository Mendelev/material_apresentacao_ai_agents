# src/memory/memory_manager.py
from pymongo import MongoClient
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class MemoryManager:
    def __init__(self, connection_string: str, db_name: str = "agent_memory"):
        try:
            self.client = MongoClient(connection_string)
            self.db = self.client[db_name]
            self.profiles = self.db.user_profiles
            self.ticket_history = self.db.ticket_history # NOVA COLEÇÃO
            logger.info("Conectado ao MongoDB para gerenciamento de perfis e histórico.")
        except Exception as e:
            logger.error(f"Não foi possível conectar ao MongoDB: {e}", exc_info=True)
            self.client = None

    def get_profile(self, user_id: str) -> dict | None:
        if not self.client: return None
        return self.profiles.find_one({"user_id": user_id})

    def update_profile(self, user_id: str, new_data: dict):
        if not self.client: return
        self.profiles.update_one(
            {"user_id": user_id},
            {"$set": new_data, "$setOnInsert": {"user_id": user_id}},
            upsert=True
        )
        logger.info(f"Perfil do usuário '{user_id}' atualizado.")
    
    def save_ticket_to_history(self, user_id: str, ticket_data: dict):
        """Salva um dicionário de ticket completo no histórico."""
        if not self.client: return
        
        # Cria uma cópia para não modificar o dicionário original
        doc_to_save = ticket_data.copy()
        
        # Adiciona metadados úteis para futuras buscas
        doc_to_save['user_id'] = user_id
        doc_to_save['created_at'] = datetime.now()
        
        self.ticket_history.insert_one(doc_to_save)
        logger.info(f"Ticket do usuário '{user_id}' salvo no histórico.")

    def query_ticket_history(self, user_id: str, query: dict, sort_by: str = 'created_at', limit: int = 5) -> list:
        """Busca no histórico de tickets de um usuário."""
        if not self.client: return []
        
        # Garante que a busca seja sempre no contexto do usuário
        full_query = query.copy()
        full_query['user_id'] = user_id
        
        logger.debug(f"Executando query no histórico de tickets: {full_query}")
        
        # find retorna um cursor, convertemos para lista
        results = list(
            self.ticket_history.find(full_query, {'_id': 0}).sort(sort_by, -1).limit(limit)
        )
        return results