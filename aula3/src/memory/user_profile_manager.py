# src/memory/user_profile_manager.py
from pymongo import MongoClient
import logging

logger = logging.getLogger(__name__)

class UserProfileManager:
    def __init__(self, connection_string: str, db_name: str = "agent_memory"):
        try:
            self.client = MongoClient(connection_string)
            self.db = self.client[db_name]
            self.profiles = self.db.user_profiles
            logger.info("Conectado ao MongoDB para gerenciamento de perfis.")
        except Exception as e:
            logger.error(f"Não foi possível conectar ao MongoDB: {e}", exc_info=True)
            self.client = None

    def get_profile(self, user_id: str) -> dict | None:
        if not self.client: return None
        return self.profiles.find_one({"user_id": user_id})

    def update_profile(self, user_id: str, new_data: dict):
        """Atualiza o perfil de um usuário com novos dados.
        Exemplo de new_data: {'common_plant': 'PDL', 'last_client_cnpj': '12345'}
        """
        if not self.client: return
        self.profiles.update_one(
            {"user_id": user_id},
            {"$set": new_data, "$setOnInsert": {"user_id": user_id}},
            upsert=True # Cria o perfil se ele não existir
        )
        logger.info(f"Perfil do usuário '{user_id}' atualizado.")