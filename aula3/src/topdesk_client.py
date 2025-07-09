import requests
from requests.auth import HTTPBasicAuth
import os
import dotenv
import html
import json
import logging # Adicionado logging

dotenv.load_dotenv()
logger = logging.getLogger(__name__) # Adicionado logger

TOPDESK_API_URL = "https://fsbioenergia.topdesk.net/tas/api/incidents"
# IDs fixos (considerar mover para config.py ou .env se mudarem com frequência)
DEFAULT_CALLER_ID = "81341708-e497-4334-ae3a-7a38282761e8"
DEFAULT_CATEGORY_ID = "26b435cc-dc0c-4634-92e6-fc8958302647"
DEFAULT_SUBCATEGORY_ID = "5cce0afb-3c41-4e10-873f-35480e3b08b9"
DEFAULT_CALLTYPE_ID = "b46bd95d-1b4b-5667-bf6a-86531696c8cc"
DEFAULT_IMPACT_ID = "b919251c-22ce-5384-8377-8f220eb8e76e"
DEFAULT_URGENCY_ID = "5ed5103a-176b-4bc3-bc3c-a0058e855d6b"
DEFAULT_OPERATOR_ID = "a690d68b-0fd1-4ecb-8b71-c271f8d774b6" # Mesmo ID para grupo/operador? Verificar.
DEFAULT_OPERATOR_GROUP_ID = "a690d68b-0fd1-4ecb-8b71-c271f8d774b6"

class TopDeskClient:
    def __init__(self, username=None, password=None, base_url=TOPDESK_API_URL):
        self.username = username or os.getenv("TOPDESK_USERNAME")
        self.password = password or os.getenv("TOPDESK_PASSWORD")
        self.base_url = base_url

        if not self.username or not self.password:
            raise ValueError("Credenciais do TopDesk (usuário/senha) não fornecidas via argumento ou .env")

    def _get_auth(self):
        """Retorna o objeto de autenticação."""
        return HTTPBasicAuth(self.username, self.password)

    def _build_payload(self, request_text: str) -> dict:
        """Constrói o payload JSON para a criação do incidente."""

        request_html = html.escape(request_text).replace("\n", "<br>")


        payload = {
            "caller": {"id": DEFAULT_CALLER_ID},
            "status": "firstLine",
            "briefDescription": "PREÇO FIXO",
            "request": request_html,
            "category": {"id": DEFAULT_CATEGORY_ID},
            "subcategory": {"id": DEFAULT_SUBCATEGORY_ID},
            "entryType": {"name": "Portal"},
            "callType": {"id": DEFAULT_CALLTYPE_ID},
            "impact": {"id": DEFAULT_IMPACT_ID},
            "urgency": {"id": DEFAULT_URGENCY_ID},
            "processingStatus": {"name": "Registrado"},
            "operator": {"id": DEFAULT_OPERATOR_ID},
            "operatorGroup": {"id": DEFAULT_OPERATOR_GROUP_ID}
        }
        return payload

    def create_incident(self, request_text: str) -> str | None:
        """
        Cria um incidente no TopDesk com a descrição fornecida.

        Args:
            request_text: A descrição textual do incidente (será formatada para HTML).

        Returns:
            O número do ticket criado em caso de sucesso, ou None em caso de erro.
        """
        auth = self._get_auth()
        payload = self._build_payload(request_text)
        headers = {'Content-Type': 'application/json'}

        try:
            logger.debug(f"Enviando requisição para TopDesk: URL={self.base_url}, Payload={json.dumps(payload)}")
            response = requests.post(self.base_url, headers=headers, json=payload, auth=auth)
            response.raise_for_status() # Lança exceção para erros HTTP 4xx/5xx

            response_json = response.json()
            ticket_number = response_json.get("number")
            if ticket_number:
                logger.info(f"Incidente criado com sucesso no TopDesk. Ticket: {ticket_number}")
                return ticket_number
            else:
                logger.error(f"Resposta OK do TopDesk, mas sem número do ticket. Resposta: {response_json}")
                return None

        except requests.exceptions.HTTPError as http_err:
            logger.error(f"Erro HTTP ao criar incidente no TopDesk: {http_err.response.status_code} - {http_err.response.text}", exc_info=True)
            return None
        except requests.exceptions.RequestException as req_err:
            logger.error(f"Erro de requisição ao criar incidente no TopDesk: {req_err}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Erro inesperado ao criar incidente no TopDesk: {e}", exc_info=True)
            return None

# Exemplo de uso (opcional)
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG) # Habilita debug para teste
    try:
        client = TopDeskClient()
        ticket = client.create_incident("Este é um chamado de teste\nCom múltiplas linhas\nCriado via script.")
        if ticket:
            print(f"Ticket de teste criado: {ticket}")
        else:
            print("Falha ao criar ticket de teste.")
    except ValueError as ve:
        print(f"Erro de configuração: {ve}")
    except Exception as ex:
        print(f"Erro durante teste: {ex}")
