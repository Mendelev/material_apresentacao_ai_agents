import unittest
from unittest.mock import patch, MagicMock, mock_open, ANY
import os
import json
import time
from flask import Flask
from requests.exceptions import HTTPError, RequestException

# Adicione o diretório src ao sys.path
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

# Importar o módulo `whatsapp_integration` DEPOIS de mockar suas dependências de config/agentes, se necessário
# Ou importar e depois mockar os objetos dentro dele.
# Para simplificar, vamos mockar as instâncias globais que ele cria.

# Mockear CONSTANTES antes de importar o módulo `whatsapp_integration`
MOCK_CONFIG = {
    'ARTIFACTS_DIR': '../artifacts_test', # Use um dir de teste
    'LLM_PROVIDER': 'openai',
    'LLM_TEMPERATURE': 0.0,
    'LLM_MAX_TOKENS': 100,
    'OPENAI_API_KEY': 'fake_openai_key',
    # ... outras configs que podem ser necessárias para inicialização dos agentes
}

# Mock das classes de agente antes de importar whatsapp_integration
# para que whatsapp_integration use nossas mocks
mock_extraction_agent_instance = MagicMock()
mock_mapping_agent_instance = MagicMock()
mock_mapping_agent_instance.data_loaded_successfully = True # Simula carregamento bem-sucedido
mock_topdesk_client_instance = MagicMock()
mock_orchestration_agent_instance = MagicMock()

# Esta função será usada para retornar nossa instância mockada do OrchestrationAgent
def get_mock_orchestrator(*args, **kwargs):
    # Simula o carregamento de estado que ocorreria em get_orchestrator_instance
    # O estado será manipulado pelo próprio teste através de load_state_mock
    return mock_orchestration_agent_instance


# Aplicar patches ANTES da importação do módulo whatsapp_integration
patches_to_apply_before_import = [
    patch('config.ARTIFACTS_DIR', MOCK_CONFIG['ARTIFACTS_DIR']),
    patch('config.OPENAI_API_KEY', MOCK_CONFIG['OPENAI_API_KEY']), # e outras chaves de API
    patch('agents.extraction_agent.ExtractionAgent', return_value=mock_extraction_agent_instance),
    patch('agents.mapping_agent.MappingAgent', return_value=mock_mapping_agent_instance),
    patch('topdesk_client.TopDeskClient', return_value=mock_topdesk_client_instance),
    # Mock OrchestrationAgent para controlar seu comportamento completamente
    patch('agents.orchestration_agent.OrchestrationAgent', return_value=mock_orchestration_agent_instance)
]

for p in patches_to_apply_before_import:
    p.start()

# Agora importe o app e outras funções do whatsapp_integration
from whatsapp_integration import app, WEBHOOK_VERIFY_TOKEN, GRAPH_API_TOKEN, \
                                 SESSION_DIR, SESSION_TTL, \
                                 save_state, load_state, clear_state, \
                                 send_whatsapp_message, _create_topdesk_ticket_and_reply, \
                                 _process_text_message, _handle_interactive_message, \
                                 _get_session_path

# Parar os patches globais após a importação (eles foram usados para a inicialização do módulo)
for p in patches_to_apply_before_import:
    p.stop()

TEST_SESSION_DIRECTORY = os.path.join(os.path.dirname(__file__), "test_whatsapp_sessions")


class TestWhatsappIntegration(unittest.TestCase):
    DUMMY_WA_ID = "1234567890"
    DUMMY_MSG_ID = "wamid.HBgLMTIzNDU2Nzg5MAUCABIYFkNOTVgwN0FBRERGM0UzMkRDN0RCAA=="
    DUMMY_PHONE_ID = "987654321"

    @classmethod
    def setUpClass(cls):
        # Configurar o Flask app para teste
        app.config['TESTING'] = True
        app.config['DEBUG'] = False
        # Mock de variáveis de ambiente globais para o módulo whatsapp_integration
        cls.env_patcher = patch.dict(os.environ, {
            "WEBHOOK_VERIFY_TOKEN": "test_verify_token",
            "GRAPH_API_TOKEN": "test_graph_token",
            "TOPDESK_USERNAME": "td_user", # Necessário para TopDeskClient mock
            "TOPDESK_PASSWORD": "td_pass"  # Necessário para TopDeskClient mock
        })
        cls.env_patcher.start()

        # Atualiza os valores no módulo importado APÓS o patch de environ
        # (Se eles foram lidos no momento da importação inicial)
        # Isso pode ser um pouco frágil; idealmente, o módulo leria do os.environ dinamicamente.
        # Mas para WEBHOOK_VERIFY_TOKEN e GRAPH_API_TOKEN, eles são lidos no topo.
        import whatsapp_integration
        whatsapp_integration.WEBHOOK_VERIFY_TOKEN = "test_verify_token"
        whatsapp_integration.GRAPH_API_TOKEN = "test_graph_token"
        # Re-instancia o topdesk_client global dentro do módulo com as env vars mockadas se necessário
        # ou assegure-se que o topdesk_client mockado globalmente seja usado.
        # No nosso caso, mock_topdesk_client_instance já é usado via patch.
        os.makedirs(TEST_SESSION_DIRECTORY, exist_ok=True) # Usa a constante
        for f in os.listdir(TEST_SESSION_DIRECTORY):
            os.remove(os.path.join(TEST_SESSION_DIRECTORY, f))

    @classmethod
    def tearDownClass(cls):
        cls.env_patcher.stop()
        # Limpar arquivos de sessão e o diretório APÓS todos os testes da classe
        if os.path.exists(TEST_SESSION_DIRECTORY):
            for f in os.listdir(TEST_SESSION_DIRECTORY):
                os.remove(os.path.join(TEST_SESSION_DIRECTORY, f))
            if not os.listdir(TEST_SESSION_DIRECTORY):
                os.rmdir(TEST_SESSION_DIRECTORY)

    def setUp(self):
        self.client = app.test_client()
        mock_orchestration_agent_instance.reset_mock()
        mock_topdesk_client_instance.reset_mock()
        # Não precisa mais criar/limpar o diretório aqui, já é feito em setUpClass/tearDownClass
        # Mas pode ser útil limpar arquivos específicos se um teste os criar e o próximo não esperar por eles.
        # Por enquanto, vamos manter a limpeza de arquivos em setUpClass e tearDownClass.

        self.patch_agents_ready = patch('whatsapp_integration.STATELESS_AGENTS_READY', True)
        self.patch_agents_ready.start()

    def tearDown(self):
        self.patch_agents_ready.stop()
        # Limpar QUALQUER arquivo de sessão que possa ter sido criado no teste atual
        # Isso evita que um teste afete o outro se a limpeza em tearDownClass não for suficiente
        # ou se você rodar testes individualmente.
        for f in os.listdir(TEST_SESSION_DIRECTORY): # Acessa via constante
            try:
                os.remove(os.path.join(TEST_SESSION_DIRECTORY, f))
            except OSError:
                pass


    # --- Testes de Sessão (com sistema de arquivos mockado) ---
    @patch('whatsapp_integration.SESSION_DIR', new=TEST_SESSION_DIRECTORY)
    @patch('time.time')
    def test_save_load_clear_state(self, mock_time):
        wa_id = "testuser1"
        state_data = {"key": "value", "count": 1}

        # Save
        mock_time.return_value = 1000.0
        save_state(wa_id, state_data.copy()) # Salva uma cópia
        session_file = _get_session_path(wa_id) # Usa o SESSION_DIR mockado
        self.assertTrue(os.path.exists(session_file))
        with open(session_file, 'r') as f:
            saved_content = json.load(f)
        self.assertEqual(saved_content["key"], "value")
        self.assertEqual(saved_content["_timestamp"], 1000.0)

        # Load (dentro do TTL)
        mock_time.return_value = 1000.0 + SESSION_TTL - 100 # Ainda dentro do TTL
        loaded_state = load_state(wa_id)
        self.assertIsNotNone(loaded_state)
        self.assertEqual(loaded_state["key"], "value")
        self.assertNotIn("_timestamp", loaded_state) # Timestamp deve ser removido

        # Load (expirado)
        mock_time.return_value = 1000.0 + SESSION_TTL + 100 # Expirou
        expired_state = load_state(wa_id)
        self.assertIsNone(expired_state)
        self.assertFalse(os.path.exists(session_file)) # Deve ser removido

        # Clear
        save_state(wa_id, state_data.copy()) # Salva de novo para testar clear
        self.assertTrue(os.path.exists(session_file))
        clear_state(wa_id)
        self.assertFalse(os.path.exists(session_file))

    @patch('whatsapp_integration.SESSION_DIR', new=TEST_SESSION_DIRECTORY)
    def test_load_state_file_not_found(self):
        self.assertIsNone(load_state("non_existent_user"))

    @patch('whatsapp_integration.SESSION_DIR', new=TEST_SESSION_DIRECTORY)
    @patch('builtins.open', new_callable=mock_open, read_data="invalid json")
    def test_load_state_json_decode_error(self, mock_file):
        # Precisa simular a existência do arquivo para 'open' ser chamado
        with patch('os.path.exists', return_value=True):
            with patch('os.remove') as mock_remove: # Mock os.remove para verificar se é chamado
                state = load_state("user_with_bad_json")
                self.assertIsNone(state)
                mock_remove.assert_called_once() # Verifica se o arquivo corrompido foi removido

    # --- Testes de Envio de Mensagem ---
    @patch('whatsapp_integration.requests.post')
    def test_send_whatsapp_message_text_success(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        result = send_whatsapp_message(self.DUMMY_PHONE_ID, self.DUMMY_WA_ID, "Hello")
        self.assertTrue(result)
        expected_url = f"https://graph.facebook.com/v18.0/{self.DUMMY_PHONE_ID}/messages"
        expected_json = {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": self.DUMMY_WA_ID,
            "type": "text",
            "text": {"body": "Hello"}
        }
        mock_post.assert_called_once_with(
            expected_url,
            headers=ANY, # Verificar conteúdo exato do header pode ser frágil
            json=expected_json,
            timeout=15
        )

    @patch('whatsapp_integration.requests.post')
    def test_send_whatsapp_message_interactive_success(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response
        payload = {"type": "button", "body": {"text": "Choose"}, "action": {"buttons": []}}

        result = send_whatsapp_message(self.DUMMY_PHONE_ID, self.DUMMY_WA_ID, interactive_payload=payload)
        self.assertTrue(result)
        expected_json = {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": self.DUMMY_WA_ID,
            "type": "interactive",
            "interactive": payload
        }
        mock_post.assert_called_once_with(ANY, headers=ANY, json=expected_json, timeout=ANY)


    @patch('whatsapp_integration.requests.post')
    def test_send_whatsapp_message_failure(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = "Error"
        mock_response.raise_for_status.side_effect = HTTPError(response=mock_response)
        mock_post.return_value = mock_response
        result = send_whatsapp_message(self.DUMMY_PHONE_ID, self.DUMMY_WA_ID, "Fail")
        self.assertFalse(result)

    @patch('whatsapp_integration.requests.post')
    def test_send_whatsapp_message_no_token(self, mock_post):
        with patch('whatsapp_integration.GRAPH_API_TOKEN', None):
            result = send_whatsapp_message(self.DUMMY_PHONE_ID, self.DUMMY_WA_ID, "No token")
            self.assertFalse(result)
            mock_post.assert_not_called()

    # --- Testes de Webhook GET ---
    def test_webhook_get_success(self):
        response = self.client.get(f'/webhook?hub.mode=subscribe&hub.verify_token=test_verify_token&hub.challenge=CHALLENGE_ACCEPTED')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data.decode(), 'CHALLENGE_ACCEPTED')

    def test_webhook_get_failure(self):
        response = self.client.get(f'/webhook?hub.mode=subscribe&hub.verify_token=wrong_token&hub.challenge=CHALLENGE_ACCEPTED')
        self.assertEqual(response.status_code, 403)

    # --- Testes de Webhook POST (Lógica Principal) ---
    def _get_sample_text_payload(self, text_content, wa_id=DUMMY_WA_ID, msg_id=DUMMY_MSG_ID, phone_id=DUMMY_PHONE_ID):
        return {
            "entry": [{
                "changes": [{
                    "value": {
                        "messaging_product": "whatsapp",
                        "metadata": {"phone_number_id": phone_id},
                        "messages": [{
                            "from": wa_id,
                            "id": msg_id,
                            "type": "text",
                            "text": {"body": text_content}
                        }]
                    }
                }]
            }]
        }

    def _get_sample_interactive_payload(self, button_id, button_title, wa_id=DUMMY_WA_ID, msg_id=DUMMY_MSG_ID, phone_id=DUMMY_PHONE_ID):
        return {
            "entry": [{
                "changes": [{
                    "value": {
                        "messaging_product": "whatsapp",
                        "metadata": {"phone_number_id": phone_id},
                        "messages": [{
                            "from": wa_id, "id": msg_id, "type": "interactive",
                            "interactive": {
                                "type": "button_reply",
                                "button_reply": {"id": button_id, "title": button_title}
                            }
                        }]
                    }
                }]
            }]
        }

    @patch('whatsapp_integration.send_whatsapp_message')
    @patch('whatsapp_integration.load_state', return_value=None) # Novo usuário, sem estado salvo
    @patch('whatsapp_integration.save_state')
    @patch('whatsapp_integration.clear_state')
    def test_webhook_post_text_message_needs_input(self, mock_clear_state, mock_save_state, mock_load_state, mock_send_wpp):
        # Configurar o mock do Orchestrator para retornar 'needs_input'
        mock_orchestration_agent_instance.process_user_input.return_value = {
            "status": "needs_input",
            "message": "Qual o CNPJ?"
        }
        mock_orchestration_agent_instance.get_state_dict.return_value = {"current_state": "asking_cnpj"}

        payload = self._get_sample_text_payload("Pedido inicial")
        response = self.client.post('/webhook', json=payload)

        self.assertEqual(response.status_code, 200)
        mock_orchestration_agent_instance.process_user_input.assert_called_once_with("Pedido inicial", metadata={'vendedor_id': self.DUMMY_WA_ID})
        mock_send_wpp.assert_called_once_with(
            phone_number_id=self.DUMMY_PHONE_ID,
            to_wa_id=self.DUMMY_WA_ID,
            message_body="Qual o CNPJ?",
            context_message_id=self.DUMMY_MSG_ID
        )
        mock_save_state.assert_called_once_with(self.DUMMY_WA_ID, {"current_state": "asking_cnpj"})
        mock_clear_state.assert_not_called()

    @patch('whatsapp_integration.send_whatsapp_message')
    @patch('whatsapp_integration.load_state', return_value=None)
    @patch('whatsapp_integration.save_state')
    @patch('whatsapp_integration.clear_state')
    @patch('whatsapp_integration._create_topdesk_ticket_and_reply', return_value=True) # Simula criação de ticket
    def test_webhook_post_text_message_confirmed_for_creation(self, mock_create_td, mock_clear, mock_save, mock_load, mock_send_wpp):
        final_payload_data = {"Cliente": "Teste", "Valor": 100, "Cadencia_Formatada": "JAN/25:10 ton"}
        mock_orchestration_agent_instance.process_user_input.return_value = {
            "status": "confirmed_for_creation",
            "message": "Confirmado!", # Esta mensagem não é enviada, _create_topdesk_ticket_and_reply envia a sua
            "payload": final_payload_data
        }
        # O orchestrator já terá resetado seu estado interno para "confirmed_for_creation"
        mock_orchestration_agent_instance.get_state_dict.return_value = {} # Estado resetado

        payload = self._get_sample_text_payload("Sim, confirmo")
        response = self.client.post('/webhook', json=payload)

        self.assertEqual(response.status_code, 200)
        mock_create_td.assert_called_once_with(final_payload_data, self.DUMMY_WA_ID, self.DUMMY_PHONE_ID)
        # mock_send_wpp é chamado DENTRO de _create_topdesk_ticket_and_reply
        mock_save.assert_not_called()
        mock_clear.assert_called_once_with(self.DUMMY_WA_ID)


    @patch('whatsapp_integration.send_whatsapp_message')
    @patch('whatsapp_integration.load_state')
    @patch('whatsapp_integration.save_state')
    def test_webhook_post_interactive_confirm_edit(self, mock_save_state, mock_load_state, mock_send_wpp):
        # Simula um estado onde o usuário está prestes a confirmar
        initial_state = {
            "pending_confirmation": True,
            "last_question_context": "confirmation_response",
            "pending_confirmation_payload": {"Cliente": "Antigo"},
            # ... outros campos do estado ...
        }
        mock_load_state.return_value = initial_state

        payload = self._get_sample_interactive_payload("confirm_edit", "Não, Corrigir")
        response = self.client.post('/webhook', json=payload)
        self.assertEqual(response.status_code, 200)

        # Verifica se o estado foi modificado para aguardar a correção
        expected_saved_state = initial_state.copy()
        expected_saved_state["pending_confirmation"] = False
        expected_saved_state["last_question_context"] = "awaiting_user_correction_text"
        expected_saved_state["last_asked_fields"] = None
        mock_save_state.assert_called_once_with(self.DUMMY_WA_ID, expected_saved_state)

        # Verifica se a mensagem correta foi enviada para o usuário
        mock_send_wpp.assert_called_once_with(
            phone_number_id=self.DUMMY_PHONE_ID,
            to_wa_id=self.DUMMY_WA_ID,
            message_body="Ok, por favor, digite APENAS a informação que deseja corrigir (ex: 'Cidade é Cuiabá', 'Preço Frete 500').",
            context_message_id=self.DUMMY_MSG_ID
        )

    @patch('whatsapp_integration.send_whatsapp_message')
    @patch('whatsapp_integration.load_state', return_value=None)
    @patch('whatsapp_integration.save_state')
    def test_webhook_post_text_message_orchestrator_exception(self, mock_save, mock_load, mock_send_wpp):
        mock_orchestration_agent_instance.process_user_input.side_effect = Exception("Erro no orchestrator")
        # mock_orchestration_agent_instance.get_state_dict.return_value = {} # O estado não será salvo de qualquer forma

        payload = self._get_sample_text_payload("Input que causa erro")
        with patch('whatsapp_integration.clear_state') as mock_clear:
            response = self.client.post('/webhook', json=payload)
            self.assertEqual(response.status_code, 200) # Retorna 200 para evitar reenvios do WPP
            mock_send_wpp.assert_called_once_with(
                phone_number_id=self.DUMMY_PHONE_ID,
                to_wa_id=self.DUMMY_WA_ID,
                message_body=ANY, # Mensagem de erro genérica
                context_message_id=self.DUMMY_MSG_ID
            )
            self.assertTrue("Desculpe, ocorreu um erro interno inesperado." in mock_send_wpp.call_args[1]['message_body'])
            mock_clear.assert_called_once_with(self.DUMMY_WA_ID)
            mock_save.assert_not_called()


    def test_webhook_post_agents_not_ready(self):
        with patch('whatsapp_integration.STATELESS_AGENTS_READY', False):
            payload = self._get_sample_text_payload("Qualquer coisa")
            response = self.client.post('/webhook', json=payload)
            self.assertEqual(response.status_code, 503)
            self.assertIn("Service temporarily unavailable", response.get_json()["message"])

    # Adicionar mais testes para:
    # - _create_topdesk_ticket_and_reply (sucesso e falha na criação do ticket)
    # - Diferentes tipos de mensagens não tratadas (ex: audio, video sem legenda)
    # - Falha na formatação do payload para TopDesk em _create_topdesk_ticket_and_reply
    # - Cenário onde `load_state` retorna um estado que o orchestrator usa.
    # - Outros status de resposta do orchestrator: needs_confirmation, aborted, error.

    @patch('whatsapp_integration.send_whatsapp_message')
    @patch('whatsapp_integration.format_final_summary_text', return_value="Resumo Formatado para TopDesk")
    def test_create_topdesk_ticket_and_reply_success(self, mock_formatter, mock_send_wpp):
        mock_topdesk_client_instance.create_incident.return_value = "TICKET_007"
        payload = {"key": "value", "Cadencia_Formatada": "Cad"}

        result = _create_topdesk_ticket_and_reply(payload, self.DUMMY_WA_ID, self.DUMMY_PHONE_ID)

        self.assertTrue(result)
        mock_formatter.assert_called_once_with(payload, "Cad")
        mock_topdesk_client_instance.create_incident.assert_called_once_with("Resumo Formatado para TopDesk")
        mock_send_wpp.assert_called_once_with(
            phone_number_id=self.DUMMY_PHONE_ID,
            to_wa_id=self.DUMMY_WA_ID,
            message_body="Chamado criado com sucesso! Número: TICKET_007"
        )

    @patch('whatsapp_integration.send_whatsapp_message')
    @patch('whatsapp_integration.format_final_summary_text', return_value="Resumo Formatado")
    def test_create_topdesk_ticket_and_reply_failure(self, mock_formatter, mock_send_wpp):
        mock_topdesk_client_instance.create_incident.return_value = None # Falha na criação
        payload = {"key": "value", "Cadencia_Formatada": "Cad"}

        result = _create_topdesk_ticket_and_reply(payload, self.DUMMY_WA_ID, self.DUMMY_PHONE_ID)

        self.assertFalse(result)
        mock_send_wpp.assert_called_once_with(
            phone_number_id=self.DUMMY_PHONE_ID,
            to_wa_id=self.DUMMY_WA_ID,
            message_body="Ocorreu um erro ao criar o chamado no TopDesk. A equipe responsável foi notificada."
        )


if __name__ == '__main__':
    unittest.main()