import unittest
from unittest.mock import patch, MagicMock, mock_open
import os
import json
import html
from requests.auth import HTTPBasicAuth
from requests.exceptions import HTTPError, RequestException

# Adicione o diretório src ao sys.path para que os módulos possam ser importados
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from topdesk_client import TopDeskClient, DEFAULT_CALLER_ID, DEFAULT_CATEGORY_ID, \
                           DEFAULT_SUBCATEGORY_ID, DEFAULT_CALLTYPE_ID, DEFAULT_IMPACT_ID, \
                           DEFAULT_URGENCY_ID, DEFAULT_OPERATOR_ID, DEFAULT_OPERATOR_GROUP_ID, \
                           TOPDESK_API_URL


class TestTopDeskClient(unittest.TestCase):

    def setUp(self):
        # Mock das variáveis de ambiente para os testes
        self.mock_env_vars = {
            "TOPDESK_USERNAME": "test_user",
            "TOPDESK_PASSWORD": "test_password"
        }
        self.patcher = patch.dict(os.environ, self.mock_env_vars)
        self.patcher.start()
        self.client = TopDeskClient()

    def tearDown(self):
        self.patcher.stop()

    def test_init_with_env_vars(self):
        self.assertEqual(self.client.username, "test_user")
        self.assertEqual(self.client.password, "test_password")
        self.assertEqual(self.client.base_url, TOPDESK_API_URL)

    def test_init_with_args(self):
        client = TopDeskClient(username="arg_user", password="arg_password", base_url="http://test.url")
        self.assertEqual(client.username, "arg_user")
        self.assertEqual(client.password, "arg_password")
        self.assertEqual(client.base_url, "http://test.url")

    def test_init_missing_credentials(self):
        # Temporariamente remove as variáveis de ambiente
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "Credenciais do TopDesk .* não fornecidas"):
                TopDeskClient()

    def test_get_auth(self):
        auth = self.client._get_auth()
        self.assertIsInstance(auth, HTTPBasicAuth)
        self.assertEqual(auth.username, "test_user")
        self.assertEqual(auth.password, "test_password")

    def test_build_payload(self):
        request_text = "Linha 1\nLinha 2 com < & >"
        expected_request_html = "Linha 1<br>Linha 2 com &lt; &amp; &gt;"
        payload = self.client._build_payload(request_text)

        self.assertEqual(payload["caller"]["id"], DEFAULT_CALLER_ID)
        self.assertEqual(payload["status"], "firstLine")
        self.assertEqual(payload["briefDescription"], "PREÇO FIXO")
        self.assertEqual(payload["request"], expected_request_html)
        self.assertEqual(payload["category"]["id"], DEFAULT_CATEGORY_ID)
        self.assertEqual(payload["subcategory"]["id"], DEFAULT_SUBCATEGORY_ID)
        self.assertEqual(payload["entryType"]["name"], "Portal")
        self.assertEqual(payload["callType"]["id"], DEFAULT_CALLTYPE_ID)
        self.assertEqual(payload["impact"]["id"], DEFAULT_IMPACT_ID)
        self.assertEqual(payload["urgency"]["id"], DEFAULT_URGENCY_ID)
        self.assertEqual(payload["processingStatus"]["name"], "Registrado")
        self.assertEqual(payload["operator"]["id"], DEFAULT_OPERATOR_ID)
        self.assertEqual(payload["operatorGroup"]["id"], DEFAULT_OPERATOR_GROUP_ID)

    @patch('topdesk_client.requests.post')
    def test_create_incident_success(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 201 # Ou 200, dependendo da API
        mock_response.json.return_value = {"id": "incident_id_123", "number": "T2024_001"}
        mock_post.return_value = mock_response

        request_text = "Teste de incidente"
        ticket_number = self.client.create_incident(request_text)

        self.assertEqual(ticket_number, "T2024_001")
        expected_payload = self.client._build_payload(request_text)
        mock_post.assert_called_once_with(
            TOPDESK_API_URL,
            headers={'Content-Type': 'application/json'},
            json=expected_payload,
            auth=self.client._get_auth()
        )
        mock_response.raise_for_status.assert_called_once()

    @patch('topdesk_client.requests.post')
    def test_create_incident_http_error(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = "Bad Request"
        # Configurar raise_for_status para levantar um HTTPError com este mock_response
        mock_response.raise_for_status.side_effect = HTTPError(response=mock_response)
        mock_post.return_value = mock_response

        ticket_number = self.client.create_incident("Teste com erro HTTP")
        self.assertIsNone(ticket_number)

    @patch('topdesk_client.requests.post')
    def test_create_incident_request_exception(self, mock_post):
        mock_post.side_effect = RequestException("Erro de conexão")

        ticket_number = self.client.create_incident("Teste com erro de request")
        self.assertIsNone(ticket_number)

    @patch('topdesk_client.requests.post')
    def test_create_incident_success_no_ticket_number_in_response(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"id": "incident_id_123", "message": "Criado mas sem numero"} # Sem 'number'
        mock_post.return_value = mock_response

        ticket_number = self.client.create_incident("Teste sem numero no retorno")
        self.assertIsNone(ticket_number)
        mock_response.raise_for_status.assert_called_once()

    @patch('topdesk_client.requests.post')
    def test_create_incident_unexpected_exception(self, mock_post):
        mock_post.side_effect = Exception("Erro inesperado genérico")

        ticket_number = self.client.create_incident("Teste com erro genérico")
        self.assertIsNone(ticket_number)

if __name__ == '__main__':
    unittest.main()