# tests/test_regression.py
import unittest
from datetime import datetime
import sys
import os
import re # Para extrair partes da mensagem
from langchain.memory import ConversationBufferMemory

# Adiciona o diretório raiz ao sys.path para encontrar os módulos em 'src' ou similar
# Ajuste o caminho se a estrutura do seu projeto for diferente
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, '..')) # Assume que 'tests' está no mesmo nível que 'agents', 'utils', etc.
sys.path.insert(0, project_root)

short_term_memory = ConversationBufferMemory(memory_key="history", return_messages=False)
user_id="1"

# Importa os novos agentes e configurações
try:
    from agents.extraction_agent import ExtractionAgent
    from agents.mapping_agent import MappingAgent
    from agents.orchestration_agent import OrchestrationAgent
    import config # Para o diretório de artifacts
except ImportError as e:
    print(f"Erro ao importar módulos dos agentes: {e}")
    print("Verifique se o PYTHONPATH está correto ou se os arquivos existem.")
    sys.exit(1)

class TestRegression(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Inicializa agentes stateless uma vez para toda a classe de teste."""
        print("Inicializando agentes stateless (Extraction, Mapping)...")
        try:
            # Certifique-se de que as variáveis de ambiente (ex: OPENAI_API_KEY) estejam acessíveis
            # para o ambiente de teste, se necessário.
            cls.extraction_agent = ExtractionAgent()
            cls.mapping_agent = MappingAgent(artifacts_dir=config.ARTIFACTS_DIR)
            cls.profile_manager = None # Inicializa sem o gerenciador de perfis
            if not cls.mapping_agent.data_loaded_successfully:
                 print("AVISO: Dados de mapeamento não carregados.")
        except Exception as e:
            print(f"ERRO CRÍTICO ao inicializar agentes stateless: {e}")
            # Decide como lidar, talvez pular testes se falhar
            raise unittest.SkipTest(f"Falha ao inicializar agentes stateless: {e}")

    def setUp(self):
        """Configura o necessário antes de cada teste individual."""
        # Cria uma NOVA instância do Orquestrador para cada teste, garantindo estado limpo
        self.orchestrator = OrchestrationAgent(self.extraction_agent, self.mapping_agent, self.profile_manager)
        self.hoje = datetime.now().strftime("%d/%m/%Y")
        print(f"\n--- Iniciando Teste: {self.id()} ---")

    def assert_message_content_equal(self, actual_message, expected_content):
        """
        Compara o conteúdo principal das mensagens, ignorando espaços extras
        e tratando a lista de campos faltantes de forma flexível (comparação de conjuntos).
        """
        actual_clean = "\n".join([line.strip() for line in actual_message.strip().splitlines() if line.strip()])
        expected_clean = "\n".join([line.strip() for line in expected_content.strip().splitlines() if line.strip()])

        # Verifica se é uma mensagem de campos faltantes
        if "Ainda precisamos das seguintes informações:" in actual_clean:
            # Extrai os campos faltantes (linhas que começam com '-')
            actual_missing_lines = {line.strip() for line in actual_clean.splitlines() if line.strip().startswith("-")}
            expected_missing_lines = {line.strip() for line in expected_clean.splitlines() if line.strip().startswith("-")}

            # Compara os conjuntos de linhas faltantes (ignora a ordem)
            self.assertSetEqual(actual_missing_lines, expected_missing_lines,
                              f"A lista de campos faltantes difere.\n"
                              f"--- ATUAL ---\n{actual_clean}\n"
                              f"--- ESPERADO ---\n{expected_clean}")
        elif "Por favor, resolva a ambiguidade abaixo:" in actual_clean or \
             "Sobre a Forma de Pagamento, encontrei estas opções:" in actual_clean or \
             "Encontrei múltiplos clientes que podem corresponder" in actual_clean or \
             "Encontrei múltiplos materiais que podem corresponder" in actual_clean:
            # Para mensagens de ambiguidade, compara o texto exato (após limpar espaços)
            # Poderíamos fazer uma análise mais profunda das opções se necessário, mas
            # comparar o texto formatado costuma ser suficiente aqui.
            self.assertEqual(actual_clean, expected_clean,
                             f"A mensagem de ambiguidade difere.\n"
                             f"--- ATUAL ---\n{actual_clean}\n"
                             f"--- ESPERADO ---\n{expected_clean}")
        else:
            # Para resumos finais ou outras mensagens, compara o texto exato
            self.assertEqual(actual_clean, expected_clean,
                             f"O conteúdo da mensagem difere.\n"
                             f"--- ATUAL ---\n{actual_clean}\n"
                             f"--- ESPERADO ---\n{expected_clean}")


    def run_test(self, inputs, expected_final_content):
        """
        Executa uma sequência de inputs através do OrchestrationAgent e
        verifica se o conteúdo da mensagem final corresponde ao esperado.
        """
        last_response_dict = {}
        final_message = ""

        print(f"Inputs do teste: {len(inputs)}")
        for i, user_input in enumerate(inputs):
            print(f"  Processando input {i+1}/{len(inputs)}: '{user_input[:50]}...'")
            # Chama o orquestrador da instância de teste
            response_dict = self.orchestrator.process_user_input(user_text=user_input,short_term_memory=short_term_memory, user_id=user_id)
            last_response_dict = response_dict
            final_message = response_dict.get("message", "ERRO: Mensagem não encontrada na resposta")
            status = response_dict.get("status")
            print(f"    Status retornado: {status}")
            # print(f"    Mensagem retornada: '{final_message[:100]}...'") # Debug opcional

            # Se for a última interação esperada pelo teste
            if i == len(inputs) - 1:
                print("  Última interação do teste.")
                if status == 'needs_confirmation':
                    # Extrai o corpo principal da mensagem de confirmação
                    # (Remove a pergunta final "Os dados estão corretos?")
                    match = re.match(r"(.*?)\n\nOs dados estão corretos\?", final_message, re.DOTALL | re.IGNORECASE)
                    if match:
                        actual_content_to_compare = match.group(1).strip()
                        # Compara com o conteúdo esperado (que deve ser o resumo + pergunta inicial)
                        self.assert_message_content_equal(actual_content_to_compare, expected_final_content)
                    else:
                        self.fail(f"Formato inesperado da mensagem 'needs_confirmation':\n{final_message}")
                elif status == 'needs_input':
                    # Compara a mensagem de solicitação diretamente
                    self.assert_message_content_equal(final_message, expected_final_content)
                else:
                    # Se o teste esperava terminar em needs_input ou needs_confirmation, mas recebeu outro status
                    self.fail(f"Status final inesperado '{status}' na última interação. Mensagem: {final_message}")
                return # Termina a verificação após a última interação

            # Se não for a última interação, verifica se o status permite continuar
            elif status not in ['needs_input', 'needs_confirmation']:
                 # O fluxo foi interrompido antes do esperado (erro, abortado, completado?)
                 self.fail(f"Fluxo interrompido prematuramente na interação {i+1} com status '{status}'. Mensagem: {final_message}")
                 return

        # Se o loop terminar sem retornar (ex: lista de inputs vazia?), falha.
        self.fail("O loop de teste terminou sem uma verificação final ser realizada.")


    # --- CASOS DE TESTE ADAPTADOS ---

    # CAMINHO FELIZ - TODOS OS CAMPOS PREENCHIDOS -> needs_confirmation
    def test_exemplo1_caminho_feliz(self):
        inputs = [
            """CNPJ/CPF: 040.074.561-51
Planta: PDL
Condição de Pagamento: 15 dias
Forma de Pagamento: boleto
Código do Material:  FS Ouro
Cadência: 40 fev 20 mar 58 abr
Vendedor: 5522999540302
Cidade: Lucas do Rio Verde
Data de Negociação: 10/03/2025
Incoterms: CIF
Preço Frete: 170
Valor: 2200""",
        ]
        expected_output = f"""Por favor, revise os dados abaixo antes de prosseguir:

Data da solicitação: {self.hoje}
Vendedor: 5522999540302
CNPJ/CPF: 04007456151
Cidade: Lucas do Rio Verde
Email do vendedor: None
Planta: PDL
Nome do cliente: VITOR SILVA RODRIGUES E OUTRO
Código do cliente: 403943
Campanha: SEM REF
Data da negociação: 10/03/2025
Condição de pagamento: Z015
Forma de pagamento: D
Incoterms: CIF
Preço frete: 170,00 (CIF)
Preço: 2200,00
Código do material: 300002
-- Cadência --
02.2025:40 ton
03.2025:20 ton
04.2025:58 ton""" # NOTE: Ano da cadência ajustado baseado na data negociação 10/03/2025
        self.run_test(inputs, expected_output)


    def test_exemplo2_falta_frete_cif(self):
        inputs = [
            """CNPJ/CPF: 040.074.561-51
Planta: PDL
Condição de Pagamento: 15 dias
Forma de Pagamento: boleto
Código do Material:  FS Ouro
Cadência: 40 fev 20 mar 58 abr
Vendedor: 5522999540302
Cidade: Lucas do Rio Verde
Data de Negociação: 10/03/2025
Incoterms: CIF
Valor: 2200""",
        ]
        expected_output = """Como o Incoterms é 'CIF', por favor, informe o Preço Frete."""
        self.run_test(inputs, expected_output)

    def test_exemplo3_falta_valor(self):
        inputs = [
            """CNPJ/CPF: 040.074.561-51
Planta: PDL
Condição de Pagamento: 15 dias
Forma de Pagamento: boleto
Código do Material: FS Ouro
Cadência: 40 fev 20 mar 58 abr
Vendedor: 5522999540302
Cidade: Lucas do Rio Verde
Data de Negociação: 10/03/2025
Incoterms: CIF
Preço Frete: 170""",
        ]
        expected_output = """Quase lá! Ainda preciso destas informações: Valor."""
        self.run_test(inputs, expected_output)

    def test_exemplo4_falta_incoterms(self):
        inputs = [
            """CNPJ/CPF: 040.074.561-51
Planta: PDL
Condição de Pagamento: 15 dias
Forma de Pagamento: boleto
Código do Material:  FS Ouro
Cadência: 40 fev 20 mar 58 abr
Vendedor: 5522999540302
Cidade: Lucas do Rio Verde
Data de Negociação: 10/03/2025
Preço Frete: 170
Valor: 2200""",
        ]
        expected_output = """Quase lá! Ainda preciso destas informações: Incoterms."""
        self.run_test(inputs, expected_output)

    def test_exemplo5_falta_cidade(self):
        inputs = [
            """CNPJ/CPF: 040.074.561-51
Planta: PDL
Condição de Pagamento: 15 dias
Forma de Pagamento: boleto
Código do Material: FS Ouro
Cadência: 40 fev 20 mar 58 abr
Vendedor: 5522999540302
Data de Negociação: 10/03/2025
Incoterms: CIF
Preço Frete: 170
Valor: 2200""",
        ]
        expected_output = """Quase lá! Ainda preciso destas informações: Cidade."""
        self.run_test(inputs, expected_output)

    def test_exemplo6_falta_cliente_id(self):
        inputs = [
            """Planta: PDL
Condição de Pagamento: 15 dias
Forma de Pagamento: boleto
Código do Material: FS Ouro
Cadência: 40 fev 20 mar 58 abr
Vendedor: 5522999540302
Cidade: Lucas do Rio Verde
Data de Negociação: 10/03/2025
Incoterms: CIF
Preço Frete: 170
Valor: 2200""",
        ]
        expected_output = """Quase lá! Ainda preciso destas informações: CNPJ/CPF, Nome do cliente (Nome é obrigatório se cliente for novo e não tiver CPF/CNPJ)."""
        self.run_test(inputs, expected_output)

    def test_exemplo7_faltando_varios(self):
        inputs = [
            """Cliente: Nelson Pelle
Produto: FS Ouro
Cidade: Tapurah - MT
Quantidade Total: 2.100
Cadência:
CADÊNCIA LRV
fev/25 1050
mar/25 1050
Preço: R$960
Prazo de pagamento: 15 dias
Frete: 55
Data de negociação: 27/01/2025""", # Removido Tipo Negociação e Tributação que não são mapeados
        ]
        expected_output = """Quase lá! Ainda preciso destas informações: Forma de Pagamento, Incoterms, Vendedor."""
        self.run_test(inputs, expected_output)


    def test_exemplo8_cliente_nao_cadastrado(self):
        inputs = [
            """Cliente: Tarik Amaral Farah
Produto: FS Ouro
Cidade: POXOREU - MT
Quantidade Total: 300 TONS
CADÊNCIA PDL
FEV/25 100
Mar/25 100
abr/25 100
FRETE: FOB
Preço: R$ 1.000,00
Prazo de pagamento: 15 DIAS
Data de negociação: 04/02/2025""",
        ]
        expected_output = """Quase lá! Ainda preciso destas informações: CNPJ/CPF, Forma de Pagamento, Vendedor."""
        self.run_test(inputs, expected_output)

    def test_exemplo9_multi_turn_resolve(self):
        inputs = [
            """Cliente: Nelson Pelle
Produto: FS Ouro
Cidade: Tapurah - MT
Quantidade Total: 2.100
Cadência:
CADÊNCIA LRV
fev/25 1050
mar/25 1050
Preço: R$960
Prazo de pagamento: 15 dias
Frete: 55
Data de negociação: 27/01/2025""",
            """Forma de Pagamento: boleto
Planta: LRV
Incoterms: CIF
Vendedor: 5522999540302"""
        ]
        expected_output = f"""Por favor, revise os dados abaixo antes de prosseguir:

Data da solicitação: {self.hoje}
Vendedor: 5522999540302
CNPJ/CPF: 65568583153
Cidade: Tapurah - MT
Email do vendedor: None
Planta: LRV
Nome do cliente: NELSON PELLE JUNIOR E OUTROS
Código do cliente: 400545
Campanha: SEM REF
Data da negociação: 27/01/2025
Condição de pagamento: Z015
Forma de pagamento: D
Incoterms: CIF
Preço frete: 55,00 (CIF)
Preço: 960,00
Código do material: 300002
-- Cadência --
02.2025:1050 ton
03.2025:1050 ton"""
        self.run_test(inputs, expected_output)

    def test_exemplo10_ambiguity_detection(self):
        inputs = [
            """CNPJ/CPF: 040.074.561-51
Planta: PDL
Condição de Pagamento: 15 dias
Forma de Pagamento: antecipação
Código do Material: FS Ouro
Cadência: 40 fev 20 mar 58 abr
Vendedor: 5522999540302
Cidade: Lucas do Rio Verde
Data de Negociação: 10/03/2025
Incoterms: CIF
Preço Frete: 170
Valor: 2200""",
        ]
        expected_output = """Para a Forma de Pagamento 'antecipação', qual destas opções você se refere (encontradas por similaridade)?
1. AR - Antecipação Risco Sacado (Código: A)
2. AR - Antecipação Fundo (Código: P)
(Responda com o número ou o código)"""
        self.run_test(inputs, expected_output)

    def test_exemplo11_ambiguity_resolution(self):
        inputs = [
            """CNPJ/CPF: 040.074.561-51
Planta: PDL
Condição de Pagamento: 15 dias
Forma de Pagamento: antecipação
Código do Material: FS Ouro
Cadência: 40 fev 20 mar 58 abr
Vendedor: 5522999540302
Cidade: Lucas do Rio Verde
Data de Negociação: 10/03/2025
Incoterms: CIF
Preço Frete: 170
Valor: 2200""",
            """2""" # Ou "P" ou "2"
        ]
        expected_output = f"""Por favor, revise os dados abaixo antes de prosseguir:

Data da solicitação: {self.hoje}
Vendedor: 5522999540302
CNPJ/CPF: 04007456151
Cidade: Lucas do Rio Verde
Email do vendedor: None
Planta: PDL
Nome do cliente: VITOR SILVA RODRIGUES E OUTRO
Código do cliente: 403943
Campanha: SEM REF
Data da negociação: 10/03/2025
Condição de pagamento: Z015
Forma de pagamento: P
Incoterms: CIF
Preço frete: 170,00 (CIF)
Preço: 2200,00
Código do material: 300002
-- Cadência --
02.2025:40 ton
03.2025:20 ton
04.2025:58 ton""" # NOTE: Ano da cadência ajustado
        self.run_test(inputs, expected_output)

    def test_exemplo12_cpf_puro(self):
        inputs = [
            """CPF: 040.074.561-51
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
Valor: 2200"""
        ]
        expected_output = f"""Por favor, revise os dados abaixo antes de prosseguir:

Data da solicitação: {self.hoje}
Vendedor: 5522999540302
CNPJ/CPF: 04007456151
Cidade: Lucas do Rio Verde
Email do vendedor: None
Planta: PDL
Nome do cliente: VITOR SILVA RODRIGUES E OUTRO
Código do cliente: 403943
Campanha: SEM REF
Data da negociação: 10/03/2025
Condição de pagamento: Z015
Forma de pagamento: D
Incoterms: CIF
Preço frete: 170,00 (CIF)
Preço: 2200,00
Código do material: 300002
-- Cadência --
02.2025:40 ton
03.2025:20 ton
04.2025:58 ton""" # NOTE: Ano da cadência ajustado
        self.run_test(inputs, expected_output)

    def test_exemplo13_fob_sem_frete(self):
        inputs = [
            """CNPJ/CPF: 040.074.561-51
Planta: PDL
Condição de Pagamento: 15 dias
Forma de Pagamento: boleto
Código do Material: FS Ouro
Cadência: 40 fev 20 mar 58 abr
Vendedor: 5522999540302
Cidade: Lucas do Rio Verde
Data de Negociação: 10/03/2025
Incoterms: FOB
# Preço Frete: (Omitido)
Valor: 2200""",
        ]
        expected_output = f"""Por favor, revise os dados abaixo antes de prosseguir:

Data da solicitação: {self.hoje}
Vendedor: 5522999540302
CNPJ/CPF: 04007456151
Cidade: Lucas do Rio Verde
Email do vendedor: None
Planta: PDL
Nome do cliente: VITOR SILVA RODRIGUES E OUTRO
Código do cliente: 403943
Campanha: SEM REF
Data da negociação: 10/03/2025
Condição de pagamento: Z015
Forma de pagamento: D
Incoterms: FOB
Preço frete: N/A (FOB)
Preço: 2200,00
Código do material: 300002
-- Cadência --
02.2025:40 ton
03.2025:20 ton
04.2025:58 ton""" # NOTE: Ano da cadência ajustado
        self.run_test(inputs, expected_output)

    def test_exemplo14_fob_com_frete(self):
        inputs = [
            """CNPJ/CPF: 040.074.561-51
Planta: PDL
Condição de Pagamento: 15 dias
Forma de Pagamento: boleto
Código do Material: FS Ouro
Cadência: 40 fev 20 mar 58 abr
Vendedor: 5522999540302
Cidade: Lucas do Rio Verde
Data de Negociação: 10/03/2025
Incoterms: FOB
Preço Frete: 170
Valor: 2200""",
        ]
        expected_output = f"""Por favor, revise os dados abaixo antes de prosseguir:

Data da solicitação: {self.hoje}
Vendedor: 5522999540302
CNPJ/CPF: 04007456151
Cidade: Lucas do Rio Verde
Email do vendedor: None
Planta: PDL
Nome do cliente: VITOR SILVA RODRIGUES E OUTRO
Código do cliente: 403943
Campanha: SEM REF
Data da negociação: 10/03/2025
Condição de pagamento: Z015
Forma de pagamento: D
Incoterms: FOB
Preço frete: 170,00 (FOB - Valor Informativo)
Preço: 2200,00
Código do material: 300002
-- Cadência --
02.2025:40 ton
03.2025:20 ton
04.2025:58 ton""" # NOTE: Ano da cadência ajustado
        self.run_test(inputs, expected_output)

    def test_exemplo15_cliente_nao_encontrado_resolve(self):
        inputs = [
            """Cliente: Cliente Fantasma LTDA
Planta: SRS
Condição de Pagamento: A vista
Forma de Pagamento: Pix
Código do Material: FS Umido Super
Cadência: 100 MAR/25
Vendedor: 5511987654321
Cidade: Cuiabá
Data de Negociação: 15/03/2025
Incoterms: CIF
Preço Frete: 200
Valor: 5000""",
            """CNPJ/CPF: 99.999.999/0001-99
            Forma de pagamento: doc"""
        ]
        expected_output = f"""Por favor, revise os dados abaixo antes de prosseguir:

Data da solicitação: {self.hoje}
Vendedor: 5511987654321
CNPJ/CPF: 99999999000199
Cidade: Cuiabá
Email do vendedor: None
Planta: SRS
Nome do cliente: Cliente Fantasma LTDA
Código do cliente: None
Campanha: SEM REF
Data da negociação: 15/03/2025
Condição de pagamento: Z000
Forma de pagamento: N
Incoterms: CIF
Preço frete: 200,00 (CIF)
Preço: 5000,00
Código do material: 300141
-- Cadência --
03.2025:100 ton"""
        self.run_test(inputs, expected_output)


    def test_exemplo16_cadencia_multi_linha_valor_mes_ano(self):
        inputs = [
            """Cliente: Cliente: Roberto rodrigues Junqueira
            Produto:  FS umido
            Cidade: colider
            Quantidade Total: 360
            Cadência:
            30 tons 04/25
            30 tons 05/25
            30 tons 06/25
            30 tons 07/25
            30 tons 08/25
            30 tons 09/25
            30 tons 10/25
            30 tons 11/25
            30 tons 12/25
            30 tons 01/26
            30 tons 02/26
            30 tons 03/26
            Preço: R$365
            Prazo de pagamento: 15 dias
            Frete:
            Data de negociação: 01/04/25""",
            """Forma de pagamento: boleto
            incoterms: FOB
            Planta: PDL
            Vendedor: 5511999998888""" # CNPJ fictício para o novo cliente
        ]
        expected_output = f"""Por favor, revise os dados abaixo antes de prosseguir:

Data da solicitação: {self.hoje}
Vendedor: 5511999998888
CNPJ/CPF: 56085117604
Cidade: colider
Email do vendedor: None
Planta: PDL
Nome do cliente: ROBERTO RODRIGUES JUNQUEIRA .
Código do cliente: 400315
Campanha: SEM REF
Data da negociação: 01/04/25
Condição de pagamento: Z015
Forma de pagamento: D
Incoterms: FOB
Preço frete: N/A (FOB)
Preço: 365,00
Código do material: 300003
-- Cadência --
04.2025:30 ton
05.2025:30 ton
06.2025:30 ton
07.2025:30 ton
08.2025:30 ton
09.2025:30 ton
10.2025:30 ton
11.2025:30 ton
12.2025:30 ton
01.2026:30 ton
02.2026:30 ton
03.2026:30 ton"""
        self.run_test(inputs, expected_output)

    def test_exemplo17_condicao_forma_juntas(self):
        """Testa extração e mapeamento com condição e forma juntas ("boleto - 30 dias")."""
        inputs = [
            """Pedido de Venda (Nutrição Animal):
Cliente: Getúlio Gonçalves Viana
Incoterms: FOB
Produto ouro
Cidade: laciara - GO
Tipo de Negociação: PREÇO FIXO
Tributação (PIS/COFINS): Pj
Quantidade Total: Tons. 1.500
Planta: sor
Cadência
JUN/25 30 tons
cpf	3682098992
Preço: R$ 1.730,91
Prazo de pagamento: boleto - 30 dias
Frete: 360
Data de negociação: 24/04/2025
Vendedor: VICTOR VIDAL
Email do vendedor: victor.vidal@fs.agr.br"""
        ]
        expected_output = f"""Por favor, revise os dados abaixo antes de prosseguir:

Data da solicitação: {self.hoje}
Vendedor: VICTOR VIDAL
CNPJ/CPF: 3682098992
Cidade: Laciara - GO
Email do vendedor: victor.vidal@fs.agr.br
Planta: sor
Nome do cliente: GETULIO GONCALVES VIANA
Código do cliente: 404972
Campanha: SEM REF
Data da negociação: 24/04/2025
Condição de pagamento: Z030
Forma de pagamento: D
Incoterms: FOB
Preço frete: 360,00 (FOB - Valor Informativo)
Preço: 1730,91
Código do material: 300002
-- Cadência --
06.2025:30 ton"""
        self.run_test(inputs, expected_output)
    
    def test_novo_caso_1_paschoal_pontieri(self):
        inputs = [
            """Vendedor: Victor Vidal
e-mail victor.vidal@fs.agr.br
Cliente: Paschoal José Pontieri e Outros
CPF: 07945741000275
Produto: FS OURO
Cidade: Itápolis - SP
Tipo de Negociação: PREÇO FIXO
Planta PDL
Cadência:	 Outubro/2025 - 190 ton
Novembro/2025 - 190 ton
Dezembro/2025 - 190 ton
Janeiro/2026 - 190 ton
Fevereiro/2026 - 190 ton
Março/2026 - 190 ton
Abril/2026 - 190 ton
Maio/2026 - 190 ton
Junho/2026 - 190 ton
Julho/2026 - 190 ton
Preço: R$ 1.350,00
Prazo de pagamento: boleto 28 dias
Frete: R$ 420,00 (cif)
Data de negociação: 05/05/2025"""
        ]
        expected_output = f"""Por favor, revise os dados abaixo antes de prosseguir:

Data da solicitação: {self.hoje}
Vendedor: Victor Vidal
CNPJ/CPF: 07945741000275
Cidade: Itápolis - SP
Email do vendedor: victor.vidal@fs.agr.br
Planta: PDL
Nome do cliente: PASCHOAL JOSE PONTIERI E OUTRO
Código do cliente: 109562
Campanha: SEM REF
Data da negociação: 05/05/2025
Condição de pagamento: Z028
Forma de pagamento: D
Incoterms: CIF
Preço frete: 420,00 (CIF)
Preço: 1350,00
Código do material: 300002
-- Cadência --
10.2025:190 ton
11.2025:190 ton
12.2025:190 ton
01.2026:190 ton
02.2026:190 ton
03.2026:190 ton
04.2026:190 ton
05.2026:190 ton
06.2026:190 ton
07.2026:190 ton"""
        self.run_test(inputs, expected_output)

    def test_novo_caso_2_polioeste(self):
        inputs = [
            """Pedido de Venda (Nutrição Animal):
Vendedor: Pedro Savio
e-mail: Pedro.savio@fs.agr.br
Planta: PDL
Cliente: Polioeste
Cód. Cliente (opcional): 108281
I.E (Se cliente novo):29.191.948/0001-53
Produto: fs essencial
Cidade: São Miguel do Oeste
Tipo de Negociação: normal
Cadência: 500 toneladas jun/25
500 tons jul/25
Preço: 1.580,00
Prazo de pagamento: 30 dias boleto
Frete: fob
Incoterm: fob
Data de negociação: 02/05/2025"""
        ]
        expected_output = f"""Por favor, revise os dados abaixo antes de prosseguir:

Data da solicitação: {self.hoje}
Vendedor: Pedro Savio
CNPJ/CPF: 29191948000153
Cidade: São Miguel do Oeste
Email do vendedor: Pedro.savio@fs.agr.br
Planta: PDL
Nome do cliente: Polioeste
Código do cliente: 108281
Campanha: SEM REF
Data da negociação: 02/05/2025
Condição de pagamento: Z030
Forma de pagamento: D
Incoterms: FOB
Preço frete: N/A (FOB)
Preço: 1580,00
Código do material: 300004
-- Cadência --
06.2025:500 ton
07.2025:500 ton"""
        self.run_test(inputs, expected_output)

    def test_novo_caso_3_romualdo_dearo(self):
        inputs = [
            """Pedido de Venda (Nutrição Animal):
Vendedor: Herculano Franco
e-mail: herculano.franco@fs.agr.br
Cliente: Romualdo Dearo da Silva
Cód. Cliente (opcional): 400814
I.E (Se cliente novo): 01679072129
Produto: FS Essencial
Cidade: Gaucha do norte- MT
Tipo de Negociação: PREÇO FIXO
planta: PDL
Cadencia Maio/25 30 toneladas
Jun/25 30 toneladas
Preço: R$ 1.500,00
Prazo de pagamento: 15 dias Boleto
Frete: fob
Data de negociação: 23/04/2025"""
        ]
        expected_output = f"""Por favor, revise os dados abaixo antes de prosseguir:

Data da solicitação: {self.hoje}
Vendedor: Herculano Franco
CNPJ/CPF: 01679072129
Cidade: Gaucha do norte- MT
Email do vendedor: herculano.franco@fs.agr.br
Planta: PDL
Nome do cliente: ROMUALDO DEARO DA SILVA
Código do cliente: 400814
Campanha: SEM REF
Data da negociação: 23/04/2025
Condição de pagamento: Z015
Forma de pagamento: D
Incoterms: FOB
Preço frete: N/A (FOB)
Preço: 1500,00
Código do material: 300004
-- Cadência --
05.2025:30 ton
06.2025:30 ton"""
        self.run_test(inputs, expected_output)

    def test_novo_caso_4_fernando_nemi_costa(self):
        inputs = [
            """Cliente: Fernando Nemi Costa e Outro
Produto: FS OURO
Cidade: Irapuã - SP
Tipo de Negociação: PREÇO FIXO
Tributação (PIS/COFINS): PF
Quantidade Total: Tons. 1.850 TONS
Planta: sor
CADÊNCIA
Junho/2025 - 500 ton
Julho/2025 - 450 ton
Agosto/2025 - 450 ton
Setembro/2025 - 450 ton


Preço: R$ 1.370,00
Prazo de pagamento: boleto 28 dias
Frete: R$ 500,00 cif
Data de negociação: 05/05/2025
Vendedor: VICTOR VIDAL
Email do vendedor: victor.vidal@fs.agr.br"""
        ]
        expected_output = f"""Por favor, revise os dados abaixo antes de prosseguir:

Data da solicitação: {self.hoje}
Vendedor: VICTOR VIDAL
CNPJ/CPF: 08514915000208
Cidade: Irapuã - SP
Email do vendedor: victor.vidal@fs.agr.br
Planta: sor
Nome do cliente: FERNANDO NEMI COSTA E OUTRO
Código do cliente: 102695
Campanha: SEM REF
Data da negociação: 05/05/2025
Condição de pagamento: Z028
Forma de pagamento: D
Incoterms: CIF
Preço frete: 500,00 (CIF)
Preço: 1370,00
Código do material: 300002
-- Cadência --
06.2025:500 ton
07.2025:450 ton
08.2025:450 ton
09.2025:450 ton"""
        self.run_test(inputs, expected_output)

    def test_novo_caso_5_silva_tomaz(self):
        inputs = [
            """Cliente: SILVA & TOMAZ
Cód. Cliente (opcional):
I.E (Se cliente novo):
Produto: FS ESSENCIAL
Cidade: Água Azul do Norte - PA
Tipo de Negociação: FIXO NORMAL
Tributação (PIS/COFINS): PF
Quantidade Total: 315
Cadência
90 tons 06/25
45 tons 07/25
45 tons 08/25
45 tons 09/25
45 tons 10/25
45 tons 11/25
Planta pdl
Preço: R$1.905
Prazo de pagamento: boleto 15 dias
Frete: 490
Data de negociação: 02/05/25
Incoterms: CIF
Email do vendedor:
herculano.franco@fs.agr.br
Vendedor:
HERCULANO NETO"""
        ]
        expected_output = f"""Por favor, revise os dados abaixo antes de prosseguir:

Data da solicitação: {self.hoje}
Vendedor: HERCULANO NETO
CNPJ/CPF: 34170111000168
Cidade: Água Azul do Norte - PA
Email do vendedor: herculano.franco@fs.agr.br
Planta: PDL
Nome do cliente: SILVA & TOMAZ LTDA
Código do cliente: 109536
Campanha: SEM REF
Data da negociação: 02/05/25
Condição de pagamento: Z015
Forma de pagamento: D
Incoterms: CIF
Preço frete: 490,00 (CIF)
Preço: 1905,00
Código do material: 300004
-- Cadência --
06.2025:90 ton
07.2025:45 ton
08.2025:45 ton
09.2025:45 ton
10.2025:45 ton
11.2025:45 ton"""
        self.run_test(inputs, expected_output)

    def test_novo_caso_6_cadencia_parenteses(self):
        inputs = [
            """Vendedor Reginaldo Campos
e-mail: reginaldo.campos@fs.agr.br
Cliente: JORGE ALBERTO HILDEBRAND GONZA
Cód. cliente:  401520
IE:13.451.764/0001-45
Produto: FS Essencial
Cidade: São Pedro/MG
Tipo Negociação: Fixa
Planta: 163/MT
Quant. total: 150 t
Cadência: maio/25(50 tons)
junho/25 (50 tons)
julho/25 (50 tons)
Preço: R$ 2.025,00/t
Prazo pagamento: boleto 15 dias
Frete: CIF (R$ 500,00)"""
        ]
        expected_output = f"""Por favor, revise os dados abaixo antes de prosseguir:

Data da solicitação: {self.hoje}
Vendedor: Reginaldo Campos
CNPJ/CPF: 13451764000145
Cidade: São Pedro/MG
Email do vendedor: reginaldo.campos@fs.agr.br
Planta: 163/MT
Nome do cliente: JORGE ALBERTO HILDEBRAND GONZA
Código do cliente: 401520
Campanha: SEM REF
Data da negociação: {self.hoje}
Condição de pagamento: Z015
Forma de pagamento: D
Incoterms: CIF
Preço frete: 500,00 (CIF)
Preço: 2025,00
Código do material: 300004
-- Cadência --
05.2025:50 ton
06.2025:50 ton
07.2025:50 ton"""
        self.run_test(inputs, expected_output)

    def test_novo_caso_7_ted(self):
        inputs = [
            """Vendedor: Herculano Nato
e-mail: herculano.franco@fs.agr.br
Cliente: SANTA IZABEL AGROPÁSTORIL COMERCIO E INDUSTRIA LTDA
CPF: 000.111.222-33
Cód. Cliente (opcional): 111097
I.E (Se cliente novo): 0238252000143
Produto: FS Essencial
Cidade: SINOP
Tipo de Negociação: PREÇO FIXO
Cadência:
Jun/25 50 toneladas
Jul/25 50 toneladas
Ago/25 50 toneladas
Set/25 50 toneladas
Out/25 50 toneladas
Nov/25 50 toneladas
Planta: lrv
Preço R$ 1.460,00
Prazo de pagamento: ted a vista
Frete: fob R$ 0,00
Data de negociação: 02/05/2025"""
        ]
        expected_output = f"""Por favor, revise os dados abaixo antes de prosseguir:

Data da solicitação: {self.hoje}
Vendedor: Herculano Nato
CNPJ/CPF: 00011122233
Cidade: SINOP
Email do vendedor: herculano.franco@fs.agr.br
Planta: LRV
Nome do cliente: SANTA IZABEL AGROPASTORIL COMERCIO E INDUSTRIA LTDA
Código do cliente: 111097
Campanha: SEM REF
Data da negociação: 02/05/2025
Condição de pagamento: Z000
Forma de pagamento: N
Incoterms: FOB
Preço frete: N/A (FOB)
Preço: 1460,00
Código do material: 300004
-- Cadência --
06.2025:50 ton
07.2025:50 ton
08.2025:50 ton
09.2025:50 ton
10.2025:50 ton
11.2025:50 ton"""
        self.run_test(inputs, expected_output)

    def test_exemplo_octavio_celso_multi_turn(self):
        inputs = [
            """Pedido de Venda (Nutrição Animal)
Cliente: OCTAVIO CELSO PACHECO DE ALMEIDA PRADO NETO
Cód. Cliente (opcional):
I.E: (Se cliente novo):
Produto: FS Essencial
Cidade: colider
Tipo de negociação: PREÇO FIXO
Tributação (PIS/COFINS)): PF
Quantidade Total: Tons. 10
Planta SRS
Cadência
Maio/25 10

Preço: R$ 1.500.00
Prazo de pagamento: 7 dias Boleto
Frete:
Data de negociação: 06/05/2025""",
            """CPF: 000.111.222.33-44
Incoterms: FOB
Vendedor: X"""
        ]
        expected_final_output = f"""Por favor, revise os dados abaixo antes de prosseguir:

Data da solicitação: {self.hoje}
Vendedor: X
CNPJ/CPF: 0001112223344
Cidade: colider
Email do vendedor: None
Planta: SRS
Nome do cliente: OCTAVIO CELSO PACHECO DE ALMEIDA PRADO NETO
Código do cliente: None
Campanha: SEM REF
Data da negociação: 06/05/2025
Condição de pagamento: Z007
Forma de pagamento: D
Incoterms: FOB
Preço frete: N/A (FOB)
Preço: 1500,00
Código do material: 300004
-- Cadência --
05.2025:10 ton"""
        self.run_test(inputs, expected_final_output)

    def test_novo_caso_luis_carssola(self):
        inputs = [
            """Pedido de Venda (Nutrição Animal):
Cliente: LUIS CARSSOLA
CNPJ/CPF: 097.579.458-20
Produto: FS OURO
Cidade: CÁCERES – MT
Tipo de Negociação: PREÇO FIXO
pLANTA LRV
Quantidade Total: 270 TONS
CADÊNCIA
JUL/25: 45 TONS
AGO/25: 45 TONS
SET/25: 45 TONS
OUT/25: 45 TONS
NOV/25: 45 TONS
DEZ/25:45 TONS
FRETE: 200,00 CIF
Preço: R$ 1.260,00
Prazo de pagamento: 15 DIAS - BOLETO
Data de negociação: 07/05/2025
Vendedor: GUSTAVO OLIVEIRA
Email do vendedor: gustavo.oliveira@fs.agr.br"""
        ]
        expected_output = f"""Por favor, revise os dados abaixo antes de prosseguir:

Data da solicitação: {self.hoje}
Vendedor: GUSTAVO OLIVEIRA
CNPJ/CPF: 09757945820
Cidade: CÁCERES – MT
Email do vendedor: gustavo.oliveira@fs.agr.br
Planta: LRV
Nome do cliente: LUIZ CASSORLA
Código do cliente: 400621
Campanha: SEM REF
Data da negociação: 07/05/2025
Condição de pagamento: Z015
Forma de pagamento: D
Incoterms: CIF
Preço frete: 200,00 (CIF)
Preço: 1260,00
Código do material: 300002
-- Cadência --
07.2025:45 ton
08.2025:45 ton
09.2025:45 ton
10.2025:45 ton
11.2025:45 ton
12.2025:45 ton"""
        self.run_test(inputs, expected_output)

    def test_novo_caso_carpec(self):
        inputs = [
            """Vendedor: Reginaldo Campos
e-mail: reginaldo.campos@fs.agr.br
Cliente: Carpec
Cód. cliente: 107436
CNPJ: 19.445.733/0005-91
Produto: FS Essencial
Cidade: Carmo do Parnaíba/MG
Tipo Negociação: Fixa
Planta: PDL/MT
Quant. total: 900 t
Cadência: maio (300 t), junho (300 t) e julho (300 t)
Preço: R$ 1.820,00/t
Prazo pagamento: 21 dias boleto
Frete: CIF (R$ 400,00)
Data negociação: 13/05/25"""
        ]
        expected_output = f"""Por favor, revise os dados abaixo antes de prosseguir:

Data da solicitação: {self.hoje}
Vendedor: Reginaldo Campos
CNPJ/CPF: 19445733000591
Cidade: Carmo do Parnaíba/MG
Email do vendedor: reginaldo.campos@fs.agr.br
Planta: PDL
Nome do cliente: COOPERATIVA AGRO PECUARIA DE C
Código do cliente: 107436
Campanha: SEM REF
Data da negociação: 13/05/25
Condição de pagamento: Z021
Forma de pagamento: D
Incoterms: CIF
Preço frete: 400,00 (CIF)
Preço: 1820,00
Código do material: 300004
-- Cadência --
05.2025:300 ton
06.2025:300 ton
07.2025:300 ton"""
        self.run_test(inputs, expected_output)

    def test_cpf_duplicado(self):
        inputs = [
            """Cliente: ANDRE DE MORAES ZUCATO DE MORAIS
Cód. Cliente (opcional):
I.E (Se cliente novo):
Produto: FS OURO
Cidade: ALTA FLORESTA – MT
Tipo de Negociação: FIXO NORMAL
Tributação (PIS/COFINS): PF
Quantidade Total: 135
Cadência:
Planta: LRV
mai/25  135 TONS

Preço: R$ 1.100,00
Prazo de pagamento: 30 dias de boleto
Frete: R$ 160,00 CIF
Data de negociação: 20/05/25

Vendedor: VICTOR VIDAL
Email do vendedor: victor.vidal@fs.agr.br
CNPJ/CPF: 695.916.791-49""",
            """1"""
        ]
        expected_output = f"""Por favor, revise os dados abaixo antes de prosseguir:

Data da solicitação: {self.hoje}
Vendedor: VICTOR VIDAL
CNPJ/CPF: 69591679149
Cidade: ALTA FLORESTA – MT
Email do vendedor: victor.vidal@fs.agr.br
Planta: LRV
Nome do cliente: ANDRE DE MORAES ZUCATO E OUTRO
Código do cliente: 404335
Campanha: SEM REF
Data da negociação: 20/05/25
Condição de pagamento: Z030
Forma de pagamento: D
Incoterms: CIF
Preço frete: 160,00 (CIF)
Preço: 1100,00
Código do material: 300002
-- Cadência --
05.2025:135 ton"""
        self.run_test(inputs, expected_output)

    def test_frete_tpd(self):
        inputs = [
            """Cód. Cliente (opcional): 108335
CNPJ: 17.717.233/0001-02
Produto: FS Essencial
Cidade: Ibirairas- RS
Tipo de Negociação: normal
Qunaitdade Total: normal
Quantidade Total: 45 ton
Cadência: 45 tons maio/2025
Preço: 1.975,00
Prazo de pagamento: boleto 30 dias
Incoterm: TPD
Data de negociação: 15/05
Planta: LRV
Vendedor: Pedro Savio"""
        ]
        expected_output = f"""Por favor, revise os dados abaixo antes de prosseguir:

Data da solicitação: {self.hoje}
Vendedor: Pedro Savio
CNPJ/CPF: 17717233000102
Cidade: Ibirairas- RS
Email do vendedor: None
Planta: LRV
Nome do cliente: AGRO CECCHIN LTDA
Código do cliente: 108335
Campanha: SEM REF
Data da negociação: 15/05
Condição de pagamento: Z030
Forma de pagamento: D
Incoterms: TPD
Preço frete: N/A (TPD)
Preço: 1975,00
Código do material: 300004
-- Cadência --
05.2025:45 ton"""
        self.run_test(inputs, expected_output)

    def test_frete_tpd(self):
        inputs = [
            """Cód. Cliente (opcional): 108335
CNPJ: 17.717.233/0001-02
Produto: FS Essencial
Cidade: Ibirairas- RS
Tipo de Negociação: normal
Qunaitdade Total: normal
Quantidade Total: 45 ton
Cadência: 45 tons maio/2025
Preço: 1.975,00
Prazo de pagamento: boleto 30 dias
Incoterm: TPD
Data de negociação: 15/05
Planta: LRV
Vendedor: Pedro Savio"""
        ]
        expected_output = f"""Por favor, revise os dados abaixo antes de prosseguir:

Data da solicitação: {self.hoje}
Vendedor: Pedro Savio
CNPJ/CPF: 17717233000102
Cidade: Ibirairas- RS
Email do vendedor: None
Planta: LRV
Nome do cliente: AGRO CECCHIN LTDA
Código do cliente: 108335
Campanha: SEM REF
Data da negociação: 15/05
Condição de pagamento: Z030
Forma de pagamento: D
Incoterms: TPD
Preço frete: N/A (TPD)
Preço: 1975,00
Código do material: 300004
-- Cadência --
05.2025:45 ton"""
        self.run_test(inputs, expected_output)

    def test_tab(self):
        inputs = [
            """Pedido de Venda (Nutrição Animal)	
Cliente: MARCOS ANTONIO ASSI TOZZATTI	
Cód. Cliente (opcional):	
Produto: FS OURO	
Cidade: NOVA LACERDA – MT 	
Tipo de Negociação: PREÇO FIXO	
Tributação (PIS/COFINS): PF	
Quantidade Total: 225 TONS	
CADÊNCIA LRV/SRS	
FRETE: CIF 215,00	
Preço: R$ 1.300,00	
Prazo de pagamento: 15 DIAS boleto	
Data de negociação: 03/06/2025	
07.2025:	45
08.2025:	45
09.2025:	45
10.2025:	45
11.2025:	45

Vendedor:	GUSTAVO OLIVEIRA
Email do vendedor:	gustavo.oliveira@fs.agr.br
CNPJ/CPF:	313.334.781-00
Data da solicitação:	04/06/2025""", "LRV"
        ]
        expected_output = f"""Por favor, revise os dados abaixo antes de prosseguir:

Data da solicitação: {self.hoje}
Vendedor: GUSTAVO OLIVEIRA
CNPJ/CPF: 31333478100
Cidade: NOVA LACERDA
Email do vendedor: gustavo.oliveira@fs.agr.br
Planta: LRV
Nome do cliente: MARCOS ANTONIO ASSI TOZZATTI
Código do cliente: 402227
Campanha: SEM REF
Data da negociação: 03/06/2025
Condição de pagamento: Z015
Forma de pagamento: D
Incoterms: CIF
Preço frete: 215,00 (CIF)
Preço: 1300,00
Código do material: 300002
-- Cadência --
07.2025:45 ton
08.2025:45 ton
09.2025:45 ton
10.2025:45 ton
11.2025:45 ton"""
        self.run_test(inputs, expected_output)

    def test_cadence_with_semicolon(self):
        inputs = [
            """Vendedor: Reginaldo Campos
e-mail: reginaldo.campos@fs.agr.br
Cliente: Cooperativa Agraria Mista de Castelo
Cód. Cliente (opcional): 106020
CNPJ: 27.443.308/0010-59
Produto: FS Essencial 
Cidade: Castelo-ES 
Tipo de Negociação: normal  
Quantidade Total: 2.100 ton 
Cadência: Junho/25 (300 tons); Julho/25 (300 tons); Agosto/25 (300 tons) Setembro/25 (300 tons) outubro/25 (300 tons) Novembro/25 (300 tons) Dezembro/25 (300 tons) 
Frete: CIF  (600,00)
Planta de Saída: PDL
Preço: R$ 1930,00 
Prazo de pagamento: Boleto 30 dias
Data de negociação: 13/06/2025"""
        ]
        expected_output = f"""Por favor, revise os dados abaixo antes de prosseguir:

Data da solicitação: {self.hoje}
Vendedor: Reginaldo Campos
CNPJ/CPF: 27443308001059
Cidade: Castelo-ES
Email do vendedor: reginaldo.campos@fs.agr.br
Planta: PDL
Nome do cliente: COOPERATIVA AGRARIA MISTA DE\xa0CASTELO
Código do cliente: 106020
Campanha: SEM REF
Data da negociação: 13/06/2025
Condição de pagamento: Z030
Forma de pagamento: D
Incoterms: CIF
Preço frete: 600,00 (CIF)
Preço: 1930,00
Código do material: 300004
-- Cadência --
06.2025:300 ton
07.2025:300 ton
08.2025:300 ton
09.2025:300 ton
10.2025:300 ton
11.2025:300 ton
12.2025:300 ton"""
        self.run_test(inputs, expected_output)




if __name__ == '__main__':
    # Configura logging básico para ver saídas dos agentes durante os testes
    import logging
    logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
    # Roda os testes
    unittest.main(verbosity=2) # verbosity=2 mostra mais detalhes