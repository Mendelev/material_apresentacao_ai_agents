# tests/test_regression.py
import unittest
from datetime import datetime
import sys
import os
import re # Para extrair partes da mensagem

# Adiciona o diretório raiz ao sys.path para encontrar os módulos em 'src' ou similar
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, '..'))
sys.path.insert(0, project_root)

try:
    from agents.extraction_agent import ExtractionAgent
    from agents.mapping_agent import MappingAgent
    from agents.orchestration_agent import OrchestrationAgent
    from utils.transcription import AudioTranscriber # <<< NOVO
    import config
except ImportError as e:
    print(f"Erro ao importar módulos dos agentes: {e}")
    print("Verifique se o PYTHONPATH está correto ou se os arquivos existem.")
    sys.exit(1)

class TestRegression(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Inicializa agentes stateless uma vez para toda a classe de teste."""
        print("Inicializando agentes stateless (Extraction, Mapping, AudioTranscriber)...") # <<< ATUALIZADO
        try:
            cls.extraction_agent = ExtractionAgent()
            cls.mapping_agent = MappingAgent(artifacts_dir=config.ARTIFACTS_DIR)
            cls.audio_transcriber = AudioTranscriber() # <<< NOVO
            if not config.OPENAI_API_KEY:
                print("AVISO: OPENAI_API_KEY não configurada. Testes de áudio podem falhar ou ser pulados.")
                # Poderia definir cls.audio_transcriber = None e pular testes de áudio

            if not cls.mapping_agent.data_loaded_successfully:
                 print("AVISO: Dados de mapeamento não carregados.")
        except Exception as e:
            print(f"ERRO CRÍTICO ao inicializar agentes stateless: {e}")
            raise unittest.SkipTest(f"Falha ao inicializar agentes stateless: {e}")

    def setUp(self):
        """Configura o necessário antes de cada teste individual."""
        self.orchestrator = OrchestrationAgent(self.extraction_agent, self.mapping_agent)
        self.hoje = datetime.now().strftime("%d/%m/%Y")
        print(f"\n--- Iniciando Teste: {self.id()} ---")

    def assert_message_content_equal(self, actual_message, expected_content):
        # ... (seu método assert_message_content_equal permanece o mesmo) ...
        actual_clean = "\n".join([line.strip() for line in actual_message.strip().splitlines() if line.strip()])
        expected_clean = "\n".join([line.strip() for line in expected_content.strip().splitlines() if line.strip()])

        # Verifica se é uma mensagem de campos faltantes
        # Ajuste para o novo texto de pergunta de campos faltantes
        if "Quase lá! Ainda preciso destas informações:" in actual_clean or "Como o Incoterms é" in actual_clean or "Não consegui entender o formato da Cadência" in actual_clean or "Por favor, corrija a informação:" in actual_clean :
            # Extrai os campos faltantes (linhas que começam com '-') ou a mensagem toda
            # Para simplificar, vamos comparar a mensagem inteira após limpeza,
            # já que a ordem e o fraseado das mensagens de "campos faltantes" podem ser importantes.
            self.assertEqual(actual_clean, expected_clean,
                              f"A mensagem de campos faltantes/inválidos difere.\n"
                              f"--- ATUAL ---\n{actual_clean}\n"
                              f"--- ESPERADO ---\n{expected_clean}")
        elif "Por favor, resolva a ambiguidade abaixo:" in actual_clean or \
             "Para a Forma de Pagamento" in actual_clean or \
             "Encontrei múltiplos clientes que podem corresponder" in actual_clean or \
             "Encontrei múltiplos materiais que podem corresponder" in actual_clean or \
             "A forma de pagamento" in actual_clean: # Adicionado para cobrir variações
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
        Executa uma sequência de inputs (texto) através do OrchestrationAgent e
        verifica se o conteúdo da mensagem final corresponde ao esperado.
        """
        # ... (seu método run_test permanece o mesmo, mas agora será chamado por run_test_with_audio) ...
        last_response_dict = {}
        final_message = ""

        print(f"Inputs do teste (texto): {len(inputs)}")
        for i, user_input in enumerate(inputs):
            print(f"  Processando input textual {i+1}/{len(inputs)}: '{user_input[:50]}...'")
            response_dict = self.orchestrator.process_user_input(user_input)
            last_response_dict = response_dict
            final_message = response_dict.get("message", "ERRO: Mensagem não encontrada na resposta")
            status = response_dict.get("status")
            print(f"    Status retornado: {status}")

            if i == len(inputs) - 1:
                print("  Última interação do teste (texto).")
                if status == 'needs_confirmation':
                    # O seu regex aqui é um pouco diferente do que está no orchestrator.
                    # O orchestrator tem:
                    # "Por favor, revise os dados abaixo antes de prosseguir:\n\n"
                    # f"{formatted_summary}\n\n"
                    # "Os dados estão corretos? Responda 'Sim' para confirmar, 'Não' para cancelar, "
                    # "ou digite a informação que deseja corrigir (ex: 'Preço Frete é 500', 'Cidade é Cuiabá')."

                    # Vamos ajustar o regex para capturar o summary
                    expected_summary_prefix = "Por favor, revise os dados abaixo antes de prosseguir:"
                    # Remove a pergunta final
                    actual_content_to_compare = final_message.split("\n\nOs dados estão corretos?")[0].strip()

                    # Garante que o esperado também comece com o prefixo e remove a pergunta genérica
                    expected_content_clean = expected_final_content.split("\n\nOs dados estão corretos?")[0].strip()


                    self.assert_message_content_equal(actual_content_to_compare, expected_content_clean)

                elif status == 'needs_input':
                    self.assert_message_content_equal(final_message, expected_final_content)
                else:
                    self.fail(f"Status final inesperado '{status}' na última interação. Mensagem: {final_message}")
                return

            elif status not in ['needs_input', 'needs_confirmation']:
                 self.fail(f"Fluxo interrompido prematuramente na interação {i+1} com status '{status}'. Mensagem: {final_message}")
                 return
        self.fail("O loop de teste (texto) terminou sem uma verificação final ser realizada.")

    # --- NOVO MÉTODO AUXILIAR PARA TESTES COM ÁUDIO ---
    def run_test_with_audio(self, audio_file_paths: list[str], expected_final_content: str):
        """
        Executa uma sequência de arquivos de áudio através do OrchestrationAgent,
        transcrevendo-os primeiro, e verifica o conteúdo da mensagem final.
        audio_file_paths: Lista de caminhos para os arquivos de áudio.
        """
        if not self.audio_transcriber:
            self.skipTest("AudioTranscriber não inicializado, pulando teste de áudio.")

        text_inputs = []
        print(f"Arquivos de áudio para o teste: {len(audio_file_paths)}")
        for i, audio_path_relative in enumerate(audio_file_paths):
            # Constrói o caminho absoluto para o arquivo de áudio
            audio_full_path = os.path.join(project_root, audio_path_relative)
            print(f"  Processando arquivo de áudio {i+1}/{len(audio_file_paths)}: '{audio_full_path}'")
            if not os.path.exists(audio_full_path):
                self.fail(f"Arquivo de áudio não encontrado: {audio_full_path}")

            with open(audio_full_path, 'rb') as f_audio:
                audio_bytes = f_audio.read()

            transcribed_text = self.audio_transcriber.transcribe_audio(audio_bytes, filename=os.path.basename(audio_full_path))
            if transcribed_text is None:
                self.fail(f"Falha ao transcrever o áudio: {audio_full_path}")

            print(f"    Texto transcrito: '{transcribed_text[:100]}...'")
            text_inputs.append(transcribed_text)

        # Agora que todos os áudios foram transcritos para texto,
        # usa o método run_test original.
        self.run_test(text_inputs, expected_final_content)

    # --- SEUS CASOS DE TESTE EXISTENTES (TEXTUAIS) ---
    # ... (mantenha todos os seus `test_exemploX_...` aqui) ...
    # Exemplo:


    # --- NOVOS CASOS DE TESTE COM ÁUDIO ---

    # Cenário 1: Áudio com todos os dados corretos
    def test_audio_caminho_feliz_completo(self):
        audio_paths = [
            'tests/audio_fixtures/input1.wav'
        ]
        expected_output = f"""Por favor, revise os dados abaixo antes de prosseguir:

Data da solicitação: {self.hoje}
Vendedor: X
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
04.2025:58 ton"""
        self.run_test_with_audio(audio_paths, expected_output)

    # Cenário 2: Áudio com dados faltando, valida a pergunta
    def test_audio_falta_varios_cif(self):
        audio_paths = [
            'tests/audio_fixtures/input2_parte1.wav' 
        ]
        expected_output = """Quase lá! Ainda preciso destas informações: Forma de Pagamento, Incoterms, Vendedor."""
        self.run_test_with_audio(audio_paths, expected_output)

    # Cenário 3: Múltiplos áudios para completar os dados
    def test_audio_multi_turn_resolve(self):
        audio_paths = [
            'tests/audio_fixtures/input2_parte1.wav', # Substitua
            'tests/audio_fixtures/input2_parte2.wav'  # Substitua
        ]
        expected_output = f"""Por favor, revise os dados abaixo antes de prosseguir:
Data da solicitação: {self.hoje}
Vendedor: X
CNPJ/CPF: 65568583153
Cidade: Tapurá
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
        self.run_test_with_audio(audio_paths, expected_output)

#     def test_input3(self):
#         audio_paths = [
#             'tests/audio_fixtures/input3_parte1.wav', 
#             'tests/audio_fixtures/input3_opcao.wav' 
#         ]
#         expected_output = f"""Por favor, revise os dados abaixo antes de prosseguir:
# Data da solicitação: {self.hoje}
# Vendedor: X
# CNPJ/CPF: 65568583153
# Cidade: Tapurá
# Email do vendedor: None
# Planta: LRV
# Nome do cliente: NELSON PELLE JUNIOR E OUTROS
# Código do cliente: 400545
# Campanha: SEM REF
# Data da negociação: 27/01/2025
# Condição de pagamento: Z015
# Forma de pagamento: D
# Incoterms: CIF
# Preço frete: 55,00 (CIF)
# Preço: 960,00
# Código do material: 300002
# -- Cadência --
# 02.2025:1050 ton
# 03.2025:1050 ton"""
#         self.run_test_with_audio(audio_paths, expected_output)

    def test_input4(self):
        audio_paths = [
            'tests/audio_fixtures/input4_part1.wav', 
            'tests/audio_fixtures/input4_part2.wav' 
        ]
        expected_output = f"""Por favor, revise os dados abaixo antes de prosseguir:
Data da solicitação: {self.hoje}
Vendedor: X
CNPJ/CPF: 99999999000199
Cidade: Cuiabá
Email do vendedor: None
Planta: SRS
Nome do cliente: Cliente Fantasma LTDA
Código do cliente: None
Campanha: SEM REF
Data da negociação: 15-03-2025
Condição de pagamento: Z000
Forma de pagamento: N
Incoterms: CIF
Preço frete: 200,00 (CIF)
Preço: 5000,00
Código do material: 300141
-- Cadência --
03.2025:100 ton"""
        self.run_test_with_audio(audio_paths, expected_output)

    def test_input5(self):
        audio_paths = [
            'tests/audio_fixtures/input5_part1.wav', 
            'tests/audio_fixtures/input5_part2.wav' 
        ]
        expected_output = f"""Por favor, revise os dados abaixo antes de prosseguir:
Data da solicitação: {self.hoje}
Vendedor: Victor Vidal
CNPJ/CPF: 3682098992
Cidade: Laceara
Email do vendedor: victor.vidal.fs.agr.br
Planta: SOR
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
        self.run_test_with_audio(audio_paths, expected_output)

    def test_input6(self):
        audio_paths = [
            'tests/audio_fixtures/input6.wav'
        ]
        expected_output = f"""Por favor, revise os dados abaixo antes de prosseguir:

Data da solicitação: {self.hoje}
Vendedor: Herculano Nato
CNPJ/CPF: 0238252000143
Cidade: Sinope
Email do vendedor: herculano.franco@fs.agr.br
Planta: LRV
Nome do cliente: SANTA IZABEL AGROPASTORIL COMERCIO E INDUSTRIA LTDA
Código do cliente: 111097
Campanha: SEM REF
Data da negociação: 2 de maio de 2025
Condição de pagamento: Z000
Forma de pagamento: N
Incoterms: FOB
Preço frete: 0,00 (FOB - Valor Informativo)
Preço: 1460,00
Código do material: 300004
-- Cadência --
06.2025:50 ton
07.2025:50 ton
08.2025:50 ton
09.2025:50 ton
10.2025:50 ton
11.2025:50 ton"""
        self.run_test_with_audio(audio_paths, expected_output)

    def test_input7(self):
        audio_paths = [
            'tests/audio_fixtures/input7.wav'
        ]
        expected_output = f"""Por favor, revise os dados abaixo antes de prosseguir:

Data da solicitação: {self.hoje}
Vendedor: Gustavo Oliveira
CNPJ/CPF: 09757945820
Cidade: Cáceres
Email do vendedor: gustavo.oliveira.afs.agr.br
Planta: LRV
Nome do cliente: LUIZ CASSORLA
Código do cliente: 400621
Campanha: SEM REF
Data da negociação: 7 de maio de 2025
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
        self.run_test_with_audio(audio_paths, expected_output)

    def test_input8(self):
        audio_paths = [
            'tests/audio_fixtures/input8.wav'
        ]
        expected_output = f"""Por favor, revise os dados abaixo antes de prosseguir:

Data da solicitação: {self.hoje}
Vendedor: Reginaldo Campos
CNPJ/CPF: 0803766300192
Cidade: Charqueada
Email do vendedor: reginaldo.campos.fs.agr.br
Planta: LRV
Nome do cliente: Isabel Altenfelder Santos Bordin
Código do cliente: 111062
Campanha: SEM REF
Data da negociação: 16/04/2025
Condição de pagamento: Z015
Forma de pagamento: D
Incoterms: CIF
Preço frete: 490,00 (CIF)
Preço: 1930,00
Código do material: 300004
-- Cadência --
06.2025:50 ton"""
        self.run_test_with_audio(audio_paths, expected_output)

    def test_input9(self):
        audio_paths = [
            'tests/audio_fixtures/input9.wav'
        ]
        expected_output = f"""Por favor, revise os dados abaixo antes de prosseguir:

Data da solicitação: {self.hoje}
Vendedor: Pedro Savio
CNPJ/CPF: 1082812919194800053
Cidade: São Miguel do Oeste
Email do vendedor: pedro.savio.fs.agr.pr
Planta: PDL
Nome do cliente: Poli Oeste
Código do cliente: 108281
Campanha: SEM REF
Data da negociação: 2/05/2025
Condição de pagamento: Z030
Forma de pagamento: D
Incoterms: FOB
Preço frete: N/A (FOB)
Preço: 1580,00
Código do material: 300004
-- Cadência --
06.2025:500 ton
07.2025:500 ton"""
        self.run_test_with_audio(audio_paths, expected_output)

    # Adicione mais testes de áudio conforme necessário...


if __name__ == '__main__':
    import logging
    # Configuração do logging para debug, se necessário
    # logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(name)s - %(module)s - %(funcName)s - %(lineno)d - %(message)s')
    logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
    unittest.main(verbosity=2)