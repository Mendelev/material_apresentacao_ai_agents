import streamlit as st
import os
import sys
import time
import logging
from streamlit_mic_recorder import mic_recorder

logger = logging.getLogger(__name__) 
logging.basicConfig(
    level=logging.DEBUG,  # Mude para logging.INFO se quiser menos verbosidade
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    # handlers=[logging.StreamHandler(sys.stdout)] # Opcional: Forçar para stdout
)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Importações
try:
    from agents.extraction_agent import ExtractionAgent
    from agents.mapping_agent import MappingAgent
    from agents.orchestration_agent import OrchestrationAgent
    from utils.transcription import AudioTranscriber
    import config
except ImportError as e:
    st.error(f"Erro ao importar módulos: {e}.")
    st.stop()

# --- Cache ---
@st.cache_resource
def load_stateless_components():
    # ... (código existente para carregar extraction, mapping, transcriber) ...
    print("--- Carregando Agente de Extração (cacheado) ---")
    extraction_agent = None
    try:
        extraction_agent = ExtractionAgent()
    except Exception as e:
        st.error(f"Falha ao carregar Agente de Extração: {e}")

    print("--- Carregando Agente de Mapeamento (cacheado) ---")
    mapping_agent = None
    try:
        mapping_agent = MappingAgent(artifacts_dir=config.ARTIFACTS_DIR)
        if not mapping_agent.data_loaded_successfully:
             st.warning("Agente de Mapeamento inicializado, mas houve falha ao carregar os arquivos CSV.")
    except Exception as e:
        st.error(f"Falha ao carregar Agente de Mapeamento: {e}")

    print("--- Carregando Transcritor de Áudio (cacheado) ---")
    audio_transcriber = None
    try:
        if config.OPENAI_API_KEY:
            audio_transcriber = AudioTranscriber()
        else:
            st.warning("Chave da API OpenAI não configurada. Transcrição de áudio desabilitada.")
    except Exception as e:
        st.error(f"Falha ao carregar AudioTranscriber: {e}")

    return extraction_agent, mapping_agent, audio_transcriber

# --- Inicialização do App ---
st.set_page_config(page_title="Chatbot de Pedidos", layout="wide")
st.title("🤖 Chatbot de Processamento de Pedidos")
st.caption("Use este chat para inserir dados via texto, áudio ou upload.")

extraction_agent, mapping_agent, audio_transcriber = load_stateless_components()

# --- Gerenciamento de Estado ---
# ESSENCIAL: Inicializa as variáveis de estado PENDING
if "messages" not in st.session_state:
    st.session_state.messages = [{"role": "assistant", "content": "Olá! Informe os dados do pedido."}]
if "orchestrator" not in st.session_state:
    if extraction_agent and mapping_agent:
        st.session_state.orchestrator = OrchestrationAgent(extraction_agent, mapping_agent)
    else:
        st.session_state.orchestrator = None
if 'run_id' not in st.session_state:
    st.session_state.run_id = 0
# Novas variáveis para desacoplar widget de processamento
if 'pending_upload_data' not in st.session_state:
    st.session_state.pending_upload_data = None
if 'pending_mic_data' not in st.session_state:
    st.session_state.pending_mic_data = None
if 'pending_text_input' not in st.session_state:
    st.session_state.pending_text_input = None
# Flag para indicar se o processamento ocorreu nesta execução
if 'input_processed_flag' not in st.session_state:
    st.session_state.input_processed_flag = False


# --- Exibição do Histórico ---
# (É importante exibir o histórico antes dos widgets de input)
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# --- Sidebar ---
with st.sidebar:
    st.header("Instruções")
    # ... (markdown das instruções) ...
    st.markdown("""
        1.  **Insira os Dados:**
            *   Digite na caixa abaixo.
            *   OU clique no microfone (🎤).
            *   OU use o Uploader abaixo para enviar um arquivo de áudio (`wav`, `mp3`, etc.).
        2.  **Responda:** Se necessário, responda ao bot por texto, áudio ou upload.
        3.  **Confirme:** Revise o resumo final e responda 'Sim' ou 'Não'.
        4.  **Novo Pedido:** Use o botão abaixo para limpar o histórico.
    """)
    st.divider()
    if st.button("✨ Iniciar Novo Pedido"):
        # Limpa o estado específico da sessão
        st.session_state.messages = [{"role": "assistant", "content": "Ok, vamos começar um novo pedido. Informe os dados."}]
        if st.session_state.get("orchestrator"):
            st.session_state.orchestrator._reset_state_data()
        # Limpa os estados pendentes
        st.session_state.pending_upload_data = None
        st.session_state.pending_mic_data = None
        st.session_state.pending_text_input = None
        st.session_state.input_processed_flag = False
        st.session_state.run_id += 1
        print("--- Estado resetado via botão ---")
        st.rerun()

    st.divider()
    st.header("Upload de Áudio (Teste)")
    # File Uploader - Apenas DEFINE o estado pendente
    uploaded_file = st.file_uploader(
        "Enviar arquivo de áudio",
        type=['wav', 'mp3', 'm4a', 'ogg', 'aac', 'flac'],
        key=f'file_uploader_{st.session_state.run_id}'
    )
    # Se um novo arquivo for carregado, armazena seus dados para processamento posterior
    if uploaded_file is not None:
        # CORREÇÃO AQUI: Compara pelo NOME, não por um ID inexistente
        # Armazena se não houver nada pendente OU se o nome do arquivo atual
        # for DIFERENTE do nome do arquivo já pendente.
        if st.session_state.pending_upload_data is None or \
           st.session_state.pending_upload_data.get("name") != uploaded_file.name:
            logger.info(f"Arquivo '{uploaded_file.name}' detectado pelo uploader. Armazenando em pending_upload_data.")
            try:
                # Lê os bytes aqui para armazenar
                file_bytes = uploaded_file.read()
                st.session_state.pending_upload_data = {
                    "bytes": file_bytes,
                    "name": uploaded_file.name
                    # Removido o campo "id"
                }
                logger.debug(f"pending_upload_data definido com nome: {uploaded_file.name}")
                # Opcional: Adicionar um st.rerun() aqui pode forçar a lógica de consumo
                # a rodar imediatamente, mas pode causar piscadas na UI. Testar sem primeiro.
                # st.rerun()
            except Exception as e:
                 logger.error(f"Erro ao ler bytes do arquivo '{uploaded_file.name}': {e}", exc_info=True)
                 st.error(f"Erro ao tentar ler o arquivo {uploaded_file.name}.")
                 st.session_state.pending_upload_data = None # Limpa se a leitura falhar

# --- Widgets de Input (Mic e Texto) ---
# Mic Recorder - Apenas DEFINE o estado pendente
if audio_transcriber and st.session_state.orchestrator:
    audio_info = mic_recorder(
        start_prompt="🎤 Gravar",
        stop_prompt="⏹️ Parar",
        just_once=False, # Permite gravar novamente sem refresh completo
        use_container_width=True,
        key=f'mic_recorder_widget_{st.session_state.run_id}'
    )
    if audio_info and isinstance(audio_info, dict) and 'bytes' in audio_info:
        audio_bytes = audio_info['bytes']
        # Verifica se o áudio é novo (evita reprocessar bytes vazios ou o mesmo áudio repetidamente)
        if audio_bytes and st.session_state.pending_mic_data is None:
             logger.info(f"Áudio gravado ({len(audio_bytes)} bytes) detectado. Armazenando em pending_mic_data.")
             st.session_state.pending_mic_data = {"bytes": audio_bytes}
             # NÃO processa aqui

elif not audio_transcriber:
    st.warning("Gravação/Transcrição de áudio desabilitada (verifique API Key).")

# Chat Input - Apenas DEFINE o estado pendente
prompt = st.chat_input("Digite os dados ou sua resposta aqui...")
if prompt and st.session_state.pending_text_input is None:
    logger.info(f"Texto '{prompt}' detectado pelo chat_input. Armazenando em pending_text_input.")
    st.session_state.pending_text_input = prompt
    # NÃO processa aqui

# --- Lógica Central de Processamento (Executa UMA VEZ por ciclo de interação) ---
input_to_process = None
input_source = None
source_info = None # Para logging e display (ex: nome do arquivo)

# Verifica e CONSOME o input pendente (prioridade: upload > mic > texto)
if st.session_state.pending_text_input:
    logger.info("Processando pending_text_input...")
    input_source = 'text'
    input_to_process = st.session_state.pending_text_input
    st.session_state.pending_text_input = None # CONSOME o input
    st.session_state.input_processed_flag = True
    source_info = "texto digitado"

elif st.session_state.pending_upload_data:
    logger.info("Processando pending_upload_data...")
    input_source = 'file'
    upload_data = st.session_state.pending_upload_data
    st.session_state.pending_upload_data = None # CONSOME o input
    st.session_state.input_processed_flag = True # Marca que processamos algo
    source_info = upload_data['name']
    if audio_transcriber:
        with st.spinner(f"Transcrevendo arquivo '{source_info}'..."):
            input_to_process = audio_transcriber.transcribe_audio(upload_data['bytes'], filename=source_info)
            if not input_to_process:
                 st.error(f"Falha ao transcrever '{source_info}'.")
                 logger.warning(f"Transcrição falhou para upload {source_info}")
    else:
        st.error("Transcritor não disponível para processar upload.")

elif st.session_state.pending_mic_data:
    logger.info("Processando pending_mic_data...")
    input_source = 'audio'
    mic_data = st.session_state.pending_mic_data
    st.session_state.pending_mic_data = None # CONSOME o input
    st.session_state.input_processed_flag = True
    source_info = "áudio gravado"
    if audio_transcriber:
        with st.spinner("Transcrevendo áudio gravado..."):
             input_to_process = audio_transcriber.transcribe_audio(mic_data['bytes'], filename="mic_audio.wav")
             if not input_to_process:
                  st.error("Falha ao transcrever áudio gravado.")
                  logger.warning("Transcrição falhou para áudio gravado")
    else:
        st.error("Transcritor não disponível para processar áudio gravado.")

# Só executa a lógica do orchestrator se um input válido foi consumido e processado
if input_to_process and input_source:
    logger.info(f"Input consumido e pronto para o Orchestrator. Fonte: {input_source}, Info: {source_info}")

    # Mostra input do usuário no chat ANTES de chamar o bot
    display_content = input_to_process
    if input_source == "audio":
        display_content = f"(Áudio🎙️): {input_to_process}"
    elif input_source == "file":
        display_content = f"(Arquivo 📁 '{source_info}'): {input_to_process}"
    # Adiciona ao histórico ANTES de exibir, para manter ordem
    st.session_state.messages.append({"role": "user", "content": display_content})

    # Exibe na interface (Atualização da UI)
    with st.chat_message("user"):
        st.write(display_content)

    # Chama o Orchestrator
    if st.session_state.orchestrator:
        # --- DEBUG ADICIONAL: Logar estado ANTES de chamar process_user_input ---
        logger.debug(f"Orchestrator state ANTES de processar '{input_source}': {st.session_state.orchestrator.get_state_dict()}")
        # -------------------------------------------------------------------------
        with st.spinner("Processando..."):
            try:
                response = st.session_state.orchestrator.process_user_input(input_to_process)
                bot_message_content = response.get('message', "Desculpe, ocorreu um erro interno.")
                status = response.get("status", "error")
                # --- DEBUG ADICIONAL: Logar estado DEPOIS de chamar process_user_input ---
                logger.debug(f"Orchestrator state DEPOIS de processar '{input_source}': {st.session_state.orchestrator.get_state_dict()}")
                logger.debug(f"Orchestrator response: Status={status}, Msg='{bot_message_content[:100]}...'")
                # --------------------------------------------------------------------------
            except Exception as e:
                bot_message_content = f"Erro durante processamento pelo chatbot: {e}"
                status = "error"
                logger.error(f"Erro no orchestrator.process_user_input: {e}", exc_info=True)

        # Mostra resposta do bot
        display_message = bot_message_content
        if status == "confirmed_for_creation":
             display_message = "Pedido confirmado! (Simulando criação de chamado)."

        with st.chat_message("assistant"):
            resposta_formatada_md = display_message.replace('\n', '  \n')
            st.markdown(resposta_formatada_md)
        st.session_state.messages.append({"role": "assistant", "content": display_message})

        # Reset do estado para fluxos terminais
        if status in ["completed", "aborted", "confirmed_for_creation", "error"]:
             st.info(f"Processo finalizado (status: {status}). Pronto para novo pedido.")
             if st.session_state.get("orchestrator"):
                 st.session_state.orchestrator._reset_state_data()
             st.session_state.pending_upload_data = None
             st.session_state.pending_mic_data = None
             st.session_state.pending_text_input = None
             st.session_state.input_processed_flag = False
             st.session_state.run_id += 1
             logger.info(f"Estado resetado após status terminal: {status}. Forçando rerun.")
             st.rerun()

    else:
        error_msg = "Chatbot indisponível (erro de inicialização)."
        with st.chat_message("assistant"): st.error(error_msg)
        st.session_state.messages.append({"role": "assistant", "content": error_msg})

# --- FIM DA LÓGICA DE PROCESSAMENTO ---

# Reseta o flag de processamento ao final de cada execução completa do script
# Isso garante que na próxima interação (se não for terminal), o sistema esteja
# pronto para detectar um NOVO input.
st.session_state.input_processed_flag = False

# Adiciona espaço no final
st.markdown("<div style='margin-bottom: 50px;'></div>", unsafe_allow_html=True)