# src/whatsapp_integration.py

import os
import json
import time
import logging
from flask import Flask, request, jsonify
import requests
from dotenv import load_dotenv
import io # Necessário para BytesIO

# Importações da arquitetura
from agents.extraction_agent import ExtractionAgent
from agents.mapping_agent import MappingAgent
from agents.orchestration_agent import OrchestrationAgent
from utils.formatting import format_final_summary_text
from utils.transcription import AudioTranscriber # Importa o transcritor
from topdesk_client import TopDeskClient
import config

# Configuração do Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()
app = Flask(__name__)

# --- Constantes e Configurações ---
WEBHOOK_VERIFY_TOKEN = os.getenv("WEBHOOK_VERIFY_TOKEN")
GRAPH_API_TOKEN = os.getenv("GRAPH_API_TOKEN")
PORT = int(os.getenv("WEBHOOK_PORT", 8000))
FACEBOOK_GRAPH_URL = f"https://graph.facebook.com/v18.0" # Ajuste a versão se necessário
SESSION_DIR = "whatsapp_sessions"
SESSION_TTL = 3600 * 4 # 4 horas

# Cria diretório de sessão se não existir
os.makedirs(SESSION_DIR, exist_ok=True)

# --- Inicialização dos Componentes Stateless ---
STATELESS_AGENTS_READY = False
audio_transcriber = None # Inicializa como None

try:
    extraction_agent = ExtractionAgent()
    mapping_agent = MappingAgent(artifacts_dir=config.ARTIFACTS_DIR)
    topdesk_client = TopDeskClient()

    if config.OPENAI_API_KEY: # Verifica se a chave existe para o transcritor
        audio_transcriber = AudioTranscriber()
        logger.info("AudioTranscriber inicializado.")
    else:
        logger.warning("OPENAI_API_KEY não configurada. Transcrição de áudio via WhatsApp estará desabilitada.")

    STATELESS_AGENTS_READY = mapping_agent.data_loaded_successfully # Checa se mapeamento carregou
    if not STATELESS_AGENTS_READY:
         logger.warning("Agente de Mapeamento não carregou dados CSV. Funcionalidade limitada.")
    elif audio_transcriber is None and config.OPENAI_API_KEY: # Se a key existe mas o transcritor falhou
        logger.error("AudioTranscriber falhou na inicialização mesmo com API Key. Verifique logs anteriores.")
        # Não impede STATELESS_AGENTS_READY, mas a transcrição não funcionará.
    elif audio_transcriber:
        logger.info("Todos os agentes principais (incluindo AudioTranscriber, se configurado) estão prontos.")


except (ValueError, FileNotFoundError, IOError) as e:
    logger.critical(f"Falha crítica ao inicializar agentes stateless ou cliente TopDesk: {e}", exc_info=True)
    STATELESS_AGENTS_READY = False
except Exception as e: # Pega outras exceções inesperadas na inicialização
    logger.critical(f"Erro inesperado durante inicialização: {e}", exc_info=True)
    STATELESS_AGENTS_READY = False


# --- Funções Auxiliares de Persistência de Sessão ---
# (Nenhuma mudança aqui, _get_session_path, save_state, load_state, clear_state permanecem iguais)
def _get_session_path(wa_id: str) -> str:
    """Retorna o caminho completo para o arquivo de sessão do usuário."""
    # Sanitiza wa_id para evitar problemas com caracteres especiais no nome do arquivo, se necessário
    safe_wa_id = "".join(c for c in wa_id if c.isalnum() or c in ('-', '_')).rstrip()
    return os.path.join(SESSION_DIR, f"session_{safe_wa_id}.json")

def save_state(wa_id: str, state_dict: dict):
    """Salva o dicionário de estado do usuário em um arquivo JSON."""
    filepath = _get_session_path(wa_id)
    try:
        state_dict['_timestamp'] = time.time() # Adiciona timestamp para TTL
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(state_dict, f, ensure_ascii=False, indent=4)
        logger.debug(f"Estado salvo para {wa_id} em {filepath}")
    except IOError as e:
        logger.error(f"Erro de IO ao salvar estado para {wa_id}: {e}", exc_info=True)
    except TypeError as e:
        logger.error(f"Erro de tipo ao serializar estado para {wa_id}: {e}. Estado: {state_dict}", exc_info=True)

def load_state(wa_id: str) -> dict | None:
    """Carrega o dicionário de estado do arquivo JSON, verificando TTL."""
    filepath = _get_session_path(wa_id)
    if not os.path.exists(filepath):
        return None
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            state_dict = json.load(f)

        timestamp = state_dict.get('_timestamp', 0)
        if time.time() - timestamp > SESSION_TTL:
            logger.info(f"Estado para {wa_id} expirou (TTL {SESSION_TTL}s). Removendo.")
            clear_state(wa_id)
            return None

        # Remove o timestamp antes de retornar para o orchestrator
        state_dict.pop('_timestamp', None)
        logger.debug(f"Estado carregado para {wa_id} de {filepath}")
        return state_dict
    except (IOError, json.JSONDecodeError) as e:
        logger.error(f"Erro ao carregar/decodificar estado para {wa_id} de {filepath}: {e}. Removendo arquivo corrompido.", exc_info=True)
        clear_state(wa_id)
        return None

def clear_state(wa_id: str):
    """Remove o arquivo de estado do usuário."""
    filepath = _get_session_path(wa_id)
    if os.path.exists(filepath):
        try:
            os.remove(filepath)
            logger.info(f"Estado removido para {wa_id} ({filepath}).")
        except OSError as e:
            logger.error(f"Erro ao remover arquivo de estado {filepath}: {e}", exc_info=True)


# --- Função Auxiliar de Envio WhatsApp ---
# (Nenhuma mudança aqui, send_whatsapp_message permanece igual)
def send_whatsapp_message(phone_number_id, to_wa_id, message_body=None, interactive_payload=None, context_message_id=None):
    """Envia uma mensagem (texto ou interativa) via WhatsApp Graph API."""
    if not GRAPH_API_TOKEN:
        logger.error("GRAPH_API_TOKEN não configurado.")
        return False
    if not phone_number_id:
        logger.error("phone_number_id não fornecido para envio.")
        return False

    url = f"{FACEBOOK_GRAPH_URL}/{phone_number_id}/messages"
    headers = {"Authorization": f"Bearer {GRAPH_API_TOKEN}", "Content-Type": "application/json"}
    json_data = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": to_wa_id,
    }
    # Adiciona contexto (para responder a uma mensagem específica) se fornecido
    if context_message_id:
        json_data["context"] = {"message_id": context_message_id}

    # Define o tipo e o conteúdo da mensagem
    if interactive_payload:
        json_data["type"] = "interactive"
        json_data["interactive"] = interactive_payload
    elif message_body:
        json_data["type"] = "text"
        json_data["text"] = {"body": message_body}
    else:
        logger.warning(f"Tentativa de enviar mensagem para {to_wa_id} sem corpo ou payload interativo.")
        return False

    # Envia a requisição
    try:
        response = requests.post(url, headers=headers, json=json_data, timeout=15) # Adicionado timeout
        response.raise_for_status()
        logger.info(f"Mensagem enviada para {to_wa_id}. Status: {response.status_code}.")
        logger.debug(f"Detalhe envio Wpp: URL={url} Payload={json.dumps(json_data)} Resp={response.text}")
        return True
    except requests.exceptions.Timeout:
         logger.error(f"Timeout ao enviar mensagem para {to_wa_id}", exc_info=True)
         return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao enviar mensagem para {to_wa_id}: {e}", exc_info=True)
        if e.response is not None:
             logger.error(f"Resposta do erro: {e.response.status_code} - {e.response.text}")
        return False

# --- Nova Função: Download de Mídia do WhatsApp ---
def _download_whatsapp_media(media_id: str) -> bytes | None:
    """Baixa um arquivo de mídia do WhatsApp usando seu ID."""
    if not GRAPH_API_TOKEN:
        logger.error("GRAPH_API_TOKEN não configurado para download de mídia.")
        return None

    # 1. Obter a URL da mídia
    media_info_url = f"{FACEBOOK_GRAPH_URL}/{media_id}"
    headers = {"Authorization": f"Bearer {GRAPH_API_TOKEN}"}
    media_url = None
    mime_type = None

    try:
        response_info = requests.get(media_info_url, headers=headers, timeout=10)
        response_info.raise_for_status()
        media_data = response_info.json()
        media_url = media_data.get("url")
        mime_type = media_data.get("mime_type")
        logger.debug(f"Informações da mídia {media_id}: {media_data}")
        if not media_url:
            logger.error(f"Não foi possível obter a URL de download para media_id {media_id}. Resposta: {media_data}")
            return None
    except requests.exceptions.Timeout:
        logger.error(f"Timeout ao obter URL da mídia {media_id}.", exc_info=True)
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao obter URL da mídia {media_id}: {e}", exc_info=True)
        if e.response is not None:
            logger.error(f"Resposta do erro (media URL): {e.response.status_code} - {e.response.text}")
        return None

    # 2. Baixar o arquivo de mídia
    try:
        # O token também é necessário para acessar a URL da mídia
        response_download = requests.get(media_url, headers=headers, timeout=20) # Timeout maior para download
        response_download.raise_for_status()
        logger.info(f"Mídia {media_id} (tipo: {mime_type}) baixada com sucesso ({len(response_download.content)} bytes).")
        return response_download.content
    except requests.exceptions.Timeout:
        logger.error(f"Timeout ao baixar mídia de {media_url}.", exc_info=True)
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao baixar mídia de {media_url}: {e}", exc_info=True)
        if e.response is not None:
            logger.error(f"Resposta do erro (download): {e.response.status_code} - {e.response.text}")
        return None

# --- Funções de Lógica de Negócio ---
# (get_orchestrator_instance, _create_topdesk_ticket_and_reply, _handle_orchestrator_response permanecem iguais)
def get_orchestrator_instance(wa_id: str) -> OrchestrationAgent:
    """Cria uma instância do OrchestrationAgent e carrega seu estado."""
    if not STATELESS_AGENTS_READY:
        raise RuntimeError("Agentes stateless não estão prontos ou falharam na inicialização.")

    orchestrator = OrchestrationAgent(extraction_agent, mapping_agent)
    persisted_state = load_state(wa_id)
    if persisted_state:
        orchestrator.load_state(persisted_state)
        logger.info(f"Estado anterior restaurado para Orchestrator de {wa_id}.")
    else:
        logger.info(f"Nenhum estado anterior encontrado para {wa_id}. Usando estado padrão.")
    return orchestrator

def _create_topdesk_ticket_and_reply(payload: dict, wa_id: str, business_phone_number_id: str) -> bool:
    """Tenta criar o ticket no TopDesk e envia a resposta ao usuário."""
    success = False
    reply_body = ""
    try:
        cadencia_fmt = payload.get("Cadencia_Formatada", "") # Cadência já formatada pelo orchestrator
        formatted_string = format_final_summary_text(payload, cadencia_fmt)
        logger.debug(f"String formatada para TopDesk (ID: {wa_id}):\n{formatted_string}")

        ticket_id = topdesk_client.create_incident(formatted_string)
        if ticket_id:
            reply_body = f"Chamado criado com sucesso! Número: {ticket_id}"
            logger.info(f"Ticket {ticket_id} criado para {wa_id}.")
            success = True
        else:
            reply_body = "Ocorreu um erro ao criar o chamado no TopDesk. A equipe responsável foi notificada."
            logger.error(f"Falha ao criar ticket no TopDesk para {wa_id} (API retornou None/False).")
            # Aqui poderia ter uma notificação interna para a equipe de suporte sobre a falha
            success = False # Indica que o ticket não foi criado
    except Exception as e:
        logger.error(f"Exceção ao formatar ou chamar TopDesk para {wa_id}: {e}", exc_info=True)
        reply_body = "Ocorreu um erro interno ao processar a criação do chamado. Tente novamente mais tarde."
        success = False

    # Envia resposta final ao usuário
    send_whatsapp_message(phone_number_id=business_phone_number_id, to_wa_id=wa_id, message_body=reply_body)
    return success

def _handle_orchestrator_response(response_dict: dict, wa_id: str, message_id: str, business_phone_number_id: str):
    """Processa a resposta do orchestrator e envia a mensagem apropriada ao usuário."""
    status = response_dict.get("status")
    reply_body = response_dict.get("message", "Ocorreu um erro inesperado.")
    payload = response_dict.get("payload") # Payload para criação do ticket

    logger.info(f"Resposta do Orquestrador para {wa_id} - Status: {status}")

    if status == "needs_confirmation":
        # Prepara botões interativos
        interactive = {
            "type": "button",
            "body": {"text": reply_body}, # Mensagem vem do orchestrator com instruções
            "action": {
                "buttons": [
                    {"type": "reply", "reply": {"id": "confirm_yes", "title": "Sim, Confirmar"}},
                    {"type": "reply", "reply": {"id": "confirm_edit", "title": "Corrigir/Alterar"}},
                    {"type": "reply", "reply": {"id": "confirm_full_cancel", "title": "Cancelar e Novo"}}
                    # Poderia ter botão "Cancelar" explícito:
                    # {"type": "reply", "reply": {"id": "confirm_cancel", "title": "Cancelar Pedido"}}
                ]
            }
        }
        send_whatsapp_message(
            phone_number_id=business_phone_number_id, to_wa_id=wa_id,
            interactive_payload=interactive
        )
        # Estado é salvo após a chamada a esta função

    elif status == "needs_input":
        send_whatsapp_message(
            phone_number_id=business_phone_number_id, to_wa_id=wa_id,
            message_body=reply_body, context_message_id=message_id
        )
        # Estado é salvo após a chamada a esta função

    elif status == "confirmed_for_creation":
        logger.info(f"Orquestrador confirmou dados para {wa_id}. Tentando criar ticket.")
        if payload:
            ticket_created = _create_topdesk_ticket_and_reply(payload, wa_id, business_phone_number_id)
            # O estado já foi limpo pelo orchestrator ao retornar este status
            # e será salvo como vazio (ou não salvo se clear_state for chamado explicitamente)
            if ticket_created:
                 logger.info(f"Fluxo concluído com sucesso (ticket criado) para {wa_id}.")
                 clear_state(wa_id)
                 # clear_state(wa_id) # Garante limpeza se o reset do orchestrator falhar
            else:
                 logger.warning(f"Fluxo concluído com falha na criação do ticket para {wa_id}.")
                 clear_state(wa_id)
                 # clear_state(wa_id) # Limpa estado mesmo se falhou em criar ticket
        else:
            logger.error(f"Status 'confirmed_for_creation' para {wa_id} sem payload!")
            send_whatsapp_message(phone_number_id=business_phone_number_id, to_wa_id=wa_id, message_body="Erro interno: dados finais não encontrados.")
            clear_state(wa_id) # Limpa estado em caso de erro interno grave

    elif status == "completed": # Sucesso sem criação de ticket (ex: só consulta) - não usado atualmente
        send_whatsapp_message(phone_number_id=business_phone_number_id, to_wa_id=wa_id, message_body=reply_body)
        clear_state(wa_id) # Limpa estado ao completar

    elif status == "aborted":
        send_whatsapp_message(phone_number_id=business_phone_number_id, to_wa_id=wa_id, message_body=reply_body)
        clear_state(wa_id) # Estado já limpo pelo orchestrator

    elif status == "error":
        logger.error(f"Orquestrador retornou erro para {wa_id}: {reply_body}")
        send_whatsapp_message(phone_number_id=business_phone_number_id, to_wa_id=wa_id, message_body=reply_body)
        clear_state(wa_id) # Limpa estado em caso de erro

    else:
        logger.warning(f"Status desconhecido '{status}' do Orquestrador para {wa_id}.")
        send_whatsapp_message(phone_number_id=business_phone_number_id, to_wa_id=wa_id, message_body="Desculpe, algo inesperado aconteceu.")
        clear_state(wa_id)

# _process_text_message permanece igual
def _process_text_message(user_text: str, wa_id: str, message_id: str, business_phone_number_id: str):
    """Processa uma mensagem de texto recebida."""
    if not user_text:
        logger.info(f"Mensagem de texto vazia de {wa_id}. Ignorando.")
        return

    logger.info(f"Processando texto de {wa_id}: '{user_text}'")
    try:
        orchestrator = get_orchestrator_instance(wa_id)
        metadata = {'vendedor_id': wa_id}
        response_dict = orchestrator.process_user_input(user_text, metadata=metadata)

        # Envia a resposta ao usuário com base no status
        # _handle_orchestrator_response AGORA lida com clear_state
        _handle_orchestrator_response(response_dict, wa_id, message_id, business_phone_number_id)

        # Salva o estado ATUALIZADO do orchestrator, A MENOS QUE tenha sido um fluxo terminal
        # que já limpou o estado (confirmed_for_creation, aborted, completed, error).
        # O estado do orchestrator na memória já foi resetado por ele mesmo nesses casos.
        # Se clear_state foi chamado, o arquivo não existe mais.
        # Se o arquivo ainda existe (ex: needs_input, needs_confirmation), salvamos.
        status = response_dict.get("status")
        if status not in ["confirmed_for_creation", "completed", "aborted", "error"]:
            # Se o orchestrator resetou seu estado interno (ex: em _format_confirmed_for_creation ANTES de _create_topdesk_ticket_and_reply)
            # E nós NÃO chamamos clear_state(), então save_state() vai salvar um estado limpo.
            # Se NÓS chamamos clear_state(), então o arquivo já foi removido, e save_state() irá recriá-lo como limpo.
            # Isso é seguro. O importante é que após um fluxo terminal, o estado salvo seja o inicial.
            save_state(wa_id, orchestrator.get_state_dict())
        else:
            # Se o clear_state foi chamado em _handle_orchestrator_response, o arquivo já foi removido.
            # Se não foi (ex: falha na criação do ticket e você decidiu não limpar),
            # o estado interno do orchestrator já foi resetado, então save_state
            # (se fosse chamado aqui) salvaria um estado limpo.
            # Como queremos garantir que o arquivo é removido nesses casos,
            # o clear_state() dentro do _handle_orchestrator_response é a melhor abordagem.
            logger.debug(f"Estado para {wa_id} não será salvo explicitamente aqui pois status é terminal ({status}), clear_state já foi chamado se necessário.")


    except RuntimeError as e: # Erro ao carregar/instanciar agentes
        logger.error(f"Erro de Runtime ao processar texto para {wa_id}: {e}", exc_info=True)
        send_whatsapp_message(
             phone_number_id=business_phone_number_id, to_wa_id=wa_id,
             message_body="Desculpe, o serviço está temporariamente indisponível devido a um erro interno.",
             context_message_id=message_id)
        clear_state(wa_id) # Limpa estado em caso de falha grave
    except Exception as e:
        logger.error(f"Erro inesperado ao processar texto '{user_text}' de {wa_id}: {e}", exc_info=True)
        send_whatsapp_message(
             phone_number_id=business_phone_number_id, to_wa_id=wa_id,
             message_body="Desculpe, ocorreu um erro interno inesperado. Tente novamente mais tarde.",
             context_message_id=message_id)
        clear_state(wa_id) # Limpa estado

# _handle_interactive_message permanece igual
def _handle_interactive_message(message_data: dict, wa_id: str, message_id: str, business_phone_number_id: str):
    """Processa uma resposta de botão interativo."""
    interactive_data = message_data.get("interactive", {})
    button_reply = interactive_data.get("button_reply")
    if not button_reply:
        logger.warning(f"Mensagem interativa de {wa_id} sem button_reply.")
        return

    button_id = button_reply.get("id")
    button_title = button_reply.get("title")
    logger.info(f"Botão '{button_title}' (ID: {button_id}) pressionado por {wa_id}.")

    # Mapeia IDs de botão para texto que o orchestrator entende
    text_equivalent = None
    if button_id == "confirm_yes":
        # Processa confirmação como antes
        _process_text_message("Sim", wa_id, message_id, business_phone_number_id)

    elif button_id == "confirm_edit":
        # === NOVO TRATAMENTO PARA O BOTÃO CORRIGIR ===
        logger.info(f"Botão 'Corrigir' (confirm_edit) detectado para {wa_id}. Solicitando input de correção.")
        orchestrator_state = load_state(wa_id) # Carrega o estado atual

        if orchestrator_state and orchestrator_state.get("pending_confirmation"):
            # Modifica o estado para aguardar a correção
            orchestrator_state["pending_confirmation"] = False # Sai do loop de confirmação
            orchestrator_state["last_question_context"] = "awaiting_user_correction_text" # Novo contexto
            orchestrator_state["last_asked_fields"] = None # Não está pedindo campos específicos agora
            # Mantém orchestrator_state["pending_confirmation_payload"] intacto!

            save_state(wa_id, orchestrator_state) # Salva o estado modificado

            # Envia a instrução para o usuário digitar a correção
            reply_body = "Ok, por favor, digite APENAS a informação que deseja corrigir (ex: 'Cidade é Cuiabá', 'Preço Frete 500')."
            send_whatsapp_message(
                phone_number_id=business_phone_number_id, to_wa_id=wa_id,
                message_body=reply_body, context_message_id=message_id # Responde à msg do botão
            )
        else:
            logger.warning(f"Botão 'confirm_edit' recebido para {wa_id}, mas estado não era 'pending_confirmation' ou falhou ao carregar.")
            # Fallback: Informar erro ao usuário
            send_whatsapp_message(
                phone_number_id=business_phone_number_id, to_wa_id=wa_id,
                message_body="Houve um problema ao tentar iniciar a correção. Por favor, tente confirmar ou cancelar novamente.",
                context_message_id=message_id
            )
            # Considerar limpar o estado aqui se a situação for irrecuperável
            # clear_state(wa_id)

    elif button_id == "confirm_cancel": # Se você adicionar um botão "Cancelar"
         _process_text_message("Não", wa_id, message_id, business_phone_number_id)

    elif button_id == "confirm_full_cancel": # Novo tratamento
        logger.info(f"Botão 'Cancelar Pedido' (ID: {button_id}) pressionado por {wa_id}. Resetando estado.")
        clear_state(wa_id) # Limpa o estado da sessão do usuário
        reply_body = "Seu pedido foi cancelado. Para iniciar um novo pedido, por favor, envie os detalhes."
        send_whatsapp_message(
            phone_number_id=business_phone_number_id, to_wa_id=wa_id,
            message_body=reply_body, context_message_id=message_id # Responde à mensagem do botão
        )

    else:
        logger.warning(f"ID de botão não reconhecido '{button_id}' de {wa_id}.")
        # Enviar mensagem de erro ao usuário
        send_whatsapp_message(
            phone_number_id=business_phone_number_id, to_wa_id=wa_id,
            message_body="Desculpe, não reconheci essa opção.",
            context_message_id=message_id
        )

# --- Nova Função: Processar Mensagens de Áudio ---
def _process_audio_message(message_data: dict, wa_id: str, message_id: str, business_phone_number_id: str):
    """Processa uma mensagem de áudio recebida."""
    if not audio_transcriber:
        logger.warning(f"Mensagem de áudio recebida de {wa_id}, mas o AudioTranscriber não está disponível.")
        send_whatsapp_message(business_phone_number_id, wa_id,
                              "Desculpe, o processamento de áudio está temporariamente indisponível.", message_id)
        return

    audio_object = message_data.get("audio")
    if not audio_object:
        logger.warning(f"Mensagem tipo áudio de {wa_id} sem objeto 'audio'.")
        return

    media_id = audio_object.get("id")
    if not media_id:
        logger.warning(f"Mensagem de áudio de {wa_id} sem 'id' de mídia.")
        return

    logger.info(f"Processando áudio de {wa_id} (Media ID: {media_id}). Baixando...")
    audio_bytes = _download_whatsapp_media(media_id)

    if not audio_bytes:
        logger.error(f"Falha ao baixar áudio {media_id} de {wa_id}.")
        send_whatsapp_message(business_phone_number_id, wa_id,
                              "Não consegui baixar seu áudio. Por favor, tente enviar novamente.", message_id)
        return

    # O WhatsApp geralmente envia áudio em formato ogg com codec opus.
    # O Whisper lida bem com ogg, então podemos usar um nome de arquivo genérico.
    filename_for_transcription = f"whatsapp_audio_{media_id}.ogg"
    logger.info(f"Áudio baixado. Transcrevendo com {filename_for_transcription}...")

    # Envia uma mensagem de "processando" para o usuário (opcional, mas bom para UX)
    send_whatsapp_message(business_phone_number_id, wa_id,
                          "Recebi seu áudio, estou processando...", message_id)

    transcribed_text = audio_transcriber.transcribe_audio(audio_bytes, filename=filename_for_transcription)

    if transcribed_text is None or not transcribed_text.strip(): # Verifica se a transcrição não foi vazia
        logger.warning(f"Transcrição falhou ou resultou em texto vazio para áudio {media_id} de {wa_id}.")
        send_whatsapp_message(business_phone_number_id, wa_id,
                              "Não consegui entender o áudio. Poderia tentar novamente ou digitar os detalhes?", message_id)
        return

    logger.info(f"Áudio transcrito para {wa_id}: '{transcribed_text}'")
    # Agora, processa o texto transcrito como se fosse uma mensagem de texto normal
    _process_text_message(transcribed_text, wa_id, message_id, business_phone_number_id)


# --- Rotas do Webhook ---
@app.route("/webhook", methods=["POST"])
def webhook_post():
    """Recebe notificações de mensagens do WhatsApp."""
    if not STATELESS_AGENTS_READY:
         logger.critical("Webhook recebido, mas agentes não estão prontos. Retornando erro 503.")
         # Retorna 503 para indicar que o serviço está indisponível temporariamente
         return jsonify({"status": "error", "message": "Service temporarily unavailable"}), 503

    data = request.json
    logger.info("Webhook POST recebido.")
    logger.debug("Webhook payload: %s", json.dumps(data))

    # Validação básica do payload
    if not data or "entry" not in data or not data["entry"]:
        logger.warning("Payload do webhook vazio ou mal formatado.")
        return jsonify({"status": "error", "message": "Invalid payload"}), 400 # Bad request

    try:
        entry = data["entry"][0]
        changes = entry.get("changes", [{}])[0]
        value = changes.get("value", {})
        messages = value.get("messages")
        metadata_wpp = value.get("metadata", {}) # Renomeado para evitar conflito
        business_phone_number_id = metadata_wpp.get("phone_number_id")

        if not messages:
            # Pode ser um status de mensagem, etc. Ignorar por enquanto.
            logger.info("Notificação sem 'messages' recebida (ex: status de entrega). Ignorando.")
            return jsonify({"status": "ok", "message": "Notification received, no action needed"}), 200

        message = messages[0]
        wa_id = message.get("from")
        message_id = message.get("id")
        message_type = message.get("type")

        if not wa_id or not message_id or not business_phone_number_id:
             logger.warning(f"Mensagem recebida com dados essenciais faltando: wa_id={wa_id}, msg_id={message_id}, phone_id={business_phone_number_id}")
             return jsonify({"status": "error", "message": "Missing essential message data"}), 400

        # Processa com base no tipo de mensagem
        if message_type == "text":
            user_text = message.get("text", {}).get("body")
            _process_text_message(user_text, wa_id, message_id, business_phone_number_id)
        elif message_type == "interactive":
            _handle_interactive_message(message, wa_id, message_id, business_phone_number_id)
        elif message_type == "audio": # <<< NOVO HANDLER
            _process_audio_message(message, wa_id, message_id, business_phone_number_id)
        elif message_type == "image":
            caption = message.get("image", {}).get("caption")
            if caption:
                 logger.info(f"Imagem com legenda recebida de {wa_id}. Processando legenda.")
                 _process_text_message(caption, wa_id, message_id, business_phone_number_id)
            else:
                 logger.info(f"Imagem sem legenda recebida de {wa_id}. Ignorando.")
                 # Opcional: Enviar mensagem avisando que precisa de legenda
                 send_whatsapp_message(business_phone_number_id, wa_id, "Recebi sua imagem, mas preciso que você envie os detalhes do pedido como texto na legenda, por favor.", context_message_id=message_id)
        else:
            logger.info(f"Tipo de mensagem não tratado '{message_type}' recebido de {wa_id}. Ignorando.")
            # Opcional: Enviar mensagem avisando que só aceita texto/imagem com legenda
            send_whatsapp_message(business_phone_number_id, wa_id, "Desculpe, só consigo processar mensagens de texto, áudio ou imagens com os detalhes na legenda.", context_message_id=message_id)

        return jsonify({"status": "ok", "message": "Webhook processed"}), 200

    except IndexError:
         logger.warning("Estrutura inesperada no payload do webhook (sem 'entry' ou 'changes').")
         return jsonify({"status": "error", "message": "Invalid payload structure"}), 400
    except Exception as e:
        logger.error(f"Erro geral não capturado no processamento do webhook: {e}", exc_info=True)
        # Retorna 200 para evitar reenvios do WhatsApp em caso de erro interno não recuperável
        return jsonify({"status": "ok", "message": "Internal server error processing webhook"}), 200

# webhook_get permanece igual
@app.route("/webhook", methods=["GET"])
def webhook_get():
    """Verifica o token do webhook (necessário para configuração inicial)."""
    mode = request.args.get("hub.mode")
    token = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")

    if mode == "subscribe" and token == WEBHOOK_VERIFY_TOKEN:
        logger.info("Webhook verificado com sucesso!")
        return challenge, 200
    else:
        logger.warning(f"Falha na verificação do webhook. Modo: {mode}, Token recebido: {token}")
        return "Forbidden", 403

# --- Inicialização do App ---
if __name__ == "__main__":
    if not WEBHOOK_VERIFY_TOKEN or not GRAPH_API_TOKEN:
        logger.warning("!!! Variáveis WEBHOOK_VERIFY_TOKEN ou GRAPH_API_TOKEN não definidas no .env !!!")
        # Não sair, mas alertar

    if not STATELESS_AGENTS_READY:
         logger.critical("!!! Agentes stateless não inicializados corretamente. O chatbot pode não funcionar. Verifique logs anteriores. !!!")
         # Não sair, mas alertar criticamente
    elif not config.OPENAI_API_KEY: # Adiciona um alerta específico se a chave para transcrição não estiver lá
        logger.warning("!!! OPENAI_API_KEY não configurada. A funcionalidade de transcrição de áudio via WhatsApp estará desabilitada. !!!")


    logger.info(f"Iniciando servidor Flask na porta {PORT}...")
    # Use Gunicorn ou outro servidor WSGI em produção em vez de app.run(debug=True)
    # Ex: gunicorn --bind 0.0.0.0:8000 whatsapp_integration:app
    # Para desenvolvimento local:
    app.run(host="0.0.0.0", port=PORT, debug=False) # debug=False é mais seguro