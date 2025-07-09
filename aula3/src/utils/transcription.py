# src/utils/transcription.py
import io
import logging
import openai
import config # Para buscar a API Key da OpenAI
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)

# Define exceções re-tentáveis específicas para a API de transcrição da OpenAI
# (Podem ser as mesmas ou um subconjunto das usadas para LLM)
RETRYABLE_EXCEPTIONS_OPENAI_STT = (
    openai.RateLimitError,
    openai.APITimeoutError,
    openai.APIConnectionError,
    openai.InternalServerError,
)

class AudioTranscriber:
    """
    Responsável por transcrever áudio usando um serviço externo (ex: OpenAI Whisper).
    """
    def __init__(self):
        """Inicializa o cliente de transcrição."""
        if not config.OPENAI_API_KEY:
            raise ValueError("OPENAI_API_KEY não configurada no .env ou config.py, necessária para AudioTranscriber.")
        
        # Inicializa o cliente OpenAI (v1.0.0+)
        # A chave é lida automaticamente das variáveis de ambiente ou passada explicitamente
        try:
            self.client = openai.OpenAI(api_key=config.OPENAI_API_KEY)
            logger.info("AudioTranscriber inicializado com OpenAI Whisper API.")
        except Exception as e:
            logger.error(f"Falha ao inicializar cliente OpenAI para transcrição: {e}", exc_info=True)
            raise ValueError(f"Falha ao inicializar cliente OpenAI para transcrição: {e}")

    @retry(
        stop=stop_after_attempt(3), # Número de tentativas
        wait=wait_exponential(multiplier=1, min=2, max=6), # Tempo de espera exponencial
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS_OPENAI_STT), # Condições para retentar
        reraise=True # Re-levanta a exceção original se todas as tentativas falharem
    )
    def _transcribe_with_retry(self, audio_file_tuple: tuple) -> str:
        """Método interno que chama a API Whisper com lógica de retentativa."""
        try:
            # Chama a API de transcrição usando o cliente instanciado
            # O endpoint 'transcriptions' substitui o antigo 'Transcription'
            transcript_response = self.client.audio.transcriptions.create(
                model="whisper-1", # Modelo padrão e geralmente o melhor custo-benefício
                file=audio_file_tuple, # Passa a tupla (filename, file_object)
                #language="pt" # Opcional: Especificar o idioma pode melhorar a precisão
            )
            # O objeto de resposta agora tem um atributo 'text'
            return transcript_response.text
        except Exception as e:
            logger.error(f"Erro durante chamada à API Whisper: {e}", exc_info=True)
            raise # Re-levanta para acionar retentativa ou falhar

    def transcribe_audio(self, audio_bytes: bytes, filename: str = "audio_input.wav") -> str | None:
        """
        Transcreve os bytes de áudio fornecidos usando a API Whisper.

        Args:
            audio_bytes: Os bytes brutos do arquivo de áudio.
            filename: Um nome de arquivo (com extensão) para ajudar a API a identificar o formato.

        Returns:
            O texto transcrito em caso de sucesso, ou None em caso de erro.
        """
        if not audio_bytes:
            logger.warning("Tentativa de transcrever áudio com bytes vazios.")
            return None

        try:
            # Cria um objeto de arquivo em memória a partir dos bytes
            audio_file = io.BytesIO(audio_bytes)
            # A API espera um objeto tipo arquivo, e dar um nome (com extensão) ajuda.
            # Enviamos como uma tupla: (nome_arquivo, objeto_arquivo)
            audio_file_tuple = (filename, audio_file)

            logger.info(f"Enviando áudio ({len(audio_bytes)} bytes, nome: {filename}) para transcrição Whisper...")
            
            # Chama o método interno com retentativas
            transcribed_text = self._transcribe_with_retry(audio_file_tuple)

            logger.info("Transcrição de áudio bem-sucedida.")
            logger.debug(f"Texto transcrito: '{transcribed_text}'")
            return transcribed_text

        except RETRYABLE_EXCEPTIONS_OPENAI_STT as e:
            logger.error(f"Falha persistente na API Whisper após retentativas: {e}", exc_info=True)
            return None
        except openai.AuthenticationError as e:
             logger.error(f"Erro de autenticação com a API OpenAI (Whisper). Verifique sua API Key: {e}", exc_info=True)
             return None
        except Exception as e:
            # Captura outros erros inesperados (ex: formato de áudio inválido pela API)
            logger.error(f"Erro inesperado durante a transcrição de áudio: {e}", exc_info=True)
            return None