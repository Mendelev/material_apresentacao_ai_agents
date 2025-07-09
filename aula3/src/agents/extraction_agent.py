# src/agents/extraction_agent.py
import json
import re
import warnings
# LLM IMPORTS
from langchain_openai import OpenAI as OpenAIStandard
from langchain_openai import AzureOpenAI            # Para OpenAI legada (gpt-3.5-turbo-instruct)
from langchain_google_genai import ChatGoogleGenerativeAI # Para Gemini
# from langchain_openai import ChatOpenAI # Se fosse usar modelos de chat da OpenAI como gpt-3.5-turbo

import logging
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import openai # Para tipos de exceção específicos da OpenAI
import requests # Para tipos de exceção específicos de requests
# Google API Core Exceptions (para Gemini)
try:
    from google.api_core import exceptions as google_api_exceptions
except ImportError:
    google_api_exceptions = None # Permite rodar sem google-generativeai instalado se não for usado

import config

warnings.filterwarnings("ignore")
logger = logging.getLogger(__name__)

# Define quais exceções devem acionar uma retentativa
RETRYABLE_EXCEPTIONS_OPENAI = (
    openai.RateLimitError,
    openai.APITimeoutError,
    openai.APIConnectionError,
    openai.InternalServerError, # Inclui 500, 502, 503, 504
)

RETRYABLE_EXCEPTIONS_GOOGLE = ()
if google_api_exceptions:
    RETRYABLE_EXCEPTIONS_GOOGLE = (
        google_api_exceptions.ResourceExhausted, # Equivalente a RateLimitError
        google_api_exceptions.DeadlineExceeded,  # Equivalente a APITimeoutError
        google_api_exceptions.ServiceUnavailable,# Equivalente a InternalServerError (503)
        google_api_exceptions.InternalServerError, # Erro interno do Google
    )

RETRYABLE_EXCEPTIONS_REQUESTS = (
    requests.exceptions.Timeout,
    requests.exceptions.ConnectionError,
)

# Combina todas as exceções retryable
ALL_RETRYABLE_EXCEPTIONS = RETRYABLE_EXCEPTIONS_OPENAI + \
                           RETRYABLE_EXCEPTIONS_GOOGLE + \
                           RETRYABLE_EXCEPTIONS_REQUESTS

class ExtractionAgent:
    """
    Agente responsável por extrair informações estruturadas de texto bruto
    usando um Large Language Model (LLM), com suporte a contexto.
    """
    def __init__(self, prompt_file="prompts/extraction_prompt.txt"):
        """Inicializa o agente de extração."""
        self._setup_llm()
        self._load_prompt_template(prompt_file)
        self.extraction_chain = LLMChain(llm=self.llm, prompt=self.extraction_prompt)
        logger.info(f"Agente de Extração inicializado com provedor: {config.LLM_PROVIDER}.")

    def _setup_llm(self):
        """Configura a instância do LLM com base nas configurações."""
        provider = config.LLM_PROVIDER

        if provider == "openai": # Mantenha se quiser usar a API OpenAI padrão como fallback
            if not config.OPENAI_API_KEY:
                raise ValueError("OPENAI_API_KEY não configurada para o provedor OpenAI padrão")
            # Esta parte seria para OpenAI padrão, não Azure.
            # Se você só vai usar Azure, pode remover ou adaptar este bloco.
            # Para usar gpt-3.5-turbo-instruct com OpenAI padrão (não Azure):
            self.llm = OpenAIStandard(
                temperature=config.LLM_TEMPERATURE,
                openai_api_key=config.OPENAI_API_KEY,
                model_name=config.OPENAI_MODEL_NAME, # gpt-3.5-turbo-instruct
                max_tokens=config.LLM_MAX_TOKENS
            )
            logger.info(f"LLM configurado: OpenAI Padrão ({config.OPENAI_MODEL_NAME})")


        elif provider == "azure_openai": # NOVO ou ajustado provedor
            logger.info("--- DEBUG: Entrou no bloco azure_openai ---")
            if not all([config.AZURE_OPENAI_API_KEY, config.AZURE_OPENAI_ENDPOINT,
                        config.AZURE_OPENAI_DEPLOYMENT_NAME, config.AZURE_OPENAI_API_VERSION,  config.AZURE_MODEL_NAME]):
                raise ValueError("Configurações do Azure OpenAI (API_KEY, ENDPOINT, DEPLOYMENT_NAME, API_VERSION) incompletas.")
            logger.info(f"--- DEBUG: Configurações Azure: Endpoint='{config.AZURE_OPENAI_ENDPOINT}', Deployment='{config.AZURE_OPENAI_DEPLOYMENT_NAME}', ModelName='{config.AZURE_MODEL_NAME}', APIVersion='{config.AZURE_OPENAI_API_VERSION}' ---")

            self.llm = AzureOpenAI ( # Usando a classe AzureOpenAI da Langchain
                azure_endpoint=config.AZURE_OPENAI_ENDPOINT,
                api_key=config.AZURE_OPENAI_API_KEY, # Langchain espera api_key aqui, não credential
                azure_deployment=config.AZURE_OPENAI_DEPLOYMENT_NAME, # Nome da sua implantação do gpt-3.5-turbo-instruct
                api_version=config.AZURE_OPENAI_API_VERSION,
                max_tokens=config.LLM_MAX_TOKENS,
                temperature=config.LLM_TEMPERATURE,
                model_name=config.AZURE_MODEL_NAME, # Opcional se deployment_name for específico.
                                                            # Se o deployment puder servir múltiplos modelos, pode ser necessário.
                                                            # Para gpt-3.5-turbo-instruct, deployment_name é suficiente.
            )
            logger.info(f"--- DEBUG TIPO LLM: Tipo de self.llm instanciado para Azure: {type(self.llm)} ---")
            logger.info(f"LLM configurado: Azure OpenAI (Completion Model - Deployment: {config.AZURE_OPENAI_DEPLOYMENT_NAME}, Model: {config.AZURE_MODEL_NAME}, Endpoint: {config.AZURE_OPENAI_ENDPOINT})")

        elif provider == "google_genai":
            if not config.GOOGLE_API_KEY:
                raise ValueError("GOOGLE_API_KEY não configurada no .env ou config.py para o provedor Google GenAI")
            if not ChatGoogleGenerativeAI:
                raise ImportError("Pacote langchain-google-genai não instalado. Execute: pip install langchain-google-genai")
            
            self.llm = ChatGoogleGenerativeAI(
                model=config.GEMINI_MODEL_NAME,
                google_api_key=config.GOOGLE_API_KEY,
                temperature=config.LLM_TEMPERATURE,
                max_output_tokens=config.LLM_MAX_TOKENS, # Para Gemini
            )
            logger.info(f"LLM configurado: Google Gemini ({config.GEMINI_MODEL_NAME})")
        else:
            raise ValueError(f"Provedor LLM '{provider}' não suportado. Verifique LLM_PROVIDER em config.py ou .env.")
        logger.info(f"--- DEBUG FIM _setup_llm: LLM configurado. Tipo final: {type(self.llm)} ---")

    def _load_prompt_template(self, prompt_file: str):
        """Carrega o template do prompt de um arquivo."""
        try:
            with open(prompt_file, 'r', encoding='utf-8') as f:
                prompt_template_string = f.read()
        except FileNotFoundError:
            logger.error(f"Arquivo de prompt não encontrado em: {prompt_file}")
            raise
        except Exception as e:
            logger.error(f"Erro ao ler o arquivo de prompt {prompt_file}: {e}")
            raise IOError(f"Erro ao ler o arquivo de prompt {prompt_file}: {e}")

        logger.info(f"--- DEBUG ANTES LLMChain: Tipo de self.llm: {type(self.llm)} ao carregar prompt ---")
        self.extraction_prompt = PromptTemplate(
            input_variables=["input_text", "context_instruction"],
            template=prompt_template_string
        )

    def _generate_context_instruction(self, context_fields: list[str] = None, custom_instruction: str = None) -> str:
        if custom_instruction:
            logger.debug(f"Instrução de contexto PERSONALIZADA usada:\n---\n{custom_instruction.strip()}\n---")
            return custom_instruction + "\n"
        elif context_fields:
            fields_str = ", ".join(context_fields)
            instruction = (
                f"ATENÇÃO: O usuário está respondendo a uma pergunta específica sobre os seguintes campos: {fields_str}. "
                "Concentre-se em extrair APENAS as informações para estes campos a partir do 'Input do Usuário' abaixo. "
                "Para TODOS os outros campos não mencionados explicitamente no Input do Usuário ATUAL, retorne o valor `null`."
                "Sua única tarefa é analisar o 'Input do Usuário' abaixo e extrair informações **APENAS** para estes campos (`{fields_str}`)."
                "Ignore completamente qualquer outra informação no input que não seja diretamente relevante para estes campos específicos."
                "Para TODOS os outros campos do schema JSON completo que NÃO foram solicitados agora (`{fields_str}`), retorne obrigatoriamente o valor `null`."
                "Se, mesmo focando nos campos solicitados (`{fields_str}`), a informação para um deles não estiver presente no 'Input do Usuário' atual, retorne `null` para esse campo também."
                "\n"
            )
            logger.debug(f"Instrução de contexto (campos) gerada: {instruction.strip()}")
            return instruction
        else:
            logger.debug("Nenhuma instrução de contexto necessária (input geral).")
            return ""


    def _clean_and_load_json(self, llm_response_text: str) -> dict | None:
        if llm_response_text and llm_response_text.strip().startswith("<!DOCTYPE html"):
            logger.error("Resposta do LLM parece ser uma página de erro HTML.")
            logger.debug(f"Resposta HTML completa:\n{llm_response_text[:500]}...")
            return None

        match = re.search(r'\{.*\}', llm_response_text, re.DOTALL)
        if not match:
            logger.error("JSON não encontrado na resposta do LLM.")
            logger.debug(f"Resposta completa sem JSON:\n{llm_response_text}")
            return None

        json_str = match.group(0)
        cleaned_json_str = json_str.replace('\t', '    ') # Substitui tab por 4 espaços

        if cleaned_json_str != json_str:
            logger.debug(f"String JSON foi limpa. Original (início): '{json_str[:200]}...', Limpa (início): '{cleaned_json_str[:200]}...'")

        try:
            # Tenta decodificar a string JSON limpa
            return json.loads(cleaned_json_str)
        except json.JSONDecodeError as json_e:
            # Se a decodificação falhar, registra a string que foi usada para a tentativa (a limpa, se diferente)
            log_json_str_on_error = cleaned_json_str
            logger.error(f"Falha ao decodificar JSON encontrado na resposta: {json_e}. String JSON (usada para parse): '{log_json_str_on_error}'")
            # Se a limpeza alterou a string, também registra a original para comparação no debug
            if cleaned_json_str != json_str:
                logger.debug(f"String JSON ORIGINAL (antes da limpeza): '{json_str}'")
            return None

    def _post_process_extracted_data(self, data: dict) -> dict:
        if 'CNPJ/CPF' in data and data['CNPJ/CPF']:
            data['CNPJ/CPF'] = re.sub(r'[./-]', '', str(data['CNPJ/CPF']))
        for field in ["Preço Frete", "Valor"]:
            if field in data and data[field] is not None:
                value_str = str(data[field])
                logger.debug(f"Pós-processando campo '{field}'. Valor original: '{value_str}'")
                cleaned_str = re.sub(r'[R$\s]', '', value_str).strip()
                logger.debug(f"Após remoção R$/espaços: '{cleaned_str}'")
                if not cleaned_str:
                    logger.warning(f"Campo '{field}' ficou vazio após limpeza inicial de '{value_str}'. Definindo como None.")
                    data[field] = None
                    continue
                normalized_float_str = None
                try:
                    num_dots = cleaned_str.count('.')
                    num_commas = cleaned_str.count(',')
                    if num_commas == 1 and num_dots >= 0:
                        last_dot_pos = cleaned_str.rfind('.')
                        comma_pos = cleaned_str.rfind(',')
                        if comma_pos > last_dot_pos:
                            normalized_float_str = cleaned_str.replace('.', '').replace(',', '.')
                            logger.debug(f"Detectado formato BR provável. Normalizado para: '{normalized_float_str}'")
                        else:
                            normalized_float_str = cleaned_str.replace(',', '')
                            logger.debug(f"Detectado formato US/Intl provável. Normalizado para: '{normalized_float_str}'")
                    elif num_dots >= 1 and num_commas == 0:
                        logger.debug(f"Campo '{field}': {cleaned_str} tem {num_dots} ponto(s) e 0 vírgulas.")
                        parts = cleaned_str.split('.')
                        if num_dots == 1 and len(parts[-1]) == 3 and parts[0] != "":
                            normalized_float_str = "".join(parts)
                            logger.debug(f"Detectado formato tipo 'X.XXX'. Normalizado para: '{normalized_float_str}'")
                        elif len(parts[-1]) < 3:
                            normalized_float_str = "".join(parts[:-1]) + "." + parts[-1]
                            logger.debug(f"Detectado formato com último ponto decimal. Normalizado para: '{normalized_float_str}'")
                        else:
                            normalized_float_str = "".join(parts)
                            logger.debug(f"Detectado formato com múltiplos pontos. Normalizado para: '{normalized_float_str}'")
                    elif num_dots == 0 and num_commas == 0:
                         normalized_float_str = cleaned_str
                         logger.debug(f"Detectado formato inteiro. Normalizado para: '{normalized_float_str}'")
                    else:
                         logger.warning(f"Formato numérico ambíguo para '{cleaned_str}'. Fallback.")
                         normalized_float_str = cleaned_str.replace('.', '').replace(',', '.')
                    if not normalized_float_str:
                         logger.warning(f"Campo '{field}' ficou vazio após normalização de '{cleaned_str}'. Definindo como None.")
                         data[field] = None
                         continue
                    data[field] = float(normalized_float_str)
                    logger.debug(f"Campo '{field}' convertido para float: {data[field]}")
                except (ValueError, TypeError) as e:
                    log_norm_str = normalized_float_str if normalized_float_str is not None else '<Falha na Normalização>'
                    logger.warning(f"Não foi possível converter '{field}' (Original: '{value_str}' -> Limpo: '{cleaned_str}' -> Norm: '{log_norm_str}') para float. Erro: {e}. Definindo como None.")
                    data[field] = None
                except Exception as ex:
                    log_norm_str = normalized_float_str if normalized_float_str is not None else '<Falha na Normalização>'
                    logger.error(f"Erro inesperado ao processar campo numérico '{field}' (Original: '{value_str}' -> Norm: '{log_norm_str}'): {ex}", exc_info=True)
                    data[field] = None
        return data

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(ALL_RETRYABLE_EXCEPTIONS), # Usar a lista combinada
        reraise=True
    )
    def _invoke_llm_chain_with_retry(self, input_dict: dict) -> dict | str:
        """Invoca a cadeia LLM com lógica de retentativa."""
        logger.debug(f"Invocando LLM chain com provedor: {config.LLM_PROVIDER}...")
        response = self.extraction_chain.invoke(input_dict)
        logger.debug(f"LLM chain invocado com sucesso. Tipo da Resposta: {type(response)}")
        return response

    def extract(self, text: str, context_fields: list[str] = None, custom_instruction: str = None) -> dict | None:
        logger.debug(f"Texto para extração (repr): {repr(text)}")
        context_instruction = self._generate_context_instruction(context_fields, custom_instruction)
        input_payload = {
            'input_text': text,
            'context_instruction': context_instruction
        }

        try:
            response = self._invoke_llm_chain_with_retry(input_payload)
            
            if isinstance(response, dict):
                result_text = response.get('text', '')
                if not result_text and response:
                    for val in response.values():
                        if isinstance(val, str):
                            result_text = val
                            break
            elif isinstance(response, str):
                result_text = response
            else:
                logger.error(f"Resposta inesperada do LLM chain: {type(response)}. Conteúdo: {response}")
                result_text = ""

            logger.debug(f"Resposta BRUTA do LLM (após extração da chain): ---{result_text}---")

            extracted_data = self._clean_and_load_json(result_text)
            if not extracted_data:
                logger.error("Falha ao extrair JSON da resposta do LLM mesmo após retentativas.")
                return None

            if isinstance(extracted_data, dict):
                llm_valor = extracted_data.get("Valor")
                llm_preco = extracted_data.get("Preço") # 'Preço' como chave extraída pelo LLM
            
                if llm_valor is None and llm_preco is not None:
                    logger.info(f"Heurística: 'Valor' é None e 'Preço' ('{llm_preco}') existe. Copiando 'Preço' para 'Valor'.")
                    extracted_data["Valor"] = llm_preco

            processed_data = self._post_process_extracted_data(extracted_data)
            logger.debug(f"Dados extraídos (JSON válido e processado):\n{json.dumps(processed_data, indent=2, ensure_ascii=False)}")
            return processed_data

        except ALL_RETRYABLE_EXCEPTIONS as e:
             logger.error(f"Erro persistente ao chamar LLM após retentativas: {e}", exc_info=True)
             return None
        except Exception as e:
            logger.error(f"Erro inesperado na extração: {e}", exc_info=True)
            return None