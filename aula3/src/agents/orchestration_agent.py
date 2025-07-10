# src/agents/orchestration_agent.py
import json
import logging
import re
from agents.extraction_agent import ExtractionAgent
from agents.mapping_agent import MappingAgent
from utils.formatting import format_cadencia, format_final_summary_text
from utils.normalization import normalize_string
from datetime import datetime
from langchain.memory import ConversationBufferMemory
from memory.memory_manager import MemoryManager
from agents.knowledge_agent import KnowledgeAgent

logger = logging.getLogger(__name__)

class OrchestrationAgent:
    """
    Orquestra o fluxo de extração, mapeamento, validação e interação
    com o usuário para processar um pedido.
    """
    def __init__(self, extraction_agent: ExtractionAgent, mapping_agent: MappingAgent,memory_manager: MemoryManager | None):
        self.extraction_agent = extraction_agent
        self.mapping_agent = mapping_agent
        self.memory_manager = memory_manager 
        self._reset_state_data() # Inicializa o estado

        if self.extraction_agent.llm and self.memory_manager:
            self.knowledge_agent = KnowledgeAgent(self.extraction_agent.llm, self.memory_manager)
        else:
            self.knowledge_agent = None

        # Campos obrigatórios após o mapeamento inicial
        self.mandatory_fields_post_mapping = [
             "CNPJ/CPF",
             "Planta", "Condição de Pagamento", "Forma de Pagamento",
             "Código do Material", "Cadência", "Vendedor", "Cidade",
             "Data de Negociação", "Incoterms", "Valor",
        ]
        # Campos que, se presentes, DEVEM ter sido mapeados com sucesso
        self.fields_requiring_valid_mapping = [
            "Condição de Pagamento", "Forma de Pagamento", "Código do Material"
        ]
        logger.info("Agente Orquestrador inicializado (pronto para carregar estado).")

    def _archive_successful_ticket(self, user_id: str):
        if not self.memory_manager or not user_id:
            return
        
        final_payload = self.state.get("pending_confirmation_payload")
        if not final_payload:
            return
            
        logger.info(f"Arquivando ticket confirmado para o usuário {user_id} no histórico.")
        self.memory_manager.save_ticket_to_history(user_id, final_payload)
    
    def _classify_intent(self, user_text: str) -> str:
        if self.state.get("last_question_context") or self.state.get("pending_confirmation") or self.state.get("pending_ambiguity"):
            return 'CRIAR_TICKET'

        prompt = f"""
        Classifique a intenção do usuário com base na mensagem abaixo. As opções são:
        - 'CRIAR_TICKET': O usuário está tentando iniciar, continuar ou fornecer dados para um novo pedido.
        - 'PERGUNTA_SOBRE_A_CONVERSA': O usuário está fazendo uma pergunta sobre o diálogo atual.
        - 'PERGUNTA_SOBRE_O_HISTORICO': O usuário está fazendo uma pergunta sobre pedidos ou informações passadas.
        - 'CONVERSA_GERAL': O usuário está fazendo uma pergunta geral ou conversando sobre algo não relacionado a pedidos.

        Exemplos:
        - Mensagem: "Quero registrar 50 toneladas de FS Ouro" -> Intenção: CRIAR_TICKET
        - Mensagem: "qual foi a primeira coisa que eu te disse?" -> Intenção: PERGUNTA_SOBRE_A_CONVERSA
        - Mensagem: "qual o preço do último pedido que fiz para o cliente Acme?" -> Intenção: PERGUNTA_SOBRE_O_HISTORICO
        - Mensagem: "Me ensine a fazer bolo de chocolate" -> Intenção: CONVERSA_GERAL
        - Mensagem: "PDL" -> Intenção: CRIAR_TICKET (é uma resposta a uma pergunta)

        ---
        Mensagem do Usuário: "{user_text}"
        ---

        Intenção:
        """
        try:
            response = self.extraction_agent.llm.invoke(prompt)
            intent = (response.content if hasattr(response, 'content') else str(response)).strip().replace("'", "")
            logger.info(f"Intenção detectada: '{intent}'")
            # Adiciona uma verificação para garantir que a resposta é uma das opções válidas
            valid_intents = ['CRIAR_TICKET', 'PERGUNTA_SOBRE_A_CONVERSA', 'PERGUNTA_SOBRE_O_HISTORICO', 'CONVERSA_GERAL']
            if intent in valid_intents:
                return intent
            return 'CRIAR_TICKET' # Fallback se o LLM responder algo inesperado
        except Exception as e:
            logger.error(f"Erro ao classificar intenção: {e}")
            return 'CRIAR_TICKET'

    def _pre_fill_from_profile(self, user_id: str):
        """Pré-preenche dados do perfil do usuário."""
        if not self.memory_manager or not user_id:
            return

        profile = self.memory_manager.get_profile(user_id)
        if not profile:
            return

        logger.info(f"Perfil encontrado para '{user_id}'. Pré-preenchendo dados.")
        # Exemplo: pré-preencher a planta se não houver uma no pedido atual
        if "Planta" not in self.state["request_data"] or not self.state["request_data"]["Planta"]:
            if profile.get("common_plant"):
                self.state["request_data"]["Planta"] = profile["common_plant"]
                logger.info(f"Planta pré-preenchida com '{profile['common_plant']}' do perfil.")
        
        # Exemplo: pré-preencher o vendedor com o nome do perfil
        if "Vendedor" not in self.state["request_data"] or not self.state["request_data"]["Vendedor"]:
            if profile.get("full_name"):
                 self.state["request_data"]["Vendedor"] = profile["full_name"]

    # --- Métodos de Gerenciamento de Estado ---
    def _reset_state_data(self):
        """Reseta o estado interno para um novo pedido."""
        self.state = {
            "request_data": {},
            "mapping_issues": {"avisos": [], "erros": [], "ambiguidades": []},
            "last_question_context": None,
            "last_asked_fields": None,
            "pending_confirmation": False,
            "pending_ambiguity": None, # Guarda dados da ambiguidade atual
            "pending_confirmation_payload": None, # Guarda dados do resumo final
            "current_original_input_text": None
        }
        logger.debug("Estrutura de estado resetada para o padrão.")

    def _update_profile_from_ticket(self, user_id: str):
        """Atualiza o perfil do usuário com dados do ticket concluído."""
        if not self.memory_manager or not user_id:
            return
        
        final_data = self.state.get("pending_confirmation_payload", {})
        if not final_data:
            return

        profile_update = {}
        if final_data.get("Planta"):
            profile_update["common_plant"] = final_data["Planta"]
        if final_data.get("Nome do cliente"):
            profile_update["last_client_name"] = final_data["Nome do cliente"]
        
        if profile_update:
            logger.info(f"Atualizando perfil de '{user_id}' com dados do ticket: {profile_update}")
            self.memory_manager.update_profile(user_id, profile_update)

    def load_state(self, state_data: dict):
        """Carrega um estado previamente salvo."""
        # Garante que todas as chaves padrão existem ao carregar
        default_state = {
            "request_data": {}, "mapping_issues": {"avisos": [], "erros": [], "ambiguidades": []},
            "last_question_context": None, "last_asked_fields": None,
            "pending_confirmation": False, "pending_ambiguity": None,
            "pending_confirmation_payload": None
        }
        default_state.update(state_data)
        self.state = default_state
        logger.debug(f"Estado carregado: {json.dumps(self.state, indent=2, ensure_ascii=False)}")

    def get_state_dict(self) -> dict:
        """Retorna uma cópia do estado atual."""
        return self.state.copy()

    def _update_request_data(self, new_data: dict | None):
        """Atualiza o dicionário request_data no estado de forma segura."""
        if not new_data: return
        logger.debug(f"Atualizando request_data com: {json.dumps(new_data, indent=2, ensure_ascii=False)}")
        current_request_data = self.state.get("request_data", {})
        # Regra: Atualiza se o novo valor não for None,
        # ou se a chave não existe no estado atual,
        # ou se o valor atual é None,
        # ou se o novo valor é uma string vazia (permitindo limpar campos)
        for key, value in new_data.items():
             if value is not None or key not in current_request_data or current_request_data.get(key) is None:
                 current_request_data[key] = value
             elif isinstance(value, str) and value == "" and current_request_data.get(key) is not None:
                 current_request_data[key] = value # Permite limpar com string vazia
        self.state["request_data"] = current_request_data
        logger.debug(f"request_data após atualização: {json.dumps(self.state['request_data'], indent=2, ensure_ascii=False)}")

    def _check_missing_fields(self, field_list: list[str]) -> list[str]:
        """Verifica quais campos da lista estão faltando (None ou vazio) em request_data."""
        missing = []
        request_data = self.state.get("request_data", {})
        for field in field_list:
            value = request_data.get(field)
            if value is None or (isinstance(value, str) and not value.strip()):
                 missing.append(field)
        return missing

    # --- Métodos de Formatação de Resposta ---
    # (Estes métodos são relativamente simples e focados, não precisam de grande refatoração interna)
    def _format_user_question(self, message: str, context: str = None, missing_fields: list = None) -> dict:
        """Formata uma resposta que requer input do usuário."""
        self.state["last_question_context"] = context
        self.state["last_asked_fields"] = missing_fields
        self.state["pending_ambiguity"] = None # Limpa ambiguidade ao fazer nova pergunta
        self.state["pending_confirmation"] = False # Limpa confirmação
        logger.debug(f"Formatando pergunta: Contexto='{context}', Campos Pedidos={missing_fields}")
        return {"status": "needs_input", "message": message}

    def _format_final_summary(self, final_data: dict) -> dict:
        """Formata o resumo final para confirmação do usuário."""
        self.state["pending_confirmation"] = True
        self.state["last_question_context"] = "confirmation_response"
        self.state["last_asked_fields"] = None
        self.state["pending_ambiguity"] = None
        self.state["pending_confirmation_payload"] = final_data.copy() # Armazena payload exato
        logger.debug("Formatando resumo para confirmação final e armazenando payload.")

        cadencia_fmt = final_data.get("Cadencia_Formatada", None) # Usa cadência já formatada
        formatted_summary = format_final_summary_text(final_data, cadencia_fmt)
        confirmation_message = (
            f"Por favor, revise os dados abaixo antes de prosseguir:\n\n"
            f"{formatted_summary}\n\n"
            f"Os dados estão corretos? Responda 'Sim' para confirmar, 'Não' para cancelar, "
            f"ou digite a informação que deseja corrigir (ex: 'Preço Frete é 500', 'Cidade é Cuiabá')."
        )
        return { "status": "needs_confirmation", "message": confirmation_message }

    def _format_success(self, ticket_id="N/A") -> dict:
        """Formata uma resposta de sucesso (usado internamente pelo orchestrator ou pela integração)."""
        logger.info(f"Processo concluído com sucesso (Ticket: {ticket_id}).")
        # Não reseta o estado aqui, quem chama é responsável por isso
        return {"status": "completed", "message": f"Solicitação processada com sucesso! (Ticket: {ticket_id})"}

    def _format_confirmed_for_creation(self) -> dict:
        """Formata a resposta indicando que os dados foram confirmados e estão prontos para criação do ticket."""
        logger.info("Usuário confirmou. Retornando payload para criação do ticket.")
        final_payload = self.state.get("pending_confirmation_payload")
        # Importante: Resetar o estado ANTES de retornar, para evitar reprocessamento
        self._reset_state_data()
        return {
            "status": "confirmed_for_creation",
            "message": "Confirmação recebida. Preparando para criar o chamado...", # Mensagem temporária
            "payload": final_payload # Envia o payload final
        }

    def _format_error(self, error_message: str) -> dict:
        """Formata uma resposta de erro."""
        logger.error(f"Erro no orquestrador: {error_message}")
        # Considerar resetar o estado em caso de erro? Depende da estratégia.
        # self._reset_state_data() # Descomentar se quiser resetar em erro
        return {"status": "error", "message": f"Ocorreu um erro: {error_message}"}

    def _format_abort(self) -> dict:
        """Formata uma resposta de cancelamento pelo usuário."""
        logger.info("Solicitação abortada pelo usuário.")
        original_payload = self.state.get("pending_confirmation_payload") # Pega antes de resetar, se houver
        self._reset_state_data() # Limpa o estado ao abortar
        return {"status": "aborted", "message": "Solicitação cancelada."}

    # --- Métodos de Lógica Principal ---

    def _handle_confirmation_response(self, user_text: str, user_id: str, short_term_memory: ConversationBufferMemory) -> dict | None:
        if not (self.state.get("pending_confirmation") and self.state.get("last_question_context") == "confirmation_response"):
            return None

        logger.info("Processando resposta no estado 'pending_confirmation'.")
        response_norm = normalize_string(user_text)
        confirmation_keywords = ["sim", "s", "yes", "y", "ok", "correto", "confirmar", "confirmo"]
        cancel_keywords = ["nao", "n", "no", "incorreto", "cancelar"]

        if response_norm in confirmation_keywords:
            self._update_profile_from_ticket(user_id)
            self._archive_successful_ticket(user_id) # NOVO: Arquiva o ticket
            return self._format_confirmed_for_creation()

        elif response_norm in cancel_keywords:
            return self._format_abort()

        else:
            return self._handle_user_edit(user_text, short_term_memory=short_term_memory)

    def _handle_user_edit(self, user_text: str, short_term_memory: ConversationBufferMemory) -> dict:
        """Processa a entrada do usuário como uma tentativa de edição dos dados do resumo."""
        logger.info(f"Resposta '{user_text}' não é confirmação/cancelamento. Tratando como EDIÇÃO.")
        original_payload = self.state.get("pending_confirmation_payload")
        if not original_payload:
            logger.error("Tentativa de edição, mas pending_confirmation_payload está vazio!")
            self._reset_state_data()
            return self._format_error("Ocorreu um problema ao tentar editar. Por favor, comece novamente.")

        # Prepara instrução de edição para o LLM
        original_data_summary_str = json.dumps(original_payload, ensure_ascii=False, indent=None) # Sem indentação para prompt
        edit_instruction = (
            "ATENÇÃO: A tarefa é extrair uma correção do usuário para um campo de formulário e retornar um JSON. "
            "Analise a frase do usuário e retorne um objeto JSON contendo APENAS o campo corrigido e seu novo valor. "
            "Seja literal e não adicione campos que não foram mencionados na correção."
            "\n\n### EXEMPLOS ###"
            "\n- Input do Usuário: 'O valor na verdade é 2300'\n- JSON de Saída: {\"Valor\": 2300}"
            "\n- Input do Usuário: 'arruma a cidade pra Cuiabá'\n- JSON de Saída: {\"Cidade\": \"Cuiabá\"}"
            "\n- Input do Usuário: 'não, o cnpj é 123456'\n- JSON de Saída: {\"CNPJ/CPF\": \"123456\"}"
            "\n\nNÃO inclua nenhuma explicação, comentário ou texto adicional. Retorne APENAS o JSON."
        )

        logger.debug(f"Chamando ExtractionAgent para EDIÇÃO. Texto: '{user_text}'")
        extracted_edit_data = self.extraction_agent.extract(
            user_text,
            memory=short_term_memory, # << Adicionado
            custom_instruction=edit_instruction
        )

        if extracted_edit_data:
            logger.info(f"Dados extraídos da edição: {json.dumps(extracted_edit_data, indent=2, ensure_ascii=False)}")

            # Cria cópia dos dados originais e aplica edições
            updated_data = original_payload.copy()
            # Simula a atualização para usar a lógica de merge segura do _update_request_data
            temp_state_for_update = {"request_data": updated_data}
            self._apply_updates_to_dict(temp_state_for_update["request_data"], extracted_edit_data)
            updated_data = temp_state_for_update["request_data"]

            logger.debug(f"Dados após aplicar edição: {json.dumps(updated_data, indent=2, ensure_ascii=False)}")

            # Limpa estado de confirmação ANTES de revalidar
            self.state["pending_confirmation"] = False
            self.state["last_question_context"] = None
            self.state["pending_confirmation_payload"] = None
            self.state["last_asked_fields"] = None

            # Atualiza o estado principal e revalida
            self.state["request_data"] = updated_data # Atualiza estado real
            return self._run_mapping_and_validation() # Re-executa mapeamento e validação

        else:
            logger.warning("Não foi possível extrair nenhuma edição da resposta do usuário.")
            # Pede para tentar de novo, mantendo o estado de confirmação
            return self._format_user_question(
                "Desculpe, não consegui entender a correção. Poderia tentar digitar novamente a informação que deseja alterar? Ou responda 'Sim'/'Não' para os dados apresentados.",
                context="confirmation_response" # Mantém o contexto
            )

    def _apply_updates_to_dict(self, target_dict: dict, updates: dict):
         """Função auxiliar para aplicar atualizações (similar a _update_request_data mas em dict genérico)."""
         for key, value in updates.items():
             if value is not None or key not in target_dict or target_dict.get(key) is None:
                 target_dict[key] = value
             elif isinstance(value, str) and value == "" and target_dict.get(key) is not None:
                 target_dict[key] = value

    def _resolve_pending_ambiguity(self, user_response: str) -> bool:
        """Tenta resolver uma ambiguidade pendente com base na resposta do usuário."""
        pending_ambiguity_data = self.state.get("pending_ambiguity")
        if not pending_ambiguity_data: return False # Nenhuma ambiguidade pendente

        options = pending_ambiguity_data.get("options", [])
        field_to_update = pending_ambiguity_data.get("field") # Ex: "Código do cliente"
        original_field_name = pending_ambiguity_data.get("original_field_name", field_to_update) # Ex: "Cliente"
        resolved_value = None
        update_dict = {}
        chosen_option = None # Adicionado para ter acesso fora do loop de texto

        logger.debug(f"Tentando resolver ambiguidade para '{original_field_name}' com resposta '{user_response}'. Opções: {options}")

        # Tentativa por índice numérico
        try:
            choice_index = int(user_response.strip()) - 1
            if 0 <= choice_index < len(options):
                chosen_option = options[choice_index] # <<< GUARDA A OPÇÃO ESCOLHIDA
                resolved_value = chosen_option.get("codigo") # O valor a ser usado é sempre 'codigo'
                match_type = "índice numérico"
        except ValueError:
             pass # Não é número, tenta por texto

        # Tentativa por texto (código ou descrição normalizados)
        if resolved_value is None:
            user_response_norm = normalize_string(user_response)
            if user_response_norm:
                for option_item in options: # Renomeado para evitar conflito com a variável 'options' mais externa
                    codigo_option = option_item.get("codigo")
                    desc_option = option_item.get("descricao") or option_item.get("produto") or option_item.get("nome")
                    match_found = False
                    if codigo_option and normalize_string(str(codigo_option)) == user_response_norm:
                        resolved_value = codigo_option
                        match_type = "match de código"
                        match_found = True
                    elif desc_option and normalize_string(desc_option) == user_response_norm:
                        resolved_value = codigo_option # Mesmo se match for na descrição, o valor é o código
                        match_type = "match de descrição"
                        match_found = True

                    if match_found:
                        chosen_option = option_item # <<< GUARDA A OPÇÃO ESCOLHIDA
                        break

        # Se encontrou um valor, atualiza o estado
        if resolved_value is not None and chosen_option is not None: # <<< VERIFICA chosen_option
            logger.debug(f"Ambiguidade resolvida por {match_type} -> Valor '{resolved_value}'")
            update_dict[field_to_update] = resolved_value
            # Se for ambiguidade de cliente, atualiza também nome e CNPJ/CPF
            if original_field_name == "Cliente":
                nome_da_planilha_escolhido = chosen_option.get("nome")
                update_dict["Nome do cliente"] = nome_da_planilha_escolhido
                update_dict["CNPJ/CPF"] = chosen_option.get("cnpj_cpf")
                update_dict["Cliente"] = nome_da_planilha_escolhido


            self._update_request_data(update_dict)
            # Limpa o estado de ambiguidade e contexto
            self.state["pending_ambiguity"] = None
            self.state["last_question_context"] = None
            self.state["last_asked_fields"] = None
            logger.debug("Ambiguidade resolvida. request_data atualizado.")
            return True
        else:
            logger.debug(f"Resposta '{user_response}' não resolveu a ambiguidade.")
            return False

    def _handle_ambiguity_response(self, user_text: str) -> dict | None:
         """Processa a resposta do usuário quando estava pendente de ambiguidade."""
         if not self.state.get("pending_ambiguity"):
             return None 

         # Guardar o campo da ambiguidade que está sendo resolvida
         resolved_ambiguity_field = self.state["pending_ambiguity"].get("original_field_name") or self.state["pending_ambiguity"].get("field")


         if self._resolve_pending_ambiguity(user_text): # Isso já limpa self.state["pending_ambiguity"]
             logger.debug(f"Ambiguidade para '{resolved_ambiguity_field}' resolvida. Limpando issue correspondente de mapping_issues.")
             
             # Limpar a issue de ambiguidade específica que foi resolvida
             if "mapping_issues" in self.state and "ambiguidades" in self.state["mapping_issues"]:
                 current_ambiguities = self.state["mapping_issues"]["ambiguidades"]
                 self.state["mapping_issues"]["ambiguidades"] = [
                     amb for amb in current_ambiguities 
                     if (amb.get("original_field_name") or amb.get("campo")) != resolved_ambiguity_field
                 ]
                 if not self.state["mapping_issues"]["ambiguidades"]: # Se a lista ficou vazia
                     del self.state["mapping_issues"]["ambiguidades"]


             logger.debug("Re-executando mapeamento e validação após resolução de ambiguidade.")
             return self._run_mapping_and_validation()
         else:
             # Falhou em resolver, pergunta novamente
             pending_ambiguity_data = self.state.get("pending_ambiguity", {})
             original_question = pending_ambiguity_data.get('original_question', "Por favor, escolha uma das opções.")
             # Reusa o contexto original da pergunta de ambiguidade
             return self._format_user_question(
                 f"Não consegui entender sua escolha. {original_question}",
                 context=pending_ambiguity_data.get('context')
                 # Não passa missing_fields aqui, pois estamos no fluxo de ambiguidade
             )

    def _filter_extraction_by_context(self, extracted_data: dict | None) -> dict:
        """Filtra os dados extraídos para manter apenas os campos solicitados no contexto."""
        last_context = self.state.get("last_question_context")
        last_asked = self.state.get("last_asked_fields")

        if not (last_context and last_asked and extracted_data):
            logger.debug("[Filtro Contexto] Nenhum contexto/campos pedidos ou extração vazia, pulando filtragem.")
            # Limpa as flags de contexto *mesmo se não filtrou*, pois a interação atual as consumiu
            self.state["last_question_context"] = None
            self.state["last_asked_fields"] = None
            return extracted_data or {} # Retorna dict vazio se extracted_data for None

        logger.debug(f"[Filtro Contexto] Iniciando. Contexto: '{last_context}', Campos Pedidos: {last_asked}")
        logger.debug(f"[Filtro Contexto] Dados extraídos (pré-filtro): {json.dumps(extracted_data, indent=2, ensure_ascii=False)}")

        # Mapeia nomes de campos pedidos para chaves de extração válidas
        valid_extraction_keys = set()
        for field_name in last_asked:
            # Simplifica o nome do campo removendo detalhes como (obrigatório...)
            base_field_name = re.sub(r'\s*\(.*\)\s*', '', field_name).strip()
            # Adiciona mapeamentos específicos (ex: pedir "Cliente" permite extrair "Cliente" ou "CNPJ/CPF")
            if base_field_name in ["Cliente", "Nome do cliente"]:
                valid_extraction_keys.update(["Cliente", "CNPJ/CPF", "Nome do cliente"])
            elif base_field_name == "CNPJ/CPF":
                valid_extraction_keys.add("CNPJ/CPF")
            elif base_field_name == "Código do cliente":
                valid_extraction_keys.add("Código do cliente")
            # Adicionar outros mapeamentos se necessário
            else:
                valid_extraction_keys.add(base_field_name) # Adiciona o nome base como chave válida

        logger.debug(f"[Filtro Contexto] Chaves de extração válidas para este contexto: {valid_extraction_keys}")

        filtered_data = {}
        for key, value in extracted_data.items():
            # Mantém se a chave for válida E o valor não for nulo (LLM pode retornar nulls para campos não pedidos)
            if key in valid_extraction_keys and value is not None:
                filtered_data[key] = value
                logger.debug(f"[Filtro Contexto] Mantendo campo '{key}' (valor: '{value}')")
            elif key in valid_extraction_keys and value is None:
                 logger.debug(f"[Filtro Contexto] Ignorando campo '{key}' pois valor extraído é nulo (esperado no modo contextual).")
            else:
                 logger.debug(f"[Filtro Contexto] Ignorando campo extraído '{key}' pois não está em {valid_extraction_keys}.")

        logger.debug(f"[Filtro Contexto] Dados extraídos APÓS filtragem: {json.dumps(filtered_data, indent=2, ensure_ascii=False)}")

        # Limpa o contexto DEPOIS de usá-lo para filtrar
        self.state["last_question_context"] = None
        self.state["last_asked_fields"] = None

        return filtered_data

    def _run_mapping_and_validation(self) -> dict:
         """Executa o mapeamento e a validação pós-mapeamento."""
         logger.info("Iniciando mapeamento.")
         current_input_text = self.state.get("current_original_input_text", "")
         if not current_input_text:
             logger.warning("_run_mapping_and_validation chamado sem current_original_input_text no estado. Mapeamento de planta por substring do texto original pode ser limitado.")
         mapped_data, mapping_issues_result = self.mapping_agent.map(
             self.state["request_data"].copy(),
             original_input_text=current_input_text # Passa o texto original
         )

         self._update_request_data(mapped_data)
         self.state["mapping_issues"] = mapping_issues_result

         return self._run_post_mapping_validation()

    # --- Métodos de Validação Pós-Mapeamento (Refatorados) ---

    def _validate_mapping_issues(self) -> dict | None:
        """Verifica e lida com erros e ambiguidades do mapeamento."""
        mapping_issues = self.state.get("mapping_issues", {})

        # 5a. Erros Críticos de Mapeamento
        if mapping_issues.get("erros"):
            return self._format_error(f"Erro durante o mapeamento: {'; '.join(mapping_issues['erros'])}")

        # 5b. Ambigüidades
        if mapping_issues.get("ambiguidades"):
            ambiguity = mapping_issues["ambiguidades"][0] # Trata uma por vez
            campo_issue = ambiguity.get("campo") # Ex: "Código do cliente"
            original_field_name_issue = ambiguity.get("original_field_name") # Ex: "Cliente"
            opcoes = ambiguity.get("opcoes", [])

            if not opcoes:
                 logger.error(f"Ambiguidade para '{campo_issue}' sem opções definidas.")
                 return self._format_error(f"Inconsistência interna ao tentar resolver ambiguidade para '{campo_issue}'.")

            question = ambiguity.get("mensagem", f"Encontramos múltiplos resultados para '{ambiguity.get('valor_original')}'. Por favor, escolha.")
            # Usar o original_field_name_issue se disponível para o contexto, senão o campo_issue
            context_field_name_for_log = original_field_name_issue if original_field_name_issue else campo_issue
            context = f"resolve_ambiguity_{context_field_name_for_log.lower().replace(' ', '_')}"

            # Prepara os dados para guardar no estado pending_ambiguity
            self.state["pending_ambiguity"] = {
                "field": campo_issue, # O campo a ser atualizado diretamente com o código da opção
                "original_field_name": original_field_name_issue, # O nome original do campo que gerou a ambiguidade (ex: "Cliente")
                "options": opcoes,
                "context": context,
                "original_question": question
            }
            logger.info(f"Ambiguidade detectada para '{original_field_name_issue if original_field_name_issue else campo_issue}'. Solicitando esclarecimento ao usuário.")
            self.state["last_question_context"] = context
            self.state["last_asked_fields"] = None
            self.state["pending_confirmation"] = False
            logger.debug(f"Formatando pergunta de ambiguidade diretamente. Contexto='{context}'")
            return {"status": "needs_input", "message": question}

        return None

    def _validate_cliente_info(self, request_data: dict, missing_keys: list, missing_msgs: list):
        """Valida a presença de informações do cliente.
        Se CNPJ/CPF estiver faltando, será pego pela validação global.
        Esta função garante que, se não tivermos Código do Cliente NEM CNPJ/CPF,
        o Nome do Cliente seja solicitado.
        """
        cliente_cod = request_data.get("Código do cliente")
        cliente_cnpj = request_data.get("CNPJ/CPF")
        # Considera tanto o nome mapeado (Nome do cliente) quanto o nome extraído (Cliente)
        cliente_nome_mapeado = request_data.get("Nome do cliente")
        cliente_nome_extraido = request_data.get("Cliente")
        nome_presente = bool(cliente_nome_mapeado or cliente_nome_extraido)

        # Se não temos um código de cliente E não temos um CNPJ/CPF,
        # então o nome do cliente é essencial para prosseguir (seja para novo cadastro ou tentativa de busca).
        if not cliente_cod and not cliente_cnpj:
            if not nome_presente:
                if "Cliente" not in missing_keys: # Usar "Cliente" como chave para pedir ao LLM
                    missing_keys.append("Cliente")
                    missing_msgs.append("Nome do cliente (Nome é obrigatório se cliente for novo e não tiver CPF/CNPJ)")
                logger.debug("Validação Cliente: Código e CNPJ/CPF ausentes. Nome do cliente é necessário.")
            else:
                logger.debug("Validação Cliente: Código e CNPJ/CPF ausentes, mas nome presente. OK por ora (CNPJ/CPF será pego pela validação global se faltar).")
        elif not cliente_cod and cliente_cnpj and not nome_presente:
            # Temos CNPJ, mas nem código nem nome. O nome ainda seria útil.
            if "Cliente" not in missing_keys:
                missing_keys.append("Cliente")
                missing_msgs.append("Nome do cliente (recomendado para confirmação/novo cadastro)")
            logger.debug("Validação Cliente: Código ausente, CNPJ presente, mas Nome ausente. Solicitando Nome.")
        else:
            logger.debug(f"Validação Cliente: Código='{cliente_cod}', CNPJ/CPF='{cliente_cnpj}', Nome Presente='{nome_presente}'. Verificações de cliente OK ou delegadas.")

    def _validate_frete_condicional(self, request_data: dict, missing_keys: list, missing_msgs: list):
        """Valida a obrigatoriedade do Preço Frete com base no Incoterms."""
        incoterms_value = request_data.get('Incoterms')
        incoterms = str(incoterms_value).upper() if incoterms_value else ''
        preco_frete = request_data.get('Preço Frete')

        # Define o que é considerado frete ausente (None, vazio, 'null', 0)
        frete_ausente = False
        if preco_frete is None:
            frete_ausente = True
        elif isinstance(preco_frete, str):
            valor_str_limpo = preco_frete.strip()
            if not valor_str_limpo or valor_str_limpo.lower() == 'null':
                frete_ausente = True
        elif isinstance(preco_frete, (int, float)) and preco_frete == 0:
            # Consideramos 0 como ausente para fins de obrigatoriedade,
            # a menos que o Incoterm seja FOB/TPD.
            # Se for CIF 0, ainda pode ser um valor explícito.
            # Se for FOB 0 ou TPD 0, é aceitável.
            if incoterms not in ['FOB', 'TPD']:
                frete_ausente = True
            # Se for FOB/TPD e o preço é 0, não consideramos ausente para a lógica de validação abaixo.
            # A formatação cuidará de exibir "R$ 0,00 (FOB)" ou "N/A (FOB)" se fosse None.


        logger.debug(f"Validação Frete: Incoterms='{incoterms}', PrecoFrete='{preco_frete}', Ausente={frete_ausente}")

        # MODIFICAÇÃO: Adiciona 'TPD' à lista de Incoterms que não exigem frete
        if incoterms and incoterms not in ['FOB', 'TPD'] and frete_ausente:
            if "Preço Frete" not in missing_keys:
                logger.debug(f"Adicionando Preço Frete (Obrigatório para Incoterms: {incoterms})")
                missing_keys.append("Preço Frete")
                missing_msgs.append(f"Preço Frete (obrigatório para Incoterms '{incoterms}')")

    def _validate_mapped_fields(self, request_data: dict, missing_keys: list, missing_msgs: list):
         """Verifica se campos que exigem mapeamento válido realmente foram mapeados."""
         logger.debug("Verificando avisos de mapeamento para campos mapeáveis obrigatórios.")
         avisos_mapeamento = self.state.get("mapping_issues", {}).get("avisos", [])
         campos_com_aviso_invalido = set()

         for aviso in avisos_mapeamento:
             campo_aviso = aviso.get("campo")
             # Verifica se o aviso é sobre um campo que precisa de mapeamento válido E se ele ainda está presente no request_data
             # (pode ter sido corrigido em interação anterior mas o aviso persistiu no estado)
             if campo_aviso in self.fields_requiring_valid_mapping and request_data.get(campo_aviso) == aviso.get("valor_original"):
                  campos_com_aviso_invalido.add(campo_aviso)
                  logger.warning(f"Campo '{campo_aviso}' preenchido com valor inválido '{aviso.get('valor_original')}' (conforme aviso de mapeamento).")

         for campo in campos_com_aviso_invalido:
              if campo not in missing_keys:
                   missing_keys.append(campo)
                   valor_original = request_data.get(campo, 'N/A')
                   missing_msgs.append(f"{campo} (valor '{valor_original}' inválido ou não encontrado)")
                   # Remove mensagens genéricas para este campo se houver
                   missing_msgs = [msg for msg in missing_msgs if msg != campo]


    def _validate_and_format_cadencia(self, request_data: dict, missing_keys: list, missing_msgs: list):
        """Formata a cadência e valida o resultado."""
        logger.info("Formatando e validando cadência.")
        cadencia_original = request_data.get("Cadência")
        qtd_total_str = request_data.get("Quantidade Total")
        data_negociacao = request_data.get("Data de Negociação")

        # Extrai parte numérica da quantidade total, se houver
        qtd_total_num_str = None
        if qtd_total_str:
            match = re.search(r'(\d[\d.,]*)', str(qtd_total_str))
            if match:
                qtd_total_num_str = re.sub(r'[.,](?=\d{3})', '', match.group(1)) # Remove milhar
                qtd_total_num_str = qtd_total_num_str.replace(',', '.') # Ajusta decimal

        # Tenta formatar
        cadencia_formatada = format_cadencia(cadencia_original, qtd_total=qtd_total_num_str, data_negociacao=data_negociacao)

        # Atualiza no request_data (mesmo se for None, para ficar consistente)
        request_data["Cadencia_Formatada"] = cadencia_formatada
        self.state["request_data"] = request_data # Atualiza estado

        # Valida se a formatação falhou para uma cadência original não vazia
        if cadencia_original and cadencia_formatada is None:
            if "Cadência" not in missing_keys:
                logger.warning(f"Formatação da Cadência falhou para o input: '{cadencia_original}'. Solicitando novamente.")
                missing_keys.append("Cadência")
                missing_msgs.append("Cadência (formato inválido ou não reconhecido)")
                # Remove mensagem genérica de "Cadência" se houver
                missing_msgs = [msg for msg in missing_msgs if msg != "Cadência"]

    def _build_missing_fields_question(self, missing_keys: list[str], missing_msgs: list[str]) -> dict:
        """Constrói a pergunta para o usuário sobre os campos faltantes/inválidos."""
        campos_unicos_msg = sorted(list(dict.fromkeys(missing_msgs))) # Remove duplicatas de mensagens
        fields_str = ', '.join(campos_unicos_msg)

        # Determina um contexto mais específico para a pergunta, se possível
        context = "missing_fields"
        first_msg = campos_unicos_msg[0] if campos_unicos_msg else ""
        if any("Cliente" in f or "CNPJ/CPF" in f for f in first_msg):
             context = "missing_client_info"
        elif first_msg.startswith("Preço Frete"):
             context = "missing_frete_for_incoterm"
        elif first_msg.startswith("Cadência"):
             context = "invalid_cadencia_format"
        elif "inválido" in first_msg:
             context = "invalid_mapped_field_value"

        # Monta a pergunta
        question = f"Quase lá! Ainda preciso destas informações: {fields_str}."
        # Personaliza a pergunta com base no contexto, se aplicável
        if context == "missing_frete_for_incoterm":
             incoterms = self.state["request_data"].get('Incoterms', 'N/A')
             question = f"Como o Incoterms é '{incoterms}', por favor, informe o Preço Frete."
        elif context == "invalid_cadencia_format":
             cadencia_original = self.state["request_data"].get('Cadência', '')
             question = f"Não consegui entender o formato da Cadência fornecida ('{cadencia_original}'). Poderia informar novamente? (Ex: 50 FEV, 100 MAR/25, etc.)"
        elif context == "invalid_mapped_field_value":
             invalid_msg = next((msg for msg in campos_unicos_msg if 'inválido' in msg), fields_str)
             question = f"Por favor, corrija a informação: {invalid_msg}"

        # Retorna a pergunta formatada, passando as CHAVES únicas faltantes para o contexto da próxima extração
        campos_faltantes_keys_unicos = sorted(list(dict.fromkeys(missing_keys)))
        return self._format_user_question(question, context=context, missing_fields=campos_faltantes_keys_unicos)

    def _run_post_mapping_validation(self) -> dict:
        """Executa todas as validações após o mapeamento e retorna a próxima ação."""
        logger.debug("Executando validação pós-mapeamento.")
        logger.debug(f"Estado atual para validação: {json.dumps(self.state, indent=2, ensure_ascii=False)}")

        request_data_ref = self.state.get("request_data", {})
        if request_data_ref.get("Data de Negociação") is None:
            current_date_str = datetime.now().strftime("%d/%m/%Y")
            request_data_ref["Data de Negociação"] = current_date_str
            # self.state["request_data"] já é uma referência para request_data_ref,
            # mas para clareza ou se a estrutura mudar, pode ser explícito:
            # self._update_request_data({"Data de Negociação": current_date_str})
            # No entanto, modificar request_data_ref diretamente é mais eficiente aqui.
            logger.info(f"Data de Negociação ausente. Padrão definido para data atual: {current_date_str}")

        # 1. Verifica Erros e Ambigüidades do Mapeamento
        issue_response = self._validate_mapping_issues()
        if issue_response:
            return issue_response # Retorna pergunta de ambiguidade ou erro

        # Se não há erros/ambiguidades críticas, procede com validações de dados
        request_data = self.state.get("request_data", {})
        missing_post_mapping_keys = []
        campos_faltantes_msg = []

        # 2. Validação do Cliente
        self._validate_cliente_info(request_data, missing_post_mapping_keys, campos_faltantes_msg)

        # 3. Validação dos campos obrigatórios genéricos
        missing_generic = self._check_missing_fields(self.mandatory_fields_post_mapping)
        missing_post_mapping_keys.extend(k for k in missing_generic if k not in missing_post_mapping_keys)
        campos_faltantes_msg.extend(k for k in missing_generic if k not in campos_faltantes_msg)

        # 4. Validação Condicional do Frete
        self._validate_frete_condicional(request_data, missing_post_mapping_keys, campos_faltantes_msg)

        # 5. Validação de Campos que Precisam de Mapeamento Válido
        self._validate_mapped_fields(request_data, missing_post_mapping_keys, campos_faltantes_msg)

        # 6. Formatação e Validação da Cadência (atualiza request_data internamente)
        self._validate_and_format_cadencia(request_data, missing_post_mapping_keys, campos_faltantes_msg)

        # 7. Montar e Retornar Pergunta se Algo Falta
        logger.debug(f"Validação Final: Missing Keys = {missing_post_mapping_keys}")
        logger.debug(f"Validação Final: Mensagens Faltantes = {campos_faltantes_msg}")
        if missing_post_mapping_keys:
            return self._build_missing_fields_question(missing_post_mapping_keys, campos_faltantes_msg)

        # 8. Tudo OK - Preparar Confirmação Final
        logger.info("Validações pós-mapeamento concluídas sem pendências.")
        logger.info("Preparando resumo final para confirmação.")
        # Passa o request_data atualizado (com cadência formatada) para o resumo
        return self._format_final_summary(self.state["request_data"])


    # --- Método Principal de Processamento ---
    def process_user_input(self, user_text: str, short_term_memory: ConversationBufferMemory, user_id: str,metadata: dict = None) -> dict:
        """
        Processa a entrada do usuário, gerenciando o estado da conversa,
        extração, mapeamento e validação. Prioriza respostas a perguntas pendentes.
        """
        if not self.state.get("request_data"):
            self._pre_fill_from_profile(user_id)
        logger.info(f"--- Ciclo Orquestrador: Processando Input ---")
        logger.info(f"Texto do Usuário: '{user_text}'")
        self.state["current_original_input_text"] = user_text
        logger.debug(f"Estado ANTES do processamento (current_original_input_text SET): {json.dumps(self.state, indent=2, ensure_ascii=False)}")

        # PASSO 0: CLASSIFICAR INTENÇÃO
        intent = self._classify_intent(user_text)

        # Se a intenção for responder uma pergunta, delegue ao KnowledgeAgent
        if intent in ['PERGUNTA_SOBRE_A_CONVERSA', 'PERGUNTA_SOBRE_O_HISTORICO', 'CONVERSA_GERAL']:
            if self.knowledge_agent:
                answer = self.knowledge_agent.answer_question(
                    user_id=user_id,
                    question=user_text,
                    intent=intent,
                    chat_history=short_term_memory.buffer_as_str
                )
                return {"status": "answered", "message": answer}
            else:
                return {"status": "error", "message": "Desculpe, a função de responder perguntas não está disponível no momento."}

        # Se a intenção for CRIAR_TICKET, prossiga com o fluxo original
        self.state["current_original_input_text"] = user_text


        # --- PASSO 0: TRATAR RESPOSTAS PENDENTES PRIMEIRO ---
        if self.state.get("pending_confirmation") and self.state.get("last_question_context") == "confirmation_response":
            logger.info(">>> Estado é 'pending_confirmation'. Chamando _handle_confirmation_response.")
            confirmation_result = self._handle_confirmation_response(
                user_text, 
                user_id=user_id,
                short_term_memory=short_term_memory # << Adicionado
            ) # user_text aqui é Sim/Não/Correção
            # Se for correção, current_original_input_text será o texto da correção, que é o esperado por _handle_user_edit
            if confirmation_result:
                return confirmation_result
            else:
                 logger.warning("!!! _handle_confirmation_response retornou None inesperadamente.")
                 return self._format_error("Ocorreu um problema ao processar sua resposta de confirmação.")

        if self.state.get("pending_ambiguity"):
            logger.info(">>> Estado é 'pending_ambiguity'. Chamando _handle_ambiguity_response.")
            ambiguity_result = self._handle_ambiguity_response(user_text) # user_text é a escolha da ambiguidade
            if ambiguity_result:
                return ambiguity_result
            else:
                 logger.warning("!!! _handle_ambiguity_response retornou None inesperadamente.")
                 return self._format_error("Ocorreu um problema ao processar sua resposta para a ambiguidade.")


        if self.state.get("last_question_context") == "awaiting_user_correction_text":
            logger.info(">>> Contexto é 'awaiting_user_correction_text'. Chamando _handle_user_edit com o texto da correção.")
            edit_result = self._handle_user_edit(user_text) 
            return edit_result

        # --- PASSO 1: EXTRAÇÃO (Só executa se não for resposta a confirmação/ambiguidade) ---
        logger.info(">>> Input não é confirmação/ambiguidade direta. Iniciando extração normal.")
        last_asked = self.state.get("last_asked_fields")
        extracted_data = self.extraction_agent.extract(
            user_text, 
            memory=short_term_memory, # Passa a memória
            context_fields=last_asked
        )

        # --- Tratamento de Falha na Extração (mantém a lógica atual) ---
        if extracted_data is None:
            logger.error(f"Extração retornou None para o input '{user_text[:50]}...' mesmo após retentativas.")
            if user_text.strip(): 
                if last_asked: 
                    fields_str = ', '.join(last_asked)
                    return self._format_user_question(
                        f"Desculpe, tive um problema temporário ao processar sua resposta para '{fields_str}'. Poderia tentar fornecer novamente?",
                        context=self.state.get("last_question_context"), 
                        missing_fields=last_asked 
                    )
                else: 
                    return self._format_user_question(
                        "Desculpe, tive um problema temporário para processar sua solicitação. Poderia tentar enviar os dados novamente?",
                        context="extraction_failed_initial"
                    )
            else: 
                return self._format_user_question("Por favor, insira os dados do pedido.", context="empty_input")
        # --- FIM DO TRATAMENTO DE FALHA NA EXTRAÇÃO ---


        # --- PASSO 2: FILTRAGEM DA EXTRAÇÃO BASEADA NO CONTEXTO ---
        # Usa a função _filter_extraction_by_context que já existe e limpa o contexto internamente
        logger.debug(">>> Aplicando filtro de contexto (se aplicável).")
        filtered_data = self._filter_extraction_by_context(extracted_data)
        
        original_last_context = self.state.get("last_question_context") # Captura antes de _filter_extraction_by_context limpar
        original_last_asked = self.state.get("last_asked_fields")     # Captura antes de _filter_extraction_by_context limpar
        # _filter_extraction_by_context já limpou o contexto do ESTADO se ele foi usado.

        if original_last_context and original_last_asked and not filtered_data and extracted_data:
            logger.warning("[Filtro Contexto Pós-Execução] Filtragem removeu todos os campos extraídos válidos.")
            fields_str = ', '.join(original_last_asked)
            # Repergunta, restaurando o contexto para a nova pergunta.
            return self._format_user_question(
                f"Não consegui identificar a informação para '{fields_str}' na sua resposta. Poderia fornecer novamente?",
                context=original_last_context, 
                missing_fields=original_last_asked 
            )

        # --- PASSO 3: ATUALIZAÇÃO DO ESTADO (request_data) ---
        data_to_update = filtered_data if (original_last_context and original_last_asked) else extracted_data
        logger.debug(f">>> Atualizando estado com dados {'filtrados' if (original_last_context and original_last_asked) else 'extraídos'}.")
        if metadata: # Adiciona metadados ANTES de atualizar com dados da extração, para que possam ser sobrescritos se necessário
            logger.debug(f"Processando com metadata: {metadata}")
            if 'vendedor_id' in metadata:
                current_req_data_for_meta = self.state.get("request_data", {})
                if not current_req_data_for_meta.get("Vendedor"): # Só define se não existir
                    current_req_data_for_meta["Vendedor"] = metadata['vendedor_id']
                    # Não chama _update_request_data aqui, pois data_to_update será usado em seguida
                    self.state["request_data"] = current_req_data_for_meta 
                    logger.debug(f"Set Vendedor from metadata: {metadata['vendedor_id']}")
        
        self._update_request_data(data_to_update)


        # --- PASSO 4 & 5: MAPEAMENTO E VALIDAÇÃO PÓS-MAPEAMENTO ---
        logger.info(">>> Iniciando mapeamento e validação final.")
        # _run_mapping_and_validation usará self.state["current_original_input_text"]
        return self._run_mapping_and_validation() # Não precisa mais passar user_text