import pandas as pd
import os
import re
import json # Para debug
import logging
from utils.normalization import normalize_string
import config
from thefuzz import fuzz

logger = logging.getLogger(__name__)

class MappingAgent:
    """
    Agente responsável por realizar o mapeamento ('de-para') dos dados extraídos
    com base em planilhas CSV e identificar inconsistências ou ambiguidades.
    """
    def __init__(self, artifacts_dir: str = config.ARTIFACTS_DIR):
        self.artifacts_dir = artifacts_dir
        self.df_precofixo = None
        self.df_material = None
        self.df_condicao = None
        self.df_forma = None
        self.df_planta = None # NOVO: DataFrame para plantas
        self._initialize_empty_sets() # Inicializa conjuntos vazios primeiro
        self.data_loaded_successfully = self._load_and_preprocess_data()
        if self.data_loaded_successfully:
            logger.info("Agente de Mapeamento inicializado e dados carregados.")
            self._preload_valid_codes_and_terms() # Renomeado para clareza
            self._preload_planta_codes() # NOVO: Carregar códigos de planta
        else:
            logger.error("Falha ao carregar dados para o Agente de Mapeamento.")
            # _initialize_empty_sets() já foi chamado

    def _initialize_empty_sets(self):
        """Inicializa conjuntos de códigos e termos como vazios."""
        self.valid_material_codes = set()
        self.valid_condicao_codes = set()
        self.valid_forma_codes = set()
        self.valid_planta_codes = set() # NOVO: Set para códigos de planta
        self.forma_terms_norm = set()
        self.condicao_terms_norm = set()
        self.forma_map_norm_to_code = {} # Mapeia termo normalizado -> código original
        self.condicao_map_norm_to_code = {}
        # NOVO: Para lidar com sinônimos que podem ser ambíguos
        self.ambiguous_forma_terms = set()
        self.forma_keyword_map_norm_to_codes = {} # Mapeia keyword -> lista de códigos MP

    def _preload_planta_codes(self): # NOVO MÉTODO
        """Pré-carrega códigos de planta válidos."""
        if self._is_valid_df(self.df_planta, 'Códigos'):
            # Converte para string, remove NaNs/None, normaliza para maiúsculas e remove espaços
            self.valid_planta_codes = set(
                code.strip().upper()
                for code in self.df_planta['Códigos'].dropna().astype(str)
                if code.strip()
            )
            logger.info(f"Códigos de planta válidos pré-carregados: {self.valid_planta_codes}")
        else:
            logger.warning("DataFrame de planta ou coluna 'Códigos' não encontrada. Mapeamento de planta por CSV desabilitado.")
            self.valid_planta_codes = set()

    def _preload_valid_codes_and_terms(self):
        """Pré-carrega conjuntos de códigos válidos e termos normalizados para otimização."""
        if self._is_valid_df(self.df_material, 'Cód'):
            self.valid_material_codes = set(self.df_material['Cód'].astype(str))

        if self._is_valid_df(self.df_condicao, 'Cond Pagamento'):
            self.valid_condicao_codes = set(self.df_condicao['Cond Pagamento'].astype(str))
            for _, row in self.df_condicao.iterrows():
                code_orig = str(row['Cond Pagamento'])
                code_norm = row.get('Cond_Pagamento_NORMALIZADO') # Termo do próprio código, normalizado
                sig_norm = row.get('Significado_NORMALIZADO')   # Significado da condição, normalizado

                # Adiciona o código original normalizado (se existir) ao set de termos e ao mapa
                if pd.notna(code_norm) and code_norm.strip(): # Verifica se não é NaN e não é vazio
                    self.condicao_terms_norm.add(code_norm)
                    # Prioriza mapear para o código original mais curto se já existir um mapeamento
                    if code_norm not in self.condicao_map_norm_to_code or \
                       len(code_orig) < len(self.condicao_map_norm_to_code[code_norm]):
                        self.condicao_map_norm_to_code[code_norm] = code_orig
                
                # Adiciona o significado normalizado (se existir) ao set de termos e ao mapa
                if pd.notna(sig_norm) and sig_norm.strip(): # Verifica se não é NaN e não é vazio
                    self.condicao_terms_norm.add(sig_norm)
                    if sig_norm not in self.condicao_map_norm_to_code or \
                       len(code_orig) < len(self.condicao_map_norm_to_code[sig_norm]):
                        self.condicao_map_norm_to_code[sig_norm] = code_orig

        if self._is_valid_df(self.df_forma, 'MP'):
            self.valid_forma_codes = set(self.df_forma['MP'].astype(str))
            for _, row in self.df_forma.iterrows():
                code_orig = str(row['MP'])

                # Termos diretos (MP e Significado)
                terms_direct = []
                mp_norm = row.get('MP_NORMALIZADO')
                sig_norm = row.get('Significado_NORMALIZADO')
                if pd.notna(mp_norm) and mp_norm.strip():
                    terms_direct.append(mp_norm)
                if pd.notna(sig_norm) and sig_norm.strip():
                    terms_direct.append(sig_norm)

                for term_norm in terms_direct:
                    self.forma_terms_norm.add(term_norm)
                    # Se já existe e aponta para código diferente, é ambíguo (improvável para MP/Significado)
                    if term_norm in self.forma_map_norm_to_code and self.forma_map_norm_to_code[term_norm] != code_orig:
                        logger.warning(f"Termo direto '{term_norm}' mapeando para múltiplos códigos: {self.forma_map_norm_to_code[term_norm]} e {code_orig}. Verifique dados.")
                        # Decide estratégia: sobrescrever, ignorar, ou marcar como ambíguo
                    self.forma_map_norm_to_code[term_norm] = code_orig

                # Palavras-chave/Sinônimos
                keywords_norm_list = row.get('PalavrasChave_NORMALIZADAS', [])
                for kw_norm in keywords_norm_list:
                    if kw_norm: # Ignora strings vazias que podem vir do split
                        self.forma_terms_norm.add(kw_norm) # Adiciona ao conjunto geral de termos

                        # Construir o mapa de keywords para LISTA de códigos
                        # Isso permite que uma keyword ("ted") possa mapear para múltiplos códigos se necessário
                        if kw_norm not in self.forma_keyword_map_norm_to_codes:
                            self.forma_keyword_map_norm_to_codes[kw_norm] = []

                        # Adiciona o código apenas se ainda não estiver na lista para essa keyword
                        if code_orig not in self.forma_keyword_map_norm_to_codes[kw_norm]:
                            self.forma_keyword_map_norm_to_codes[kw_norm].append(code_orig)

            # Identificar keywords ambíguas (mapeiam para mais de um código)
            for kw, codes in self.forma_keyword_map_norm_to_codes.items():
                if len(codes) > 1:
                    self.ambiguous_forma_terms.add(kw)
                    logger.info(f"Keyword de Forma de Pagamento '{kw}' é ambígua, mapeia para: {codes}")


        logger.debug("Conjuntos de códigos válidos e termos normalizados pré-carregados (Forma de Pagamento).")
        logger.debug(f"Forma - Mapa Termo Direto->Código: {self.forma_map_norm_to_code}")
        logger.debug(f"Forma - Mapa Keyword->Códigos: {self.forma_keyword_map_norm_to_codes}")
        logger.debug(f"Forma - Keywords Ambíguas: {self.ambiguous_forma_terms}")
        
    def _preload_valid_codes(self):
        """Pré-carrega conjuntos de códigos válidos para otimização."""
        self.valid_material_codes = set(self.df_material['Cód'].astype(str)) if self._is_valid_df(self.df_material, 'Cód') else set()
        self.valid_condicao_codes = set(self.df_condicao['Cond Pagamento'].astype(str)) if self._is_valid_df(self.df_condicao, 'Cond Pagamento') else set()
        self.valid_forma_codes = set(self.df_forma['MP'].astype(str)) if self._is_valid_df(self.df_forma, 'MP') else set()
        logger.debug("Conjuntos de códigos válidos pré-carregados.")

    def _is_valid_df(self, df, column_name):
         """Verifica se o DataFrame é válido e contém a coluna."""
         return df is not None and column_name in df.columns

    def _load_and_preprocess_data(self) -> bool:
        """Carrega e pré-processa os dados das planilhas CSV."""
        try:
            # Carrega os DataFrames (como antes)
            self.df_precofixo = self._read_csv("precofixo-de-para.csv", dtype={'CNPJ/CPF': str, 'Cliente': str, 'Nome Cliente': str})
            self.df_material = self._read_csv("material.csv", dtype={'Cód': str, 'Produto': str})
            self.df_condicao = self._read_csv("condicao-de-pagamento.csv", dtype={'Cond Pagamento': str, 'Significado': str})
            self.df_forma = self._read_csv("forma-de-pagamento.csv", dtype={'MP': str, 'Significado': str, 'PalavrasChave': str})
            self.df_planta = self._read_csv("planta.csv", dtype={'Plantas': str, 'Códigos': str}) # NOVO: Carregar planta.csv

    
            # --- Pré-processamento com remoção de hífens para campos relevantes ---
            if self._is_valid_df(self.df_forma, 'PalavrasChave'):
                # Converte NaN para string vazia, normaliza e remove hífens das keywords
                self.df_forma['PalavrasChave_NORMALIZADAS'] = self.df_forma['PalavrasChave'].fillna('').astype(str).apply(
                    lambda x: [normalize_string(kw, remove_hyphens=True) for kw in x.split('|') if kw.strip()] # <--- Flag True aqui
                )
            else:
                logger.warning("(MappingAgent): Coluna 'PalavrasChave' não encontrada no CSV de forma de pagamento.")
                # Garante que a coluna exista mesmo assim, vazia.
                self.df_forma['PalavrasChave_NORMALIZADAS'] = pd.Series([[] for _ in range(len(self.df_forma))])
    
            # CNPJ/CPF já tem hífens removidos por outra lógica, não precisa da flag aqui
            if self._is_valid_df(self.df_precofixo, 'CNPJ/CPF'):
                self.df_precofixo['CNPJ_CPF_NORMALIZADO'] = self.df_precofixo['CNPJ/CPF'].str.replace(r'[./-]', '', regex=True).fillna('')
            else:
                logger.warning("(MappingAgent): Coluna 'CNPJ/CPF' não encontrada para normalização.")
    
            # Campos de texto onde hífens devem ser removidos no pré-processamento do CSV
            self._preprocess_dataframe(self.df_precofixo, 'Nome Cliente', 'Nome_Cliente_NORMALIZADO',
                                    func=normalize_string, remove_hyphens=True) # <--- Flag True
            self._preprocess_dataframe(self.df_material, 'Produto', 'Produto_NORMALIZADO',
                                    func=normalize_string, remove_hyphens=True) # <--- Flag True
            self._preprocess_dataframe(self.df_condicao, 'Significado', 'Significado_NORMALIZADO',
                                    func=normalize_string, remove_hyphens=True) # <--- Flag True
            self._preprocess_dataframe(self.df_condicao, 'Cond Pagamento', 'Cond_Pagamento_NORMALIZADO',
                                    func=normalize_string, remove_hyphens=True) # <--- Flag True (Se Cond Pag pode ter hífen, ex: '30-dias')
            self._preprocess_dataframe(self.df_forma, 'Significado', 'Significado_NORMALIZADO',
                                    func=normalize_string, remove_hyphens=True) # <--- Flag True
            self._preprocess_dataframe(self.df_forma, 'MP', 'MP_NORMALIZADO',
                                    func=normalize_string, remove_hyphens=True) # <--- Flag True (Se MP pode ter hífen, ex: 'C-CRED')
    
            # Campos onde hífens NÃO devem ser removidos (se houvesse outros aqui)
            # Exemplo: self._preprocess_dataframe(self.df_algum, 'OutroCampo', 'OutroCampo_NORMALIZADO',
            #                            func=normalize_string, remove_hyphens=False) # Ou omitir remove_hyphens
    
            logger.debug("(MappingAgent): DataFrames de mapeamento carregados e normalizados (com remoção seletiva de hífens).")
            return True
    
        except FileNotFoundError as e:
            logger.error(f"(MappingAgent): Arquivo CSV não encontrado. Verifique o caminho: {e}")
            return False
        except AttributeError as ae:
            logger.error(f"(MappingAgent): Erro de atributo durante pré-processamento (provável problema com apply/lambda): {ae}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"(MappingAgent): Erro ao carregar ou processar arquivos CSV: {e}", exc_info=True)
            return False

        
    def _attempt_split_and_remap_payment(self, input_value: str, mapped_data: dict, issues: dict) -> bool:
        """
        Tenta dividir um valor de entrada (ex: 'ted-a-vista') em Forma e Condição de Pagamento.
        Atualiza mapped_data se uma divisão válida for encontrada. Retorna True se bem-sucedido.
        USA normalize_string com remove_hyphens=True.
        """
        if not input_value or not isinstance(input_value, str):
            return False

        # Normaliza o input removendo hífens para comparar com os termos pré-processados
        input_norm = normalize_string(input_value, remove_hyphens=True)
        if not input_norm: # Se a normalização resultar em string vazia
            return False

        logger.debug(f"Tentando dividir valor de pagamento combinado: '{input_value}' (Normalizado s/ hífen: '{input_norm}')")

        best_match_forma_code = None
        best_match_condicao_code = None
        max_combined_len = 0 # Prioriza o match que cobre a maior parte da string original

        # Ordena por comprimento decrescente para encontrar matches mais específicos/longos primeiro
        # Isso é crucial para que "ted" seja tentado antes de "d" (se "d" fosse um termo válido sozinho)
        sorted_forma_terms = sorted(list(self.forma_terms_norm), key=len, reverse=True)
        sorted_condicao_terms = sorted(list(self.condicao_terms_norm), key=len, reverse=True)

        # Cenário 1: Forma de Pagamento primeiro, depois Condição de Pagamento
        logger.debug("Split: Tentando Forma + Condição")
        for forma_term_norm in sorted_forma_terms:
            if forma_term_norm in input_norm:
                # Tenta encontrar o forma_term_norm no início, meio ou fim da string normalizada.
                # Regex para encontrar o termo e capturar o que vem antes e depois.
                # Usamos re.escape para o termo, pois ele pode conter caracteres especiais após a normalização.
                escaped_forma_term = re.escape(forma_term_norm)
                
                # Tentamos encontrar o forma_term como uma palavra inteira, se possível,
                # ou como substring. \b ajuda a casar palavras inteiras.
                # Se o termo for curto (ex: 'd'), \b pode ser restritivo demais.
                # Para termos mais longos, \b é bom.
                # Vamos tentar algumas variações de regex para encontrar o "resto".
                
                # Variação 1: O termo da forma está no início
                match = re.match(rf"({escaped_forma_term})(\b\s*|\s+|$)(.*)", input_norm, re.IGNORECASE)
                if match:
                    remainder_norm = normalize_string(match.group(3), remove_hyphens=True) # O que sobrou depois do termo da forma
                    logger.debug(f"  Forma Candidata (início): '{forma_term_norm}', Remainder: '{remainder_norm}'")
                    if remainder_norm: # Só busca condição se sobrou algo
                        for condicao_term_norm in sorted_condicao_terms:
                            if remainder_norm == condicao_term_norm: # Match exato do restante
                                current_len = len(forma_term_norm) + len(condicao_term_norm)
                                if current_len > max_combined_len:
                                    temp_forma_code = self.forma_map_norm_to_code.get(forma_term_norm)
                                    temp_condicao_code = self.condicao_map_norm_to_code.get(condicao_term_norm)
                                    if temp_forma_code and temp_condicao_code:
                                        max_combined_len = current_len
                                        best_match_forma_code = temp_forma_code
                                        best_match_condicao_code = temp_condicao_code
                                        logger.info(f"    SPLIT ACHADO (F+C, início): '{forma_term_norm}' ({best_match_forma_code}) + '{condicao_term_norm}' ({best_match_condicao_code})")
                                break # Achou a melhor condição para este forma_term

                # Variação 2: O termo da forma está no fim (menos comum para "Forma + Condição")
                match = re.match(rf"(.*)(\b\s*|\s+)({escaped_forma_term})$", input_norm, re.IGNORECASE)
                if match and not best_match_forma_code: # Se ainda não achou um match melhor
                    remainder_norm = normalize_string(match.group(1)) # O que veio antes
                    logger.debug(f"  Forma Candidata (fim): '{forma_term_norm}', Remainder (antes): '{remainder_norm}'")
                    # Isso é mais para o cenário Condição + Forma, mas verificamos
                    # Aqui, o remainder seria a condição
                    if remainder_norm:
                        for condicao_term_norm in sorted_condicao_terms:
                            if remainder_norm == condicao_term_norm:
                                current_len = len(forma_term_norm) + len(condicao_term_norm)
                                if current_len > max_combined_len:
                                    temp_forma_code = self.forma_map_norm_to_code.get(forma_term_norm)
                                    temp_condicao_code = self.condicao_map_norm_to_code.get(condicao_term_norm)
                                    if temp_forma_code and temp_condicao_code:
                                        max_combined_len = current_len
                                        best_match_forma_code = temp_forma_code
                                        best_match_condicao_code = temp_condicao_code
                                        logger.info(f"    SPLIT ACHADO (C+F, com forma no fim): '{condicao_term_norm}' ({best_match_condicao_code}) + '{forma_term_norm}' ({best_match_forma_code})")
                                break
                
                # Variação 3: O termo da forma está no meio (mais complexo, mas pode ser necessário)
                # Ex: "pagar com ted a vista" -> forma_term = "ted", antes="pagar com ", depois=" a vista"
                # Vamos simplificar por agora e focar em início/fim, que cobre "TED a vista" e "a vista TED".


        # Cenário 2: Condição de Pagamento primeiro, depois Forma de Pagamento
        # Só executa se o primeiro cenário não encontrou um match que cobre grande parte da string
        # ou se queremos ter certeza que a outra ordem não produz um match melhor (mais longo)
        logger.debug("Split: Tentando Condição + Forma")
        for condicao_term_norm in sorted_condicao_terms:
            if condicao_term_norm in input_norm:
                escaped_condicao_term = re.escape(condicao_term_norm)

                # Variação 1: Condição no início
                match = re.match(rf"({escaped_condicao_term})(\b\s*|\s+|$)(.*)", input_norm, re.IGNORECASE)
                if match:
                    remainder_norm = normalize_string(match.group(3))
                    logger.debug(f"  Condição Candidata (início): '{condicao_term_norm}', Remainder: '{remainder_norm}'")
                    if remainder_norm:
                        for forma_term_norm in sorted_forma_terms:
                            if remainder_norm == forma_term_norm:
                                current_len = len(condicao_term_norm) + len(forma_term_norm)
                                if current_len > max_combined_len: # Verifica se este é um match melhor
                                    temp_condicao_code = self.condicao_map_norm_to_code.get(condicao_term_norm)
                                    temp_forma_code = self.forma_map_norm_to_code.get(forma_term_norm)
                                    if temp_condicao_code and temp_forma_code:
                                        max_combined_len = current_len
                                        best_match_condicao_code = temp_condicao_code
                                        best_match_forma_code = temp_forma_code
                                        logger.info(f"    SPLIT ACHADO (C+F, início): '{condicao_term_norm}' ({best_match_condicao_code}) + '{forma_term_norm}' ({best_match_forma_code})")
                                break
                
                # Variação 2: Condição no fim (menos comum para "Condição + Forma")
                match = re.match(rf"(.*)(\b\s*|\s+)({escaped_condicao_term})$", input_norm, re.IGNORECASE)
                if match and (not best_match_condicao_code or (len(condicao_term_norm) + len(normalize_string(match.group(1)))) > max_combined_len ) :
                    remainder_norm = normalize_string(match.group(1)) # O que veio antes (seria a forma)
                    logger.debug(f"  Condição Candidata (fim): '{condicao_term_norm}', Remainder (antes): '{remainder_norm}'")
                    if remainder_norm:
                        for forma_term_norm in sorted_forma_terms:
                            if remainder_norm == forma_term_norm:
                                current_len = len(condicao_term_norm) + len(forma_term_norm)
                                if current_len > max_combined_len:
                                    temp_condicao_code = self.condicao_map_norm_to_code.get(condicao_term_norm)
                                    temp_forma_code = self.forma_map_norm_to_code.get(forma_term_norm)
                                    if temp_condicao_code and temp_forma_code:
                                        max_combined_len = current_len
                                        best_match_condicao_code = temp_condicao_code
                                        best_match_forma_code = temp_forma_code
                                        logger.info(f"    SPLIT ACHADO (F+C, com cond no fim): '{forma_term_norm}' ({best_match_forma_code}) + '{condicao_term_norm}' ({best_match_condicao_code})")
                                break


        if best_match_forma_code and best_match_condicao_code:
            logger.info(f"MELHOR SPLIT encontrado para '{input_value}': Forma='{best_match_forma_code}', Condição='{best_match_condicao_code}' (comprimento combinado dos termos: {max_combined_len})")

            # Atualiza os dados mapeados SE eles ainda não foram definidos por um mapeamento direto melhor
            # ou se o valor atual é o input original que falhou no mapeamento direto.
            # Consideramos o split bem-sucedido se ele encontrou AMBOS os códigos.
            # Ele sobrescreve o valor original se este ainda estiver lá.
            if mapped_data.get("Forma de Pagamento") == input_value or mapped_data.get("Forma de Pagamento") is None:
                 mapped_data["Forma de Pagamento"] = best_match_forma_code
            if mapped_data.get("Condição de Pagamento") == input_value or mapped_data.get("Condição de Pagamento") is None:
                 mapped_data["Condição de Pagamento"] = best_match_condicao_code

            # Remove potenciais avisos antigos sobre esse input específico, se houver
            # Esta parte é importante para limpar avisos de "não encontrado" após um split bem-sucedido.
            if "avisos" in issues:
                issues["avisos"] = [
                    aviso for aviso in issues["avisos"]
                    if not (
                        (aviso.get("campo") == "Forma de Pagamento" or aviso.get("campo") == "Condição de Pagamento") and
                        aviso.get("valor_original") == input_value
                    )
                ]
            return True
        else:
            logger.debug(f"Não foi possível dividir '{input_value}' em uma combinação válida e completa de Forma/Condição.")
            return False

    def _read_csv(self, filename: str, dtype: dict = None) -> pd.DataFrame | None:
        """Lê um arquivo CSV do diretório de artefatos."""
        path = os.path.join(self.artifacts_dir, filename)
        if not os.path.exists(path):
            logger.error(f"(MappingAgent): Arquivo não encontrado: {path}")
            raise FileNotFoundError(path)
        try:
            return pd.read_csv(path, dtype=dtype)
        except Exception as e:
            logger.error(f"(MappingAgent): Erro ao ler CSV {filename}: {e}")
            raise # Re-levanta a exceção para ser pega no _load_and_preprocess_data

    def _preprocess_dataframe(self, df: pd.DataFrame | None, col_orig: str, col_norm: str, func, **kwargs):
        """Aplica uma função de normalização a uma coluna se ela existir, passando kwargs."""
        if self._is_valid_df(df, col_orig):
            # Aplica a função passando os argumentos extras (como remove_hyphens=True)
            df[col_norm] = df[col_orig].apply(lambda x: func(x, **kwargs) if pd.notna(x) else None) # <--- Adicionado **kwargs
        else:
            logger.warning(f"(MappingAgent): Coluna '{col_orig}' não encontrada para normalização.")

    def _add_issue(self, issues: dict, type: str, issue_data):
        """Adiciona um problema (erro, aviso, ambiguidade) ao dicionário."""
        if type not in issues: issues[type] = []
        issues[type].append(issue_data)

    # --- Métodos de Mapeamento por Campo ---

    def _map_planta(self, mapped_data: dict, issues: dict, original_input_text: str): # NOVO MÉTODO
        """
        Mapeia o campo 'Planta'.
        Prioridade 1: Valor extraído pelo LLM (se for ou contiver um código válido).
        Prioridade 2: Busca por substring dos códigos válidos no texto original.
        """
        if not self.valid_planta_codes:
            logger.debug("(MappingAgent Planta): Nenhum código de planta válido carregado. Pulando mapeamento de planta.")
            return

        planta_extraida_llm_original = mapped_data.get("Planta")
        planta_final = None
        logger.debug(f"(MappingAgent Planta): Iniciando mapeamento. Extraído LLM: '{planta_extraida_llm_original}'. Texto Original: '{original_input_text[:100]}...'")

        # Etapa 1: Verificar o valor extraído pelo LLM
        if planta_extraida_llm_original:
            planta_extraida_llm_norm_upper = normalize_string(str(planta_extraida_llm_original), remove_hyphens=True)
            if planta_extraida_llm_norm_upper: # Verifica se a normalização não resultou em vazio
                planta_extraida_llm_norm_upper = planta_extraida_llm_norm_upper.upper()

                # Verifica se o valor normalizado é diretamente um código válido
                if planta_extraida_llm_norm_upper in self.valid_planta_codes:
                    planta_final = planta_extraida_llm_norm_upper
                    logger.info(f"(MappingAgent Planta): Planta extraída pelo LLM '{planta_extraida_llm_original}' é um código válido: '{planta_final}'.")
                else:
                    # Verifica se algum código válido é substring do valor extraído pelo LLM
                    # Ex: LLM extraiu "FS PDL" ou "CADÊNCIA LRV"
                    found_codes_in_llm_extraction = []
                    for code in self.valid_planta_codes:
                        # Usar word boundaries para evitar matches parciais indesejados (ex: "D" em "CADENCIA")
                        # No entanto, para "PDL" em "FS PDL", \bPDL\b não funcionaria se não houver espaço antes.
                        # Uma busca por substring simples é mais flexível aqui, dado o exemplo.
                        if code in planta_extraida_llm_norm_upper:
                            found_codes_in_llm_extraction.append(code)
                    
                    if len(found_codes_in_llm_extraction) == 1:
                        planta_final = found_codes_in_llm_extraction[0]
                        logger.info(f"(MappingAgent Planta): Código de planta '{planta_final}' encontrado dentro da extração do LLM ('{planta_extraida_llm_original}').")
                    elif len(found_codes_in_llm_extraction) > 1:
                        # Raro, mas se LLM extrair algo como "PDL ou LRV", trata como ambiguidade
                        # Ou se a normalização de códigos levar a colisões (ex: se um código fosse "P" e outro "PDL")
                        opcoes_ambiguidade = [{"codigo": c, "descricao": f"Código {c} (encontrado em '{planta_extraida_llm_original}')"} for c in found_codes_in_llm_extraction]
                        self._add_issue(issues, "ambiguidades", {
                            "campo": "Planta", "valor_original": planta_extraida_llm_original,
                            "mensagem": f"Múltiplos códigos de planta válidos ({', '.join(found_codes_in_llm_extraction)}) foram encontrados na informação de planta fornecida ('{planta_extraida_llm_original}'). Qual é o correto?",
                            "opcoes": opcoes_ambiguidade
                        })
                        logger.warning(f"(MappingAgent Planta): Ambiguidade na extração LLM para Planta: '{planta_extraida_llm_original}', códigos encontrados: {found_codes_in_llm_extraction}.")
                        # Não define planta_final, deixa a ambiguidade ser resolvida
                        mapped_data["Planta"] = planta_extraida_llm_original # Mantém o valor original para a pergunta de ambiguidade
                        return # Retorna para que a ambiguidade seja tratada

        # Etapa 2: Se não encontrou planta via LLM, buscar no texto original completo
        if not planta_final and original_input_text:
            logger.debug(f"(MappingAgent Planta): Planta não resolvida pela extração LLM. Buscando no texto original.")
            # Normaliza o texto original para busca case-insensitive
            # Não remover hífens aqui, pois os códigos podem não ter hífen e a busca é por substring exata do código.
            # Garantir que os códigos em self.valid_planta_codes já estão em maiúsculas.
            texto_original_upper = original_input_text.upper() 
            
            codigos_encontrados_no_texto = []
            for code_planta in self.valid_planta_codes: # Estes já estão em UPPER
                # Busca pelo código exato (case-insensitive devido ao texto_original_upper)
                # Usar word boundaries \b para garantir que estamos pegando o código isolado
                # e não como parte de outra palavra (ex: "SRS" em "SENSORES").
                # Pattern: \bCODE\b
                # No entanto, se o código estiver junto de outros caracteres sem espaço (ex: "CADENCIALRV"), \b não funciona bem.
                # Uma simples verificação de `code_planta in texto_original_upper` pode ser mais robusta para casos como "CADENCIALRV"
                # Se os códigos são sempre como 'LRV', 'PDL', 'SRS', a chance de falso positivo é menor.
                if code_planta in texto_original_upper:
                    codigos_encontrados_no_texto.append(code_planta)
            
            # Remove duplicatas se um código aparecer várias vezes
            codigos_encontrados_unicos = sorted(list(set(codigos_encontrados_no_texto)))

            if len(codigos_encontrados_unicos) == 1:
                planta_final = codigos_encontrados_unicos[0]
                logger.info(f"(MappingAgent Planta): Código de planta '{planta_final}' encontrado por substring no texto original.")
            elif len(codigos_encontrados_unicos) > 1:
                # Ambiguidade: múltiplos códigos de planta encontrados no texto
                opcoes_ambiguidade = [{"codigo": c, "descricao": f"Código {c} (encontrado no texto)"} for c in codigos_encontrados_unicos]
                self._add_issue(issues, "ambiguidades", {
                    "campo": "Planta", "valor_original": original_input_text[:150] + "...", # Mostra parte do texto original
                    "mensagem": f"Múltiplos códigos de planta ({', '.join(codigos_encontrados_unicos)}) foram encontrados no seu pedido. Qual planta você se refere?",
                    "opcoes": opcoes_ambiguidade
                })
                logger.warning(f"(MappingAgent Planta): Ambiguidade na busca por substring no texto original. Códigos encontrados: {codigos_encontrados_unicos}.")
                # Mantém o campo Planta como None ou o que veio do LLM (que não foi resolvido)
                mapped_data["Planta"] = planta_extraida_llm_original if planta_extraida_llm_original else None
                return

        # Atualiza o campo "Planta" em mapped_data se uma planta final foi determinada
        if planta_final:
            mapped_data["Planta"] = planta_final
            logger.debug(f"(MappingAgent Planta): Campo 'Planta' atualizado para '{planta_final}'.")
        elif planta_extraida_llm_original and not any(issue.get("campo") == "Planta" for issue in issues.get("ambiguidades",[])):
            # Se o LLM extraiu algo, mas não foi um código válido nem continha um,
            # E não foi encontrada por substring, E não gerou ambiguidade acima,
            # Adiciona um aviso que o valor extraído não é uma planta conhecida.
            self._add_issue(issues, "avisos", {
                "campo": "Planta", "valor_original": planta_extraida_llm_original,
                "mensagem": f"A planta informada '{planta_extraida_llm_original}' não é um código de planta reconhecido ({', '.join(self.valid_planta_codes)})."
            })
            logger.debug(f"(MappingAgent Planta): Planta extraída pelo LLM '{planta_extraida_llm_original}' não reconhecida e nenhum código encontrado no texto. Mantendo valor original para aviso.")
            # Mantém o valor original em mapped_data["Planta"] para que o Orchestrator o veja como inválido
        else:
            # Se nada foi extraído pelo LLM e nada encontrado no texto, o campo continua None (ou como estava)
            logger.debug(f"(MappingAgent Planta): Nenhuma planta extraída ou encontrada. Campo 'Planta' permanece: '{mapped_data.get('Planta')}'.")

    def _map_cliente(self, mapped_data: dict, issues: dict):
        cnpj_cpf_extraido = mapped_data.get("CNPJ/CPF")
        nome_cliente_extraido = mapped_data.get("Cliente")
        codigo_cliente_extraido = mapped_data.get("Código do cliente") # Este é o que o LLM extraiu como "Código do cliente"

        cliente_encontrado = False
        cliente_ambiguo = False # Flag para indicar se uma ambiguidade foi detectada

        FUZZY_MATCH_THRESHOLD = 70
        FUZZY_SCORER = fuzz.WRatio
        NUM_WORDS_FOR_TIE_BREAK = 2
        TIE_BREAK_SCORE_THRESHOLD = 85

        # --- 0. VERIFICAÇÃO PRÉVIA: Se já temos um Código do Cliente e um CNPJ/CPF consistentes,
        # e o Nome do Cliente também está presente, podemos pular a busca por CNPJ/CPF
        # para evitar re-gerar a ambiguidade se o CNPJ for duplicado mas já escolhemos um cliente.
        if codigo_cliente_extraido and cnpj_cpf_extraido:
            logger.debug(f"(MappingAgent Cliente Etapa 0): Verificando consistência para Cód.Cliente='{codigo_cliente_extraido}' e CNPJ/CPF='{cnpj_cpf_extraido}'")
            # Procura o cliente na planilha PELO CÓDIGO fornecido
            cliente_info_por_codigo = self.df_precofixo[self.df_precofixo['Cliente'].astype(str) == str(codigo_cliente_extraido)]

            if not cliente_info_por_codigo.empty:
                if len(cliente_info_por_codigo) > 1:
                    # Raro, mas se o código do cliente não for único na planilha, isso é um problema de dados.
                    logger.warning(f"(MappingAgent Cliente Etapa 0): Código do cliente '{codigo_cliente_extraido}' é duplicado na planilha de precofixo. Não é possível validar consistência de forma segura.")
                else:
                    cliente_row_planilha = cliente_info_por_codigo.iloc[0]
                    cnpj_da_planilha_para_codigo = cliente_row_planilha.get("CNPJ_CPF_NORMALIZADO")
                    nome_da_planilha = cliente_row_planilha.get("Nome Cliente") # Nome oficial da planilha
                    
                    cnpj_extraido_normalizado = re.sub(r'[./-]', '', str(cnpj_cpf_extraido))

                    # Verifica se o CNPJ fornecido bate com o CNPJ da planilha PARA O CÓDIGO fornecido
                    if cnpj_extraido_normalizado == cnpj_da_planilha_para_codigo:
                        logger.info(f"(MappingAgent Cliente Etapa 0): Código '{codigo_cliente_extraido}' e CNPJ '{cnpj_extraido_normalizado}' são consistentes com a planilha. Usando dados da planilha.")
                        
                        # Sobrescreve os campos com os dados da planilha, que são a fonte da verdade.
                        mapped_data["Código do cliente"] = str(cliente_row_planilha.get("Cliente")) # Garante que é o da planilha
                        mapped_data["CNPJ/CPF"] = cliente_row_planilha.get("CNPJ_CPF_NORMALIZADO", cnpj_extraido_normalizado) # Usa o da planilha
                        mapped_data["Nome do cliente"] = nome_da_planilha
                        mapped_data["Cliente"] = nome_da_planilha # Atualiza também o campo "Cliente" original
                        
                        cliente_encontrado = True
                        
                        # Log para mostrar qual nome foi usado (planilha vs LLM)
                        if nome_cliente_extraido and nome_cliente_extraido != nome_da_planilha:
                            logger.info(f"(MappingAgent Cliente Etapa 0): Nome do cliente atualizado para '{nome_da_planilha}' (da planilha) em vez de '{nome_cliente_extraido}' (extraído LLM).")
                    else:
                        logger.warning(f"(MappingAgent Cliente Etapa 0): Código do cliente '{codigo_cliente_extraido}' encontrado, mas CNPJ fornecido ('{cnpj_extraido_normalizado}') diverge do CNPJ da planilha ('{cnpj_da_planilha_para_codigo}') para este código. Prosseguindo com outras lógicas de busca.")
                        # cliente_encontrado permanece False, para permitir que outras lógicas (busca por CNPJ isolado, etc.) tentem.
            else:
                logger.warning(f"(MappingAgent Cliente Etapa 0): Código do cliente '{codigo_cliente_extraido}' fornecido, mas não encontrado na planilha. Prosseguindo com outras lógicas de busca.")
        
        # --- 1. Tentativa por CNPJ/CPF (se não encontrado/validado na Etapa 0) ---
        if not cliente_encontrado and cnpj_cpf_extraido and self._is_valid_df(self.df_precofixo, 'CNPJ_CPF_NORMALIZADO'):
            cnpj_cpf_extraido_norm = re.sub(r'[./-]', '', str(cnpj_cpf_extraido))
            logger.debug(f"(MappingAgent Cliente Etapa 1): Buscando por CNPJ_CPF_NORMALIZADO = '{cnpj_cpf_extraido_norm}'")
            cliente_info_cnpj = self.df_precofixo[self.df_precofixo['CNPJ_CPF_NORMALIZADO'] == cnpj_cpf_extraido_norm]
            logger.debug(f"(MappingAgent Cliente Etapa 1): Resultado da busca por CNPJ (len={len(cliente_info_cnpj)}): {cliente_info_cnpj.to_dict('records') if not cliente_info_cnpj.empty else 'DataFrame vazio'}")

            if len(cliente_info_cnpj) == 1:
                cliente_row_data = cliente_info_cnpj.iloc[0]
                codigo_cliente_planilha = str(cliente_row_data.get("Cliente")) if pd.notna(cliente_row_data.get("Cliente")) else None
                cnpj_planilha_match = cliente_row_data.get("CNPJ_CPF_NORMALIZADO")
                nome_cliente_planilha = cliente_row_data.get("Nome Cliente")
                
                mapped_data["Código do cliente"] = codigo_cliente_planilha
                mapped_data["CNPJ/CPF"] = cnpj_planilha_match # Usa o da planilha para garantir formato
                mapped_data["Nome do cliente"] = nome_cliente_planilha
                mapped_data["Cliente"] = nome_cliente_planilha # Atualiza também o campo "Cliente" original
                cliente_encontrado = True
                logger.info(f"(MappingAgent Cliente Etapa 1): CNPJ/CPF '{cnpj_cpf_extraido_norm}' mapeado para Código: {codigo_cliente_planilha}, Nome: {nome_cliente_planilha}.")

            elif len(cliente_info_cnpj) > 1: # CNPJ/CPF DUPLICADO
                logger.info(f"(MappingAgent Cliente Etapa 1): CNPJ/CPF '{cnpj_cpf_extraido_norm}' é DUPLICADO ({len(cliente_info_cnpj)} matches).")
                
                # Verifica se um CÓDIGO DE CLIENTE foi fornecido (pelo usuário ou LLM) para tentar desambiguar
                if codigo_cliente_extraido and str(codigo_cliente_extraido).strip(): # Garante que não é None ou vazio
                    logger.debug(f"(MappingAgent Cliente Etapa 1): Tentando desambiguar CNPJ duplicado com Código do Cliente fornecido: '{codigo_cliente_extraido}'")
                    cliente_row_data_desambiguado = cliente_info_cnpj[cliente_info_cnpj['Cliente'].astype(str) == str(codigo_cliente_extraido)]
                    
                    if len(cliente_row_data_desambiguado) == 1:
                        logger.info(f"(MappingAgent Cliente Etapa 1): CNPJ/CPF duplicado DESAMBIGUADO com sucesso pelo Código do cliente '{codigo_cliente_extraido}'.")
                        cliente_final_row = cliente_row_data_desambiguado.iloc[0]
                        mapped_data["Código do cliente"] = str(cliente_final_row.get("Cliente"))
                        mapped_data["CNPJ/CPF"] = cliente_final_row.get("CNPJ_CPF_NORMALIZADO", cnpj_cpf_extraido_norm)
                        mapped_data["Nome do cliente"] = cliente_final_row.get("Nome Cliente")
                        mapped_data["Cliente"] = cliente_final_row.get("Nome Cliente")
                        cliente_encontrado = True
                    elif len(cliente_row_data_desambiguado) == 0:
                        logger.warning(f"(MappingAgent Cliente Etapa 1): CNPJ/CPF '{cnpj_cpf_extraido_norm}' duplicado, e Código do Cliente '{codigo_cliente_extraido}' fornecido NÃO corresponde a nenhuma das opções. Gerando ambiguidade.")
                        cliente_ambiguo = True # Força a ambiguidade para ser tratada abaixo
                    else: # Mais de um match mesmo com código + CNPJ (problema de dados na planilha)
                         logger.error(f"(MappingAgent Cliente Etapa 1): CNPJ/CPF '{cnpj_cpf_extraido_norm}' e Código '{codigo_cliente_extraido}' resultaram em múltiplos matches na planilha. Problema de dados. Gerando ambiguidade.")
                         cliente_ambiguo = True
                else: # CNPJ duplicado e NENHUM código de cliente foi fornecido (ou era None/vazio)
                    logger.info(f"(MappingAgent Cliente Etapa 1): CNPJ/CPF '{cnpj_cpf_extraido_norm}' duplicado e NENHUM Código do Cliente foi fornecido/extraído para desambiguar. Gerando ambiguidade.")
                    cliente_ambiguo = True
                
                # Se, após as tentativas de desambiguação, ainda for considerado ambíguo:
                if cliente_ambiguo:
                    nomes_planilha = cliente_info_cnpj["Nome Cliente"].tolist()
                    codigos_cliente_planilha_opts = cliente_info_cnpj["Cliente"].astype(str).tolist() # Renomeado para evitar conflito
                    
                    opcoes_ambiguidade = []
                    for nome_p, codigo_p in zip(nomes_planilha, codigos_cliente_planilha_opts):
                        opcoes_ambiguidade.append({
                            "nome": nome_p, 
                            "codigo": codigo_p, 
                            "cnpj_cpf": cnpj_cpf_extraido_norm 
                        })

                    mensagem_ambiguidade = (
                        f"O CNPJ/CPF '{cnpj_cpf_extraido_norm}' está associado a múltiplos clientes. "
                        "Por favor, escolha o correto (informe o número da opção ou o Código do Cliente):\n"
                    )
                    for i, opt in enumerate(opcoes_ambiguidade):
                        mensagem_ambiguidade += f"{i+1}. {opt['nome']} (Código do Cliente: {opt['codigo']})\n"
                    
                    self._add_issue(issues, "ambiguidades", {
                        "campo": "Código do cliente", 
                        "original_field_name": "Cliente", 
                        "valor_original": cnpj_cpf_extraido_norm, 
                        "mensagem": mensagem_ambiguidade.strip(),
                        "opcoes": opcoes_ambiguidade 
                    })
                    logger.info(f"(MappingAgent Cliente Etapa 1): AMBIGUIDADE REALMENTE GERADA por CNPJ/CPF DUPLICADO '{cnpj_cpf_extraido_norm}'. Retornando para Orchestrator.")
                    # IMPORTANTE: Retorna aqui para que o Orchestrator possa lidar com a ambiguidade
                    return 

        # --- 2. Tentativa por Nome Normalizado (str.contains) ---
        # Só executa se não foi encontrado por CNPJ/CPF e não há ambiguidade pendente de CNPJ/CPF
        if not cliente_encontrado and not cliente_ambiguo and nome_cliente_extraido and self._is_valid_df(self.df_precofixo, 'Nome_Cliente_NORMALIZADO'):
            nome_norm_input_contains = normalize_string(nome_cliente_extraido, remove_hyphens=True)
            if nome_norm_input_contains:
                logger.debug(f"(MappingAgent Cliente): Tentando NOME (contains) para: '{nome_norm_input_contains}'")
                try:
                    escaped_name = re.escape(nome_norm_input_contains)
                    cliente_info_nome_contains = self.df_precofixo[
                        self.df_precofixo['Nome_Cliente_NORMALIZADO'].str.contains(escaped_name, na=False, regex=True)
                    ]
                except Exception as e:
                    logger.error(f"Erro NOME (contains): {e}")
                    cliente_info_nome_contains = pd.DataFrame()

                if not cliente_info_nome_contains.empty:
                    if len(cliente_info_nome_contains) == 1:
                        cliente_row_data = cliente_info_nome_contains.iloc[0]
                        codigo_cliente_planilha = str(cliente_row_data.get("Cliente")) if pd.notna(cliente_row_data.get("Cliente")) else None
                        cnpj_planilha_match = cliente_row_data.get("CNPJ_CPF_NORMALIZADO")
                        nome_cliente_planilha = cliente_row_data.get("Nome Cliente")
                        
                        mapped_data["Código do cliente"] = codigo_cliente_planilha
                        if not cnpj_cpf_extraido and cnpj_planilha_match: # Só preenche CNPJ se não veio do LLM
                            mapped_data["CNPJ/CPF"] = cnpj_planilha_match
                        mapped_data["Nome do cliente"] = nome_cliente_planilha
                        mapped_data["Cliente"] = nome_cliente_planilha
                        cliente_encontrado = True
                        logger.info(f"(MappingAgent Cliente): NOME (contains - 1 match): '{nome_cliente_extraido}' -> '{nome_cliente_planilha}'.")
                    else: # Ambiguidade por nome (contains)
                        cliente_ambiguo = True
                        nomes_planilha = cliente_info_nome_contains["Nome Cliente"].tolist()
                        codigos_planilha = cliente_info_nome_contains["Cliente"].astype(str).tolist()
                        cnpjs_planilha = cliente_info_nome_contains["CNPJ_CPF_NORMALIZADO"].tolist()
                        
                        opcoes_ambiguidade = []
                        for n, c, j in zip(nomes_planilha, codigos_planilha, cnpjs_planilha):
                             opcoes_ambiguidade.append({
                                 "nome": n, 
                                 "codigo": c, 
                                 "cnpj_cpf": j if pd.notna(j) else None
                            })
                        
                        mensagem_ambiguidade_partes = [
                            f"Múltiplos clientes contêm '{nome_cliente_extraido}'. Por favor, escolha o correto (informe o número da opção ou o Código do Cliente):"
                        ]
                        for i, opt in enumerate(opcoes_ambiguidade):
                            mensagem_ambiguidade_partes.append(
                                f"{i+1}. {opt['nome']} (Cód: {opt['codigo']}, CNPJ/CPF: {opt.get('cnpj_cpf', 'N/A')})"
                            )
                        
                        self._add_issue(issues, "ambiguidades", {
                            "campo": "Código do cliente", # O campo que será efetivamente preenchido pela escolha
                            "original_field_name": "Cliente", # Usado pelo Orchestrator
                            "valor_original": nome_cliente_extraido,
                            "mensagem": "\n".join(mensagem_ambiguidade_partes),
                            "opcoes": opcoes_ambiguidade 
                        })
                        logger.info(f"(MappingAgent Cliente): AMBIGUIDADE NOME (contains) para '{nome_norm_input_contains}': {len(cliente_info_nome_contains)} matches.")

        # --- 3. Fallback para Fuzzy Matching ---
        if not cliente_encontrado and not cliente_ambiguo and nome_cliente_extraido and self._is_valid_df(self.df_precofixo, 'Nome_Cliente_NORMALIZADO'):
            nome_norm_input_fuzzy = normalize_string(nome_cliente_extraido, remove_hyphens=True)
            
            if nome_norm_input_fuzzy:
                logger.debug(f"(MappingAgent Cliente FUZZY): Tentando FUZZY MATCH para: '{nome_norm_input_fuzzy}' (Thresh: {FUZZY_MATCH_THRESHOLD}%, Scorer: {FUZZY_SCORER.__name__}, TieBreakThresh: {TIE_BREAK_SCORE_THRESHOLD}%)")
                
                potential_fuzzy_matches_raw = []
                valid_clientes_df = self.df_precofixo[self.df_precofixo['Nome_Cliente_NORMALIZADO'].notna() & (self.df_precofixo['Nome_Cliente_NORMALIZADO'] != '')].copy()
                input_first_n_words_str = " ".join(nome_norm_input_fuzzy.split()[:NUM_WORDS_FOR_TIE_BREAK])

                for _, row in valid_clientes_df.iterrows():
                    nome_planilha_norm = row['Nome_Cliente_NORMALIZADO']
                    if not nome_planilha_norm: continue

                    score = FUZZY_SCORER(nome_norm_input_fuzzy, nome_planilha_norm)
                    
                    if score >= FUZZY_MATCH_THRESHOLD:
                        planilha_first_n_words_str = " ".join(nome_planilha_norm.split()[:NUM_WORDS_FOR_TIE_BREAK])
                        tie_break_score = fuzz.ratio(input_first_n_words_str, planilha_first_n_words_str)

                        potential_fuzzy_matches_raw.append({
                            "score": score,
                            "tie_break_score": tie_break_score,
                            "row_data": row,
                            "nome_original_planilha": row.get("Nome Cliente"),
                            "codigo_cliente_planilha": str(row.get("Cliente")) if pd.notna(row.get("Cliente")) else None,
                            "cnpj_planilha": row.get("CNPJ_CPF_NORMALIZADO")
                        })
                
                potential_fuzzy_matches_raw.sort(key=lambda x: (x["score"], x["tie_break_score"]), reverse=True)
                
                final_qualified_matches = []
                if potential_fuzzy_matches_raw:
                    for match_candidate in potential_fuzzy_matches_raw:
                        if match_candidate["tie_break_score"] >= TIE_BREAK_SCORE_THRESHOLD:
                            final_qualified_matches.append(match_candidate)
                
                logger.debug(f"(MappingAgent Cliente FUZZY): Raw matches (score >= {FUZZY_MATCH_THRESHOLD}%): {len(potential_fuzzy_matches_raw)}. Final qualified (tie_break_score >= {TIE_BREAK_SCORE_THRESHOLD}%): {len(final_qualified_matches)}")

                if final_qualified_matches:
                    if len(final_qualified_matches) == 1:
                        best_match = final_qualified_matches[0]
                        cliente_row_data = best_match["row_data"]
                        codigo_cliente_planilha = best_match["codigo_cliente_planilha"]
                        cnpj_planilha_match = best_match["cnpj_planilha"]
                        nome_cliente_planilha = best_match["nome_original_planilha"]
                        
                        mapped_data["Código do cliente"] = codigo_cliente_planilha
                        if not cnpj_cpf_extraido and cnpj_planilha_match:
                            mapped_data["CNPJ/CPF"] = cnpj_planilha_match
                        mapped_data["Nome do cliente"] = nome_cliente_planilha
                        mapped_data["Cliente"] = nome_cliente_planilha
                        cliente_encontrado = True
                        logger.info(f"(MappingAgent Cliente FUZZY): FUZZY MATCH ÚNICO QUALIFICADO (Score: {best_match['score']}%, TieBreak: {best_match['tie_break_score']}%): '{nome_cliente_extraido}' -> '{nome_cliente_planilha}'.")
                    
                    else: # Múltiplos fuzzy matches QUALIFICADOS -> Ambiguidade
                        cliente_ambiguo = True
                        opcoes_ambiguidade = []
                        MAX_AMBIGUITY_OPTIONS = 5 # Definir no topo da classe se usado em mais lugares
                        for match in final_qualified_matches[:MAX_AMBIGUITY_OPTIONS]:
                            opcoes_ambiguidade.append({
                                "nome": match["nome_original_planilha"],
                                "codigo": match["codigo_cliente_planilha"],
                                "cnpj_cpf": match["cnpj_planilha"] if pd.notna(match["cnpj_planilha"]) else None,
                                "similaridade": match["score"],
                                "similaridade_inicio": match["tie_break_score"]
                            })
                        
                        ambiguity_message_parts = [
                            f"Encontrei múltiplos clientes com nomes similares a '{nome_cliente_extraido}' (correspondência aproximada e início similar). Por favor, escolha o correto (informe o número da opção ou o Código do Cliente):"
                        ]
                        for i, opt in enumerate(opcoes_ambiguidade):
                            ambiguity_message_parts.append(
                                f"{i+1}. {opt['nome']} (Cód: {opt['codigo']}, CNPJ: {opt.get('cnpj_cpf', 'N/A')}, Sim: {opt['similaridade']}% / Início: {opt['similaridade_inicio']}%)"
                            )
                        if len(final_qualified_matches) > MAX_AMBIGUITY_OPTIONS:
                            ambiguity_message_parts.append(f"... e mais {len(final_qualified_matches) - MAX_AMBIGUITY_OPTIONS} outros.")
                        
                        self._add_issue(issues, "ambiguidades", {
                            "campo": "Código do cliente", # O campo que será efetivamente preenchido
                            "original_field_name": "Cliente", # Usado pelo Orchestrator
                            "valor_original": nome_cliente_extraido,
                            "mensagem": "\n".join(ambiguity_message_parts),
                            "opcoes": opcoes_ambiguidade
                        })
                        logger.info(f"(MappingAgent Cliente FUZZY): AMBIGUIDADE FUZZY MATCH QUALIFICADO para '{nome_norm_input_fuzzy}'.")
                else: # Nenhum fuzzy match qualificado
                    logger.debug(f"(MappingAgent Cliente FUZZY): Nenhum fuzzy match QUALIFICADO encontrado para '{nome_norm_input_fuzzy}'.")
        
        # --- Fallback Final / Default ---
        if not cliente_encontrado:

            if cliente_ambiguo:
                mapped_data["Nome do cliente"] = nome_cliente_extraido # Mantém o que foi extraído pelo LLM
                mapped_data["Cliente"] = nome_cliente_extraido      # Mantém o que foi extraído pelo LLM
                mapped_data["CNPJ/CPF"] = cnpj_cpf_extraido         # Mantém o que foi extraído pelo LLM
                mapped_data["Código do cliente"] = None # Força None pois é ambíguo
                logger.debug(f"(MappingAgent Cliente): AMBIGUIDADE DETECTADA. Código do cliente indefinido. Nome='{nome_cliente_extraido}', CNPJ='{cnpj_cpf_extraido}'.")
            else:
                # Nenhuma ambiguidade, nenhum cliente encontrado. Adiciona aviso.
                # Mantém os valores extraídos pelo LLM em mapped_data.
                mapped_data["Nome do cliente"] = nome_cliente_extraido
                mapped_data["Cliente"] = nome_cliente_extraido
                mapped_data["CNPJ/CPF"] = cnpj_cpf_extraido
                mapped_data["Código do cliente"] = codigo_cliente_extraido # Mantém o que o LLM extraiu

                if (nome_cliente_extraido or cnpj_cpf_extraido or codigo_cliente_extraido):
                    identificadores_fornecidos = []
                    if nome_cliente_extraido: identificadores_fornecidos.append(f"Nome '{nome_cliente_extraido}'")
                    if cnpj_cpf_extraido: identificadores_fornecidos.append(f"CNPJ/CPF '{cnpj_cpf_extraido}'")
                    if codigo_cliente_extraido: identificadores_fornecidos.append(f"Código '{codigo_cliente_extraido}'")
                    
                    identificador_str = " e ".join(identificadores_fornecidos) if identificadores_fornecidos else "informado"
                    msg = f"Cliente com dados ({identificador_str}) não encontrado (pode ser novo ou dados insuficientes/incorretos)."
                    
                    # Determina o campo principal para o aviso
                    campo_aviso = "Cliente" if nome_cliente_extraido else \
                                  "CNPJ/CPF" if cnpj_cpf_extraido else \
                                  "Código do cliente"
                    valor_original_aviso = nome_cliente_extraido if campo_aviso=="Cliente" else \
                                           cnpj_cpf_extraido if campo_aviso=="CNPJ/CPF" else \
                                           codigo_cliente_extraido
                                           
                    self._add_issue(issues, "avisos", {"campo": campo_aviso, "valor_original": valor_original_aviso, "mensagem": msg})
                    logger.debug(f"(MappingAgent Cliente): {msg}")

        # --- Consistência Final ---
        # Se Nome do cliente foi definido (por mapeamento ou mantido da extração) e Cliente não, copia.
        if mapped_data.get("Nome do cliente") is not None and mapped_data.get("Cliente") is None:
             mapped_data["Cliente"] = mapped_data.get("Nome do cliente")
        # Se Cliente foi definido e Nome do cliente não, copia.
        elif mapped_data.get("Cliente") is not None and mapped_data.get("Nome do cliente") is None:
             mapped_data["Nome do cliente"] = mapped_data.get("Cliente")

        if cliente_encontrado and (not mapped_data.get("Código do cliente") or not mapped_data.get("Nome do cliente")):
            logger.warning(f"(MappingAgent Cliente): Inconsistência! Cliente marcado como encontrado, mas Cód Planilha ou Nome Planilha ausentes em mapped_data. Mapped CNPJ: {mapped_data.get('CNPJ/CPF')}, Mapped Código: {mapped_data.get('Código do cliente')}, Mapped Nome: {mapped_data.get('Nome do cliente')}")


    def _map_material(self, mapped_data: dict, issues: dict):
        """Mapeia o código do material."""
        material_input = mapped_data.get("Código do Material")
        if not material_input: return # Nada a mapear

        input_norm_cod = normalize_string(str(material_input), remove_hyphens=True)
        if input_norm_cod in self.valid_material_codes: # valid_material_codes são strings
            logger.debug(f"(MappingAgent Material): Input '{material_input}' normalizado p/ código válido '{input_norm_cod}'.")
            # Atualiza o mapped_data com o código encontrado no set (que não tem hífens se foram removidos no pré-proc)
            # Pode ser redundante se input_norm_cod == str(material_input), mas garante consistência.
            mapped_data["Código do Material"] = input_norm_cod # Usa o código normalizado/validado
            return # Mapeamento não necessário (ou já feito)

        # Não é código, tentar mapear por nome (REMOVENDO HÍFENS)
        logger.debug(f"(MappingAgent Material): Input '{material_input}' não é código válido. Tentando mapear por nome (s/ hífen).")
        if not self._is_valid_df(self.df_material, 'Produto_NORMALIZADO'): return

        # Normaliza o NOME do material (input) removendo hífens
        material_nome_norm = normalize_string(material_input, remove_hyphens=True)
        if not material_nome_norm:
             self._add_issue(issues, "avisos", {
                 "campo": "Código do Material", "valor_original": material_input,
                 "mensagem": f"Nome do material '{material_input}' normalizou para vazio."
             })
             return

        # Tenta match exato normalizado (sem hífens) com coluna pré-processada (sem hífens)
        material_info = self.df_material[self.df_material['Produto_NORMALIZADO'] == material_nome_norm]

        # Se exato falhou, tenta 'contains' (sem hífens) na coluna pré-processada (sem hífens)
        if material_info.empty:
            try:
                escaped_material = re.escape(material_nome_norm)
                material_info = self.df_material[self.df_material['Produto_NORMALIZADO'].str.contains(escaped_material, na=False, regex=True)]
                if not material_info.empty:
                    logger.debug(f"(MappingAgent Material): Encontrado '{material_input}' via 'contains' (s/ hífen) no nome.")
            except Exception as e:
                logger.error(f"Erro na busca por material (contains): {e}")
                material_info = pd.DataFrame()
        # Processa resultados
        if not material_info.empty:
            if len(material_info) == 1:
                mapped_data["Código do Material"] = str(material_info.iloc[0]['Cód'])
                logger.debug(f"(MappingAgent Material): Nome '{material_input}' mapeado para Código {mapped_data['Código do Material']}")
            else: # Ambiguidade no Material
                # Ambiguidade
                produtos = material_info["Produto"].tolist()
                codigos = material_info["Cód"].astype(str).tolist()
                self._add_issue(issues, "ambiguidades", {
                    "campo": "Código do Material", "valor_original": material_input,
                    "mensagem": f"Encontrei múltiplos materiais que podem corresponder a '{material_input}':\n" + \
                                "\n".join([f"{i+1}. {p} (Código: {c})" for i, (p, c) in enumerate(zip(produtos, codigos))]) + \
                                "\nQual deles é o correto? (Responda com o número ou o código)", # Mensagem completa
                    # CORREÇÃO: Adiciona 'opcoes'
                    "opcoes": [{"produto": p, "codigo": c} for p, c in zip(produtos, codigos)]
                })
                logger.debug(f"(MappingAgent Material): Ambiguidade por nome: '{material_input}' - {len(material_info)} matches")
        else:
            # Não encontrado
            self._add_issue(issues, "avisos", {
                "campo": "Código do Material", "valor_original": material_input,
                "mensagem": f"Material '{material_input}' (código ou nome) não encontrado ou inválido."
            })
            logger.debug(f"(MappingAgent Material): Valor '{material_input}' não encontrado/mapeado.")

    def _map_condicao_pagamento(self, mapped_data: dict, issues: dict):
        condicao_input_original = mapped_data.get("Condição de Pagamento") # Pode ter hífen
        if not condicao_input_original: return

        # 1. Verifica se já é um código válido (normalizado s/ hífen)
        input_norm_cod = normalize_string(str(condicao_input_original), remove_hyphens=True)
        if input_norm_cod in self.valid_condicao_codes:
            logger.debug(f"(MappingAgent CondPagto): Input '{condicao_input_original}' normalizado p/ código válido '{input_norm_cod}'.")
            mapped_data["Condição de Pagamento"] = input_norm_cod # Usa código validado
            return

        logger.debug(f"(MappingAgent CondPagto): Input '{condicao_input_original}' não é código. Tentando mapear (s/ hífen)...")
        # Normaliza o input REMOVENDO HÍFENS para comparar com termos
        condicao_norm = normalize_string(condicao_input_original, remove_hyphens=True)
        condicao_encontrada_direto = False

        # 2. Tenta mapeamento direto por termo normalizado (s/ hífen)
        # (self.condicao_map_norm_to_code já foi populado com chaves sem hífens)
        if condicao_norm and condicao_norm in self.condicao_map_norm_to_code:
            mapped_code = self.condicao_map_norm_to_code[condicao_norm]
            mapped_data["Condição de Pagamento"] = mapped_code # Atualiza com o código mapeado
            condicao_encontrada_direto = True
            logger.debug(f"(MappingAgent CondPagto): Termo '{condicao_input_original}' (norm s/hífen: '{condicao_norm}') mapeado DIRETAMENTE para '{mapped_code}'")

        # 3. Se o mapeamento direto falhou, TENTA DIVIDIR (já usa normalização s/ hífen internamente)
        split_successful = False
        if not condicao_encontrada_direto:
            logger.debug(f"(MappingAgent CondPagto): Mapeamento direto para '{condicao_input_original}' falhou. Tentando SPLIT.")
            # _attempt_split_and_remap_payment foi atualizado para usar remove_hyphens=True
            split_successful = self._attempt_split_and_remap_payment(
                str(condicao_input_original), mapped_data, issues
            )
            if split_successful:
                # Se o split foi bem-sucedido, mapped_data["Condição de Pagamento"] e possivelmente
                # mapped_data["Forma de Pagamento"] já foram atualizados com os códigos corretos.
                # O aviso de "não encontrado" para o input original será removido pelo split.
                logger.info(f"(MappingAgent CondPagto): SPLIT bem-sucedido para '{condicao_input_original}'. Dados atualizados em mapped_data.")
                return # Sai, pois o split resolveu.
            else:
                logger.debug(f"(MappingAgent CondPagto): SPLIT falhou para '{condicao_input_original}'.")

        # 4. Adiciona aviso se NADA funcionou (nem direto, nem split bem-sucedido que resultou em código)
        # Verifica se o valor em mapped_data AINDA é o original (ou seja, não foi alterado para um código)
        if not condicao_encontrada_direto and not split_successful:
            # Se chegou aqui, nem o mapeamento direto funcionou, nem o split conseguiu achar um código para Condição.
            # O valor em mapped_data["Condição de Pagamento"] ainda é o `condicao_input_original`.
            self._add_issue(issues, "avisos", {
                "campo": "Condição de Pagamento", "valor_original": condicao_input_original,
                "mensagem": f"Condição de Pagamento '{condicao_input_original}' não encontrada, inválida ou não pôde ser dividida em partes reconhecidas."
            })
            logger.debug(f"(MappingAgent CondPagto): Valor '{condicao_input_original}' não mapeado diretamente nem por split para um código válido.")


    def _map_forma_pagamento(self, mapped_data: dict, issues: dict):
        forma_input_original = mapped_data.get("Forma de Pagamento")
        if not forma_input_original: return

        if str(forma_input_original) in self.valid_forma_codes:
            logger.debug(f"(MappingAgent FormaPagto): Input '{forma_input_original}' já é um código válido.")
            return

        logger.debug(f"(MappingAgent FormaPagto): Input '{forma_input_original}' não é código. Tentando mapear...")
        forma_norm = normalize_string(forma_input_original, remove_hyphens=True)
        forma_encontrada = False

        # 1. Tenta mapeamento por termo direto (Significado Normalizado ou MP Normalizado)
        if forma_norm and forma_norm in self.forma_map_norm_to_code:
            mapped_code = self.forma_map_norm_to_code[forma_norm]
            mapped_data["Forma de Pagamento"] = mapped_code
            forma_encontrada = True
            logger.debug(f"(MappingAgent FormaPagto): Termo DIRETO '{forma_input_original}' (norm: '{forma_norm}') mapeado para '{mapped_code}'")

        # 2. Se não encontrado por termo direto, tenta por Palavra-Chave
        if not forma_encontrada and forma_norm and forma_norm in self.forma_keyword_map_norm_to_codes:
            possible_codes = self.forma_keyword_map_norm_to_codes[forma_norm]
            if len(possible_codes) == 1:
                mapped_code = possible_codes[0]
                mapped_data["Forma de Pagamento"] = mapped_code
                forma_encontrada = True
                logger.debug(f"(MappingAgent FormaPagto): Keyword '{forma_input_original}' (norm: '{forma_norm}') mapeada para '{mapped_code}'")
            elif len(possible_codes) > 1:
                # AMBIGUIDADE através de Keyword
                # Recupera os significados originais para as opções
                opcoes_ambiguidade = []
                for code_mp in possible_codes:
                    # Encontra a linha no df_forma original para pegar o significado
                    row_df = self.df_forma[self.df_forma['MP'] == code_mp]
                    if not row_df.empty:
                        significado_original = row_df.iloc[0]['Significado']
                        opcoes_ambiguidade.append({"descricao": significado_original, "codigo": code_mp})
                    else: # Fallback se não achar o significado
                        opcoes_ambiguidade.append({"descricao": f"Código {code_mp}", "codigo": code_mp})

                self._add_issue(issues, "ambiguidades", {
                    "campo": "Forma de Pagamento", "valor_original": forma_input_original,
                    "mensagem": f"A forma de pagamento '{forma_input_original}' pode se referir a mais de uma opção. Qual delas você quis dizer?\n" + \
                                "\n".join([f"{i+1}. {opt['descricao']} (Código: {opt['codigo']})" for i, opt in enumerate(opcoes_ambiguidade)]) + \
                                "\n(Responda com o número ou o código)",
                    "opcoes": opcoes_ambiguidade
                })
                logger.debug(f"(MappingAgent FormaPagto): Keyword ambígua '{forma_norm}' para '{forma_input_original}' - Opções: {possible_codes}")
                # Não define forma_encontrada = True, pois a ambiguidade precisa ser resolvida.
                # O valor original permanece em mapped_data["Forma de Pagamento"]
                return # Retorna para que o Orchestrator lide com a ambiguidade

        # 3. Se ainda não encontrado, tenta SPLIT (como antes)
        split_successful = False
        if not forma_encontrada:
            logger.debug(f"(MappingAgent FormaPagto): Mapeamento direto/keyword para '{forma_input_original}' falhou. Tentando SPLIT.")
            split_successful = self._attempt_split_and_remap_payment(
                str(forma_input_original), mapped_data, issues
            )
            if split_successful:
                logger.info(f"(MappingAgent FormaPagto): SPLIT bem-sucedido para '{forma_input_original}'. Dados atualizados.")
                # Verifica se o valor em mapped_data["Forma de Pagamento"] agora é um código válido
                if str(mapped_data.get("Forma de Pagamento")) in self.valid_forma_codes:
                    forma_encontrada = True # O split encontrou um código válido
                # Não retorna aqui ainda, pois pode ter sido split de Condição que continha Forma.
                # A lógica de aviso no final cuidará disso.

        # 4. Fallback para 'contains' (OPCIONAL, pode ser removido se as keywords forem boas)
        # Só executa se nada funcionou ATÉ AGORA e se o valor NÃO foi alterado por um split para um código válido.
        if not forma_encontrada and self._is_valid_df(self.df_forma, 'Significado_NORMALIZADO') and \
        (mapped_data.get("Forma de Pagamento") == forma_input_original or not str(mapped_data.get("Forma de Pagamento")) in self.valid_forma_codes) : # Garante que não foi mapeado por split

            logger.debug(f"(MappingAgent FormaPagto): Mapeamento direto/keyword/split falhou. Tentando 'contains' no significado para '{forma_norm}'.")
            try:
                escaped_forma_norm = re.escape(forma_norm)
                matches = self.df_forma[self.df_forma['Significado_NORMALIZADO'].str.contains(escaped_forma_norm, na=False, regex=True)]
            except Exception as e:
                logger.error(f"Erro na busca por forma de pagamento (contains): {e}")
                matches = pd.DataFrame()

            if not matches.empty:
                if len(matches) == 1:
                    mapped_code = str(matches.iloc[0]['MP'])
                    mapped_data["Forma de Pagamento"] = mapped_code
                    forma_encontrada = True
                    logger.debug(f"(MappingAgent FormaPagto): Significado '{forma_input_original}' (norm: '{forma_norm}') mapeado para '{mapped_code}' (via CONTAINS)")
                else: # Ambiguidade via 'contains'
                    if mapped_data.get("Forma de Pagamento") == forma_input_original: # Só se ainda não resolvido
                        descricoes = matches['Significado'].tolist()
                        codigos_mp = matches['MP'].astype(str).tolist()
                        self._add_issue(issues, "ambiguidades", {
                            "campo": "Forma de Pagamento", "valor_original": forma_input_original,
                            "mensagem": f"Para a Forma de Pagamento '{forma_input_original}', qual destas opções você se refere (encontradas por similaridade)?\n" + \
                                        "\n".join([f"{i+1}. {d} (Código: {c})" for i, (d, c) in enumerate(zip(descricoes, codigos_mp))]) + \
                                        "\n(Responda com o número ou o código)",
                            "opcoes": [{"descricao": d, "codigo": c} for d, c in zip(descricoes, codigos_mp)]
                        })
                        logger.debug(f"(MappingAgent FormaPagto): Ambiguidade por significado (contains): '{forma_input_original}' - {len(matches)} matches")
                        return # Retorna para Orchestrator lidar com a ambiguidade

        # 5. Adiciona aviso se NADA funcionou e o valor AINDA é o original
        # (nem direto, nem keyword, nem split que resultou em código, nem contains que resultou em código único)
        # E não há uma ambiguidade pendente para este campo
        if not forma_encontrada and mapped_data.get("Forma de Pagamento") == forma_input_original and \
        not any(iss.get("campo") == "Forma de Pagamento" and iss.get("tipo") == "ambiguidades" for iss in issues.get("ambiguidades", [])):
            self._add_issue(issues, "avisos", {
                "campo": "Forma de Pagamento", "valor_original": forma_input_original,
                "mensagem": f"Forma de Pagamento '{forma_input_original}' não encontrada ou inválida."
            })
            logger.debug(f"(MappingAgent FormaPagto): Valor '{forma_input_original}' não mapeado.")

    def _ensure_default_keys(self, mapped_data: dict):
        """Garante que todas as chaves esperadas existam no dicionário final."""
        default_keys = [
            "Cliente", "CNPJ/CPF", "Planta", "Condição de Pagamento",
            "Forma de Pagamento", "Código do Material", "Quantidade Total",
            "Cadência", "Vendedor", "Cidade", "Data de Negociação",
            "Incoterms", "Preço Frete", "Valor", "Nome do cliente",
            "Código do cliente", "Email do vendedor", "Campanha"
        ]
        for key in default_keys:
            mapped_data.setdefault(key, None)


    def map(self, extracted_data: dict, original_input_text: str) -> tuple[dict, dict]:
        """
        Realiza o mapeamento ('de-para') dos dados extraídos.

        Args:
            extracted_data (dict): Dicionário com dados extraídos pelo ExtractionAgent.

        Returns:
            tuple[dict, dict]: Uma tupla contendo:
                - mapped_data (dict): Cópia dos dados extraídos com campos mapeados/adicionados.
                - mapping_issues (dict): Dicionário com listas de 'avisos', 'erros', 'ambiguidades'.
        """
        if not self.data_loaded_successfully:
            return extracted_data, {"avisos": [], "erros": ["DataFrames de mapeamento não carregados."], "ambiguidades": []}

        mapped_data = extracted_data.copy()
        mapping_issues = {"avisos": [], "erros": [], "ambiguidades": []}

        # Executa mapeamento para cada campo relevante
        self._map_planta(mapped_data, mapping_issues, original_input_text)
        self._map_cliente(mapped_data, mapping_issues)
        self._map_material(mapped_data, mapping_issues)
        self._map_condicao_pagamento(mapped_data, mapping_issues)
        self._map_forma_pagamento(mapped_data, mapping_issues)

        # Garante chaves padrão e email do vendedor (que não tem mapeamento)
        mapped_data.setdefault("Email do vendedor", None)
        self._ensure_default_keys(mapped_data)

        logger.debug("(MappingAgent): Dados mapeados finais:")
        logger.debug(json.dumps(mapped_data, indent=2, ensure_ascii=False))
        logger.debug("(MappingAgent): Issues de mapeamento finais:")
        logger.debug(json.dumps(mapping_issues, indent=2, ensure_ascii=False))

        return mapped_data, mapping_issues