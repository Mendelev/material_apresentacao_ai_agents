# src/utils/formatting.py
import re
from datetime import datetime
from unidecode import unidecode
import logging

logger = logging.getLogger(__name__)

mes_map = {
    'jan': '01', 'janeiro': '01', 'fev': '02', 'fevereiro': '02',
    'mar': '03', 'marco': '03', 'março': '03', 'abr': '04', 'abril': '04',
    'mai': '05', 'maio': '05', 'jun': '06', 'junho': '06',
    'jul': '07', 'julho': '07', 'ago': '08', 'agosto': '08',
    'set': '09', 'setembro': '09', 'out': '10', 'outubro': '10',
    'nov': '11', 'novembro': '11', 'dez': '12', 'dezembro': '12'
}

def _clean_valor(valor_str: str) -> str | None:
    if not valor_str: return None
    # Remove R$, espaços extras, converte vírgula para ponto como decimal
    # e remove pontos de milhar.
    cleaned = re.sub(r'[R$\s]', '', valor_str).strip()
    
    # Se tem '.' e ',' , trata como BR (1.234,56 -> 1234.56) ou US (1,234.56 -> 1234.56)
    if '.' in cleaned and ',' in cleaned:
        if cleaned.rfind('.') > cleaned.rfind(','): # Formato US: 1,234.56
            cleaned = cleaned.replace(',', '')
        else: # Formato BR: 1.234,56
            cleaned = cleaned.replace('.', '').replace(',', '.')
    elif ',' in cleaned: # Só vírgula: 1234,56
        cleaned = cleaned.replace(',', '.')
    # Se tem múltiplos pontos e o último não é decimal, remove os anteriores (ex: 1.234.567)
    # Esta parte pode ser simplificada se o LLM já envia algo mais limpo.
    # Por ora, uma limpeza básica:
    if cleaned.count('.') > 1:
        parts = cleaned.split('.')
        if len(parts[-1]) < 3: # Heurística: se a última parte tem menos de 3 dígitos, é decimal
            cleaned = "".join(parts[:-1]) + "." + parts[-1]
        else: # Provavelmente separador de milhar
            cleaned = "".join(parts)
            
    try:
        float(cleaned) # Valida se é um float válido
        return cleaned
    except ValueError:
        logger.warning(f"Valor de cadência '{valor_str}' resultou em '{cleaned}' que não é um float válido.")
        return None

def _determine_year(ano_str: str | None, mes_num_int: int, current_year_ref: int, previous_month_num_ref: int) -> tuple[int, int, int]:
    """
    Determina o ano a ser usado para a cadência.
    Retorna (year_to_use, new_current_year, new_previous_month_num)
    """
    year_to_use = current_year_ref
    new_current_year = current_year_ref
    new_previous_month_num = previous_month_num_ref

    if ano_str:
        try:
            if len(ano_str) == 2: ano_int = int(f"20{ano_str}")
            elif len(ano_str) == 4: ano_int = int(ano_str)
            else: raise ValueError("Formato de ano inválido")

            if 2000 <= ano_int < 2100:
                year_to_use = ano_int
                # Se o ano explícito é diferente do current_year, reseta a contagem de mês
                if ano_int != new_current_year:
                    new_previous_month_num = 0 
                new_current_year = ano_int # Atualiza o ano base para próximas inferências SEM ano explícito
            else: # Ano explícito inválido, tenta inferir
                if mes_num_int < new_previous_month_num and new_previous_month_num != 0:
                    year_to_use = new_current_year + 1
                    new_current_year +=1 # O ano base para inferência avança
                    new_previous_month_num = 0 # Resetou o ano, reseta o mês anterior
        except ValueError: # Ano não é número ou formato errado, tenta inferir
            if mes_num_int < new_previous_month_num and new_previous_month_num != 0:
                year_to_use = new_current_year + 1
                new_current_year += 1
                new_previous_month_num = 0
    else: # Sem ano explícito, infere
        if mes_num_int < new_previous_month_num and new_previous_month_num != 0:
            year_to_use = new_current_year + 1
            new_current_year +=1
            new_previous_month_num = 0
            
    new_previous_month_num = mes_num_int # Atualiza o último mês processado com o mês atual
    return year_to_use, new_current_year, new_previous_month_num

def _parse_multi_item_line(line_text: str, ano_base: int, current_year_ref: int, previous_month_num_ref: int) -> tuple[list[dict], int, int]:
    """
    Tenta parsear uma linha que pode conter múltiplos pares de (valor, mês) ou (mês, valor).
    Ex: "40 fev 20 mar 58 abr"
    Retorna (lista_de_itens_cadencia, novo_ano_inferido, novo_mes_anterior)
    """
    items_found = []
    new_current_year = current_year_ref
    new_previous_month = previous_month_num_ref
    
    regex_vm_multi = re.compile(r"(\d[\d.,]*)\s*(?:t(?:ons?)?|toneladas)?\s*([a-zA-Zç]+)", re.IGNORECASE)
    
    last_match_end = 0
    matched_vm = False
    temp_items_vm = []

    for match in regex_vm_multi.finditer(line_text):
        matched_vm = True
        valor_str, mes_str = match.groups()
        
        mes_norm = unidecode(mes_str.strip().lower())
        mes_num_str = mes_map.get(mes_norm)
        valor_limpo = _clean_valor(valor_str)

        if not mes_num_str or not valor_limpo:
            logger.warning(f"[_parse_multi_item_line VM] Mês ou valor inválido: '{mes_str}', '{valor_str}'. Pulando item.")
            continue

        mes_num_int = int(mes_num_str)
        year_for_this_item, updated_year_ref, updated_prev_month = _determine_year(
            None, mes_num_int, new_current_year, new_previous_month
        )
        new_current_year = updated_year_ref
        new_previous_month = updated_prev_month
        
        temp_items_vm.append({
            "mes": mes_num_int,
            "ano": year_for_this_item,
            "texto": f"{mes_num_str}.{year_for_this_item}:{valor_limpo} ton"
        })
        last_match_end = match.end()

    if matched_vm and last_match_end > len(line_text) * 0.7: 
        logger.debug(f"[_parse_multi_item_line] Padrão 'Valor Mês (multi)' aplicado à linha. Itens: {len(temp_items_vm)}")
        return temp_items_vm, new_current_year, new_previous_month

    logger.debug(f"[_parse_multi_item_line] Nenhum padrão multi-item forte encontrado para: '{line_text}'")
    return [], current_year_ref, previous_month_num_ref


def format_cadencia(cadencia_str: str | None, qtd_total: str | None = None, data_negociacao: str | None = None) -> str | None:

    known_single_item_connectors = [
        " toneladas em ", " tonelada em ", " ton em ", " t em "," un em ",
        " toneladas para ", " tonelada para ", " ton para ", " t para ", " un para "
    ]

    raw_qtd_total_param = qtd_total # Preserva o parâmetro original se necessário para logs
    cleaned_qtd_total_for_extractors = _clean_valor(raw_qtd_total_param) if raw_qtd_total_param else None
    logger.debug(f"[format_cadencia] Qtd Total (parâmetro original): '{raw_qtd_total_param}', Limpo para extratores: '{cleaned_qtd_total_for_extractors}'")

    if not cadencia_str:
        return None
    
    cadencia_padronizada = cadencia_str.strip().replace(';', '\n')

    cadencia_padronizada = re.sub(r'\)\s+', ')\n', cadencia_padronizada)


    initial_lines = [line.strip() for line in cadencia_padronizada.split('\n') if line.strip()]

    processed_sub_lines = []

    item_cadencia_regex_text = (
        r"([a-zA-Zç]+|\d{1,2})"  # Mês (texto ou número)
        r"(?:\s*[/.\-]\s*(\d{2,4}))?"  # Ano opcional (ex: /25)
        r"\s+"  # Espaço obrigatório antes do valor
        r"(\d[\d.,]*)"  # Valor (número)
        r"(?:\s*(?:t|ton|tons?|toneladas))?"  # Unidade opcional (t, ton, etc.)
    )

    item_cadencia_regex_valor_com_fim = item_cadencia_regex_text + r"(?:\b|$)"
    
    _temp_pattern_mes_parenteses_valor = re.compile(
        r'([a-zA-Zç]+|\d{1,2})'                         # Mês (texto ou número)
        r'(?:\s*[/.\-]\s*(\d{2,4}))?'                  # Ano opcional (ex: /25)
        r'\s*\('                                        # Abre parêntese
        r'([\d.,]+)'                                   # Valor (número)
        r'\s*(?:t|ton|tons?|toneladas)?'                # Unidade opcional (t, ton, etc.)
        r'\s*\)',                                       # Fecha parêntese
        re.IGNORECASE
    )


    for line in initial_lines:
        # Tenta dividir linhas como "maio (300 t), junho (300 t) e julho (300 t)"
        # Verifica se a linha parece conter múltiplos itens "mes (valor t)" separados por vírgula ou "e"
        # Heurística: presença de parênteses, "t)", e (vírgula ou " e ")
        if '(' in line and 't)' in line.lower() and (',' in line or ' e ' in line.lower()):
            logger.debug(f"[format_cadencia PreProc] Linha '{line}' candidata a split por vírgula/e para formato 'mes (valor t)'.")
            
            # Substitui " e " por "," para ter um delimitador único (vírgula)
            # Cuidado para não substituir "e" dentro de nomes de meses como "setembro"
            # Usar \s+e\s+ é mais seguro.
            temp_line = re.sub(r'\s+e\s+', ',', line, flags=re.IGNORECASE)
            
            parts = [p.strip() for p in temp_line.split(',') if p.strip()]
            
            all_parts_valid = True
            if not parts: # Se o split não resultou em nada
                all_parts_valid = False

            for part in parts:
                # Verifica se cada parte individual corresponde ao padrão "mes (valor t)"
                if not _temp_pattern_mes_parenteses_valor.fullmatch(part): # Usa fullmatch para a parte individual
                    all_parts_valid = False
                    logger.debug(f"[format_cadencia PreProc] Parte '{part}' da linha '{line}' não validou como 'mes (valor t)' isolado. Não aplicando split especial.")
                    break
            
            if all_parts_valid:
                logger.info(f"[format_cadencia PreProc] Linha '{line}' dividida em sub-itens: {parts}")
                processed_sub_lines.extend(parts)
                continue # Pula para a próxima linha original
        
        # Lógica de split por ". " existente (se necessário, ajuste a condição)
        if re.search(r'\s+E\s+', line, flags=re.IGNORECASE) and not _temp_pattern_mes_parenteses_valor.search(line):
            logger.debug(f"[format_cadencia PreProc] Linha '{line}' candidata a split por ' E '.")
            
            # Tenta dividir por " E " (case insensitive)
            # O re.split com grupo de captura ( ) mantém o delimitador, mas não queremos o "E"
            # Então usamos um split mais simples e validamos as partes.
            potential_parts = re.split(r'\s+E\s+', line, flags=re.IGNORECASE)
            parts = [p.strip() for p in potential_parts if p.strip()]

            all_parts_valid_for_E_split = True if parts else False
            if len(parts) <= 1: # Se não dividiu em pelo menos 2 partes, não é o caso "item E item"
                all_parts_valid_for_E_split = False
                
            for part_idx, part_str in enumerate(parts):
                # Valida cada parte com um regex que busca "MÊS/ANO VALOR" ou "MÊS VALOR"
                # Usar re.fullmatch para garantir que a parte inteira é um item de cadência
                if not re.fullmatch(item_cadencia_regex_valor_com_fim, part_str, flags=re.IGNORECASE):
                    all_parts_valid_for_E_split = False
                    logger.debug(f"[format_cadencia PreProc] Parte '{part_str}' (de split por 'E' da linha '{line}') não validou como item de cadência. Não aplicando split por 'E'.")
                    break
            
            if all_parts_valid_for_E_split:
                logger.info(f"[format_cadencia PreProc] Linha '{line}' dividida por ' E ' em sub-itens: {parts}")
                processed_sub_lines.extend(parts)
                continue # Pula para a próxima linha original

        # Se nenhum split especial foi aplicado, adiciona a linha como está
        processed_sub_lines.append(line)

    input_lines = [l for l in processed_sub_lines if l]
    if not input_lines: # Se o pré-processamento resultou em nenhuma linha válida
        logger.warning(f"[format_cadencia] Após pré-processamento, nenhuma linha de cadência válida encontrada para: '{cadencia_str}'")
        return None

    ano_negociacao_base = datetime.now().year
    logger.debug(f"[format_cadencia] Ano padrão inicializado como: {ano_negociacao_base}")
    if data_negociacao:
        try:
            partes_data = [p.strip() for p in data_negociacao.split('/') if p.strip()]
            if len(partes_data) == 3:
                ano_parte = partes_data[2]
                ano_neg = None
                if len(ano_parte) == 4: ano_neg = int(ano_parte)
                elif len(ano_parte) == 2: ano_neg = int(f"20{ano_parte}")
                if ano_neg is not None and 2000 <= ano_neg < 2100:
                    ano_negociacao_base = ano_neg
            elif len(partes_data) == 2: # Pode ser DD/MM ou MM/YY
                try:
                    p0_val = int(partes_data[0])
                    p1_val = int(partes_data[1])

                    # Heurística: se p0 é dia (1-31) e p1 é mês (1-12), é DD/MM
                    # Neste caso, não alteramos o ano_negociacao_base (ele permanece o ano atual)
                    if (1 <= p0_val <= 31) and (1 <= p1_val <= 12):
                        logger.debug(f"[format_cadencia] Data de negociação '{data_negociacao}' interpretada como DD/MM. Ano base ({ano_negociacao_base}) não será alterado por esta parte.")
                    else:
                        # Caso contrário, pode ser MM/YY ou algo inválido que tentaremos tratar como MM/YY
                        # Aqui, partes_data[1] é o candidato a ano.
                        ano_neg_str = partes_data[1] # Usamos o segundo elemento como potencial ano
                        ano_neg = None
                        if len(ano_neg_str) == 4: ano_neg = int(ano_neg_str)
                        elif len(ano_neg_str) == 2: ano_neg = int(f"20{ano_neg_str}")

                        if ano_neg is not None and 2000 <= ano_neg < 2100:
                            ano_negociacao_base = ano_neg
                            logger.debug(f"[format_cadencia] Data de negociação '{data_negociacao}' interpretada como MM/YY. Ano base definido para {ano_negociacao_base}.")
                        # else: não foi possível determinar um ano válido a partir de MM/YY, mantém o ano_negociacao_base atual.
                except ValueError:
                    # Se as partes não forem inteiros, não é um formato DD/MM ou MM/YY numérico simples.
                    logger.debug(f"[format_cadencia] Partes de '{data_negociacao}' não são puramente numéricas para DD/MM ou MM/YY. Ano base ({ano_negociacao_base}) não alterado.")
            elif len(partes_data) == 1 and partes_data[0].lower() not in mes_map and partes_data[0].count(' ') > 0 :
                 match_ano_texto = re.search(r'(\d{4})', partes_data[0]) # Tenta achar 4 digitos de ano
                 if match_ano_texto:
                     ano_neg = int(match_ano_texto.group(1))
                     if 2000 <= ano_neg < 2100:
                         ano_negociacao_base = ano_neg
        except (ValueError, IndexError, TypeError) as e:
             logger.warning(f"[format_cadencia] Erro ao processar data_negociacao '{data_negociacao}': {e}. Usando ano atual: {ano_negociacao_base}")
    logger.debug(f"[format_cadencia] Ano base da negociação definido como {ano_negociacao_base}")

    output_cadencia_items = []
    current_inferred_year = ano_negociacao_base
    last_processed_month_num = 0

    pattern1_regex = re.compile(r'([a-zA-Zç]+)\s*[/.\-]\s*(\d{2,4})\s*-\s*(\d[\d.,]*)\s*(?:t|ton|tons?|toneladas)\b', re.IGNORECASE)
    def extractor1(match): return match.group(1), match.group(2), match.group(3)

    pattern2_regex = re.compile(
        r"^\s*(\d[\d.,]*)"                                
        r"\s*(?:t|ton|tons?|toneladas)?\b"                
        r"\s+"                                            
        r"([a-zA-Zç]+|\d{1,2})"                           
        r"(?:\s*[/.\-]\s*(\d{2,4}))?",                    
        re.IGNORECASE
    )
    def extractor2(match):
        return match.group(2), match.group(3), match.group(1)


    pattern3_regex = re.compile(
        r'^\s*([a-zA-Zç]+|\d{1,2})'                         
        r'(?:\s*[/.\-]\s*(\d{2,4}))?'                  
        r'\s+'                                          
        r'(\d[\d.,]*)'                                 
        r'\s*(?:t|ton|tons?|toneladas)?\b',             
        re.IGNORECASE
    )
    def extractor3(match):
        return match.group(1), match.group(2), match.group(3)

    pattern_mes_ano_opcional_parenteses_valor_regex = _temp_pattern_mes_parenteses_valor

    def extractor_mes_ano_opcional_parenteses_valor(match):
        return match.group(1), match.group(2), match.group(3)

    pattern_mes_ano_colon_valor_regex = re.compile(
        r'^\s*'                                         
        r'([a-zA-Zç]+|\d{1,2})'                         
        r'\s*[/.\-]\s*'                                 
        r'(\d{2,4})'                                   
        r'\s*:\s*'                                      
        r'(\d[\d.,]*)'                                 
        r'\s*(?:t|ton|tons?|toneladas)?'                
        r'\s*$',                                        
        re.IGNORECASE
    )
    def extractor_mes_ano_colon_valor(match):
        return match.group(1), match.group(2), match.group(3)
    
    pattern_mes_de_ano_qtotal_regex = re.compile(
        r'^\s*([a-zA-Zç]+|\d{1,2})'      # Grupo 1: Mês
        r'\s+de\s+'                     # " de "
        r'(\d{2,4})\s*$',               # Grupo 2: Ano
        re.IGNORECASE
    )

    def extractor_mes_de_ano_qtotal(match):
        # cleaned_qtd_total_for_extractors é acessível aqui por closure
        if not cleaned_qtd_total_for_extractors:
            logger.warning(f"[ExtractorMesDeAnoQtotal] Quantidade total limpa não disponível (original: '{raw_qtd_total_param}').")
            return None
        # Retorna (mês, ano, valor_da_qtd_total_limpa)
        return match.group(1), match.group(2), cleaned_qtd_total_for_extractors

    pattern6_regex = re.compile(r'^\s*([a-zA-Zç]+|\d{1,2})\s*[/.\-]\s*(\d{2,4})\s*$', re.IGNORECASE)
    def extractor6(match):
        if not cleaned_qtd_total_for_extractors:
            logger.warning(f"[Extractor6] Quantidade total limpa não disponível (original: '{raw_qtd_total_param}').")
            return None
        # Retorna (mês, ano, valor_da_qtd_total_limpa)
        return match.group(1), match.group(2), cleaned_qtd_total_for_extractors
    
    pattern_valor_mes_ano_flex_regex = re.compile(
        r"^\s*"                                        
        r"(?P<valor>\d[\d.,]*)"                        
        r"\s*(?:t|ton|tons?|toneladas|kg|un)\b"        
        r"(?:\s+(?:em|para|no|na|para o|para a))?"    
        r"\s+"                                         
        r"(?P<mes>[a-zA-Zç]+|\d{1,2})"                 
        r"(?:\s+(?:de|do))?"                           
        r"\s+"                                         
        r"(?P<ano>\d{2,4})"                            
        r"\s*$",                                       
        re.IGNORECASE
    )

    def extractor_valor_mes_ano_flex(match):
        valor_str = match.group("valor")
        mes_input_original = match.group("mes")
        ano_str_capturado = match.group("ano")
        return mes_input_original, ano_str_capturado, valor_str
    

    pattern_mes_de_ano_valor_regex = re.compile(
        r"^\s*"                                        
        r"(?P<mes>[a-zA-Zç]+)"                         
        r"(?:\s+(?:de|do)\s+(?P<ano_de>\d{2,4}))?"     
        r"(?:\s+(?P<ano_direto>\d{2,4}))?"             
        r"(?:(?:\s*,\s*)|\s+)"                         
        r"(?P<valor>\d[\d.,]*)"                        
        r"\s*(?:t|ton|tons?|toneladas|kg|un)?\b"       
        r"\s*$",                                       
        re.IGNORECASE
    )

    def extractor_mes_de_ano_valor(match):
        mes_input = match.group("mes")
        ano_capturado = match.group("ano_de") if match.group("ano_de") else match.group("ano_direto")
        valor_str = match.group("valor")
        return mes_input, ano_capturado, valor_str
    
    pattern_valor_unidade_em_mes_ano_opcional_regex = re.compile(
        r"^\s*"                                       # Início da string com espaços opcionais
        r"(?P<valor>\d[\d.,]*)"                       # Grupo 'valor': números, pontos, vírgulas
        r"\s*(?:t|ton|tons?|tonelada|toneladas|kg|un)\b" # Unidade (t, ton, etc.) com word boundary
        r"\s+(?:em|para)\s+"                          # Obrigatório "em" ou "para" cercado por espaços
        r"(?P<mes>[a-zA-Zç]+|\d{1,2})"                # Grupo 'mes': nome do mês ou número
        r"(?:\s*[/.\-]?\s*(?P<ano>\d{2,4}))?"         # Grupo 'ano' opcional, precedido por / . - ou espaço opcional
        r"\s*$",                                      # Fim da string com espaços opcionais
        re.IGNORECASE
    )

    def extractor_valor_unidade_em_mes_ano_opcional(match):
        valor_str = match.group("valor")
        mes_input_original = match.group("mes")
        ano_str_capturado = match.group("ano") # Pode ser None
        return mes_input_original, ano_str_capturado, valor_str

    single_line_patterns = [
        ("Valor Unidade EM Mês [Ano Opcional]", pattern_valor_unidade_em_mes_ano_opcional_regex, extractor_valor_unidade_em_mes_ano_opcional),
        ("Mês/Ano - Valor T", pattern1_regex, extractor1),
        ("Mês de Ano, Valor T", pattern_mes_de_ano_valor_regex, extractor_mes_de_ano_valor),
        ("Valor T Mês[/Ano]", pattern2_regex, extractor2),
        ("Mês[/Ano] (Valor T)", pattern_mes_ano_opcional_parenteses_valor_regex, extractor_mes_ano_opcional_parenteses_valor),
        ("Mês/Ano: Valor T", pattern_mes_ano_colon_valor_regex, extractor_mes_ano_colon_valor),
        ("Mês[/Ano] Valor T", pattern3_regex, extractor3), 
        ("Mês de Ano (usa qtd_total)", pattern_mes_de_ano_qtotal_regex, extractor_mes_de_ano_qtotal),
        ("Mês/Ano (usa qtd_total)", pattern6_regex, extractor6),
        ("Valor Unidade Mês Ano (Flexível)", pattern_valor_mes_ano_flex_regex, extractor_valor_mes_ano_flex),
    ]
    
    for line_text in input_lines:
        logger.debug(f"[format_cadencia] Processando linha (pós-pré-processamento): '{line_text}'")

        if line_text.upper() in ["CADÊNCIA", "CADENCIA", "CADÊNCIA LRV", "CADÊNCIA PDL", "CADENCIA SRS"]:
            logger.info(f"[format_cadencia] Ignorando linha de cabeçalho: '{line_text}'")
            continue

        matched_line = False
        contains_explicit_year_pattern = bool(re.search(r'[/.\-]\s*\d{2,4}\b', line_text))
        
        # Modificada a condição para is_simple_multi_item_candidate
        contains_known_single_item_connector = any(connector in line_text.lower() for connector in known_single_item_connectors)

        is_simple_multi_item_candidate = (
            not contains_explicit_year_pattern and
            not contains_known_single_item_connector and # <-- NOVA CONDIÇÃO AQUI
            " de " not in line_text.lower() and
            ("," not in line_text or (line_text.lower().rfind(',') < line_text.lower().rfind("toneladas") if "toneladas" in line_text.lower() else True)) and
            ("," not in line_text or (line_text.lower().rfind(',') < line_text.lower().rfind("ton") if "ton " in line_text.lower() else True)) and
            line_text.count(" ") > 1 # Garante que há pelo menos duas "palavras" (ex: "valor mes")
        )

        if is_simple_multi_item_candidate:
            logger.debug(f"[format_cadencia] Linha '{line_text}' candidata para _parse_multi_item_line.")
            multi_items, updated_year, updated_month = _parse_multi_item_line(
                line_text, ano_negociacao_base, current_inferred_year, last_processed_month_num
            )
            if multi_items:
                output_cadencia_items.extend(multi_items)
                current_inferred_year = updated_year
                last_processed_month_num = updated_month
                logger.info(f"[format_cadencia] Linha '{line_text}' processada por _parse_multi_item_line. Itens: {len(multi_items)}")
                matched_line = True
        
        if not matched_line: 
            for pattern_name, regex, extractor_fn in single_line_patterns:
                # IMPORTANTE: Usar regex.fullmatch() aqui porque cada line_text agora deve ser um item de cadência completo.
                # Se usar search(), pode pegar partes de uma linha maior que não foi corretamente dividida.
                # No entanto, o log original mostra que `search` foi usado e funcionou para o PRIMEIRO item.
                # Se `line_text` agora é "maio (300 t)", `fullmatch` seria mais apropriado.
                # Vamos manter `search` por enquanto, pois é o que está no seu log, mas `fullmatch` é algo a se considerar
                # se `input_lines` contiver exatamente um item de cadência por string.
                match = regex.search(line_text) # Ou regex.fullmatch(line_text) se apropriado
                
                if match:
                    if pattern_name == "Mês/Ano (usa qtd_total)":
                        if line_text[:match.start()].strip():
                           logger.debug(f"[format_cadencia] Padrão '{pattern_name}' encontrou '{match.group(0)}' mas há prefixo não-espaço: '{line_text[:match.start()]}'. Pulando.")
                           continue 
                    
                    logger.debug(f"[format_cadencia] Linha '{line_text}' teve correspondência (search) com padrão: {pattern_name}, match: '{match.group(0)}'")
                    
                    extracted_info = extractor_fn(match)
                    if not extracted_info:
                        logger.debug(f"[DEBUG EXTRACTED_INFO] Padrão: {pattern_name}, Linha: '{line_text}', Match Group0: '{match.group(0)}', Extracted Info: {extracted_info}")
                        logger.debug(f"[format_cadencia] Extrator para '{pattern_name}' retornou None. Tentando próximo padrão.")
                        continue

                    mes_input_original, ano_str_capturado, valor_str = extracted_info
                    
                    mes_numero_canonico_str = None
                    if isinstance(mes_input_original, str) and mes_input_original.isdigit():
                        num_mes = int(mes_input_original)
                        if 1 <= num_mes <= 12:
                            mes_numero_canonico_str = str(num_mes).zfill(2)
                            logger.debug(f"[format_cadencia SL] Mês numérico '{mes_input_original}' normalizado para '{mes_numero_canonico_str}'.")
                        else:
                            logger.warning(f"[format_cadencia SL] Mês numérico '{mes_input_original}' fora do intervalo 1-12 na linha '{line_text}'. Pulando item.")
                            continue 
                    elif isinstance(mes_input_original, str): 
                        nome_mes_norm = unidecode(mes_input_original.strip().lower())
                        mes_numero_canonico_str = mes_map.get(nome_mes_norm)
                        if mes_numero_canonico_str:
                            logger.debug(f"[format_cadencia SL] Nome de mês '{mes_input_original}' (norm: {nome_mes_norm}) mapeado para '{mes_numero_canonico_str}'.")
                        else:
                            logger.warning(f"[format_cadencia SL] Nome de mês '{mes_input_original}' (norm: {nome_mes_norm}) não encontrado no mes_map na linha '{line_text}'. Pulando item.")
                            continue
                    else:
                        logger.warning(f"[format_cadencia SL] Tipo de mês inesperado: {type(mes_input_original)} com valor '{mes_input_original}' na linha '{line_text}'. Pulando item.")
                        continue 

                    valor_limpo = _clean_valor(valor_str)
                    if not valor_limpo:
                        logger.warning(f"[format_cadencia SL] Valor '{valor_str}' inválido na linha '{line_text}'. Pulando este match.")
                        continue
                    
                    mes_num_int = int(mes_numero_canonico_str)

                    year_for_this_entry, next_inferred_year, next_last_month = _determine_year(
                        ano_str_capturado, 
                        mes_num_int,
                        current_inferred_year, 
                        last_processed_month_num
                    )
                    current_inferred_year = next_inferred_year
                    last_processed_month_num = next_last_month
                    
                    output_cadencia_items.append({
                        "mes": mes_num_int,
                        "ano": year_for_this_entry,
                        "texto": f"{mes_numero_canonico_str}.{year_for_this_entry}:{valor_limpo} ton"
                    })
                    matched_line = True
                    logger.info(f"[format_cadencia SL] Linha processada: {output_cadencia_items[-1]['texto']} (Padrão: {pattern_name})")
                    break 
        
        if not matched_line:
            logger.warning(f"[format_cadencia] Nenhum padrão de cadência RELEVANTE encontrado para a linha: '{line_text}'")

    if not output_cadencia_items:
        logger.warning(f"[format_cadencia] Nenhuma linha de cadência pôde ser processada para o input: '{cadencia_str}'")
        return None

    output_cadencia_items.sort(key=lambda x: (x["ano"], x["mes"]))
    return "\n".join([item["texto"] for item in output_cadencia_items])


# --- O restante do arquivo (format_output_python, format_final_summary_text) ---
def format_output_python(mapped_data, cadencia_formatada):
    incoterm_val = mapped_data.get('Incoterms')
    frete_val = mapped_data.get('Preço Frete', None)
    display_frete_info = "N/A"

    if frete_val is not None and str(frete_val).strip():
        frete_str = str(frete_val)
        display_frete_info = frete_str
        if incoterm_val:
            incoterm_upper = incoterm_val.upper()
            if incoterm_upper == 'CIF': display_frete_info += " (CIF)"
            elif incoterm_upper == 'FOB': display_frete_info += " (FOB - Valor Informativo)"
            elif incoterm_upper == 'TPD': display_frete_info += " (TPD - Valor Informativo)" # MODIFICADO
            else: display_frete_info += f" (Incoterms: {incoterm_val})"
        else: display_frete_info += " (Incoterms não especificado)"
    elif incoterm_val:
        incoterm_upper = incoterm_val.upper()
        if incoterm_upper == 'CIF': display_frete_info = "N/A (CIF - Valor não informado)"
        elif incoterm_upper == 'FOB': display_frete_info = "N/A (FOB)"
        elif incoterm_upper == 'TPD': display_frete_info = "N/A (TPD)" # MODIFICADO
        else: display_frete_info = f"N/A (Incoterms: {incoterm_val})"

    cadencia_html = cadencia_formatada.replace('\n', '<br>') if cadencia_formatada is not None else 'N/A (Formato Inválido)'

    output = f"""
Data da solicitação: {datetime.now().strftime("%d/%m/%Y")}<br>
Vendedor: {mapped_data.get('Vendedor', 'N/A')}<br>
CNPJ/CPF: {mapped_data.get('CNPJ/CPF', 'N/A')}<br>
Cidade: {mapped_data.get('Cidade', 'N/A')}<br>
Email do vendedor: {mapped_data.get('Email do vendedor', 'N/A')}<br>
Planta: {mapped_data.get('Planta', 'N/A')}<br>
Nome do cliente: {mapped_data.get('Nome do cliente', 'N/A')}<br>
Código do cliente: {mapped_data.get('Código do cliente', 'N/A')}<br>
Campanha: {mapped_data.get('Campanha') or 'SEM REF'}<br>
Data da negociação: {mapped_data.get('Data de Negociação', 'N/A')}<br>
Condição de pagamento: {mapped_data.get('Condição de Pagamento', 'N/A')}<br>
Forma de pagamento: {mapped_data.get('Forma de Pagamento', 'N/A')}<br>
Incoterms: {mapped_data.get('Incoterms', 'N/A')}<br>
Preço frete: {display_frete_info}<br>
Preço: {mapped_data.get('Valor', 'N/A')}<br>
Código do material: {mapped_data.get('Código do Material', 'N/A')}<br>
-- Cadência Formatada --<br>
{cadencia_html}
"""
    return output.strip()

def format_final_summary_text(mapped_data, cadencia_formatada):
    incoterm_val = mapped_data.get('Incoterms')
    frete_val = mapped_data.get('Preço Frete', None)
    display_frete_info = "N/A"

    if frete_val is not None and str(frete_val).strip() and float(frete_val) != 0: # Se frete for 0.0, tratamos como N/A para FOB/TPD
        try: frete_str = f"{float(frete_val):.2f}".replace('.', ',') if isinstance(frete_val, (int, float)) else str(frete_val)
        except ValueError: frete_str = str(frete_val)
        display_frete_info = frete_str
        if incoterm_val:
            incoterm_upper = incoterm_val.upper()
            if incoterm_upper == 'CIF': display_frete_info += " (CIF)"
            elif incoterm_upper == 'FOB': display_frete_info += " (FOB - Valor Informativo)"
            elif incoterm_upper == 'TPD': display_frete_info += " (TPD - Valor Informativo)" # MODIFICADO
            else: display_frete_info += f" (Incoterms: {incoterm_val})"
        else: display_frete_info += " (Incoterms não especificado)"
    elif incoterm_val: # Se frete_val é None, vazio, ou 0.0
        incoterm_upper = incoterm_val.upper()
        if incoterm_upper == 'CIF':
            display_frete_info = "N/A (CIF - Valor não informado)"
        elif incoterm_upper == 'FOB':
            display_frete_info = "N/A (FOB)"
        elif incoterm_upper == 'TPD':
            display_frete_info = "N/A (TPD)" # MODIFICADO
        else:
            display_frete_info = f"N/A (Incoterms: {incoterm_val})"
    # Se incoterm_val também for None, display_frete_info permanece "N/A" (já inicializado)

    cadencia_plain = cadencia_formatada if cadencia_formatada is not None else 'N/A (Formato Inválido ou Não Reconhecido)'

    valor_str = "N/A"
    valor_val = mapped_data.get('Valor')
    if valor_val is not None:
        try: valor_str = f"{float(valor_val):.2f}".replace('.', ',') if isinstance(valor_val, (int, float)) else str(valor_val)
        except ValueError: valor_str = str(valor_val)

    output = f"""Data da solicitação: {datetime.now().strftime("%d/%m/%Y")}
Vendedor: {mapped_data.get('Vendedor', 'N/A')}
CNPJ/CPF: {mapped_data.get('CNPJ/CPF', 'N/A')}
Cidade: {mapped_data.get('Cidade', 'N/A')}
Email do vendedor: {mapped_data.get('Email do vendedor', 'N/A')}
Planta: {mapped_data.get('Planta', 'N/A')}
Nome do cliente: {mapped_data.get('Nome do cliente', '<não encontrado>')}
Código do cliente: {mapped_data.get('Código do cliente', 'N/A')}
Campanha: {mapped_data.get('Campanha') or 'SEM REF'}
Data da negociação: {mapped_data.get('Data de Negociação', 'N/A')}
Condição de pagamento: {mapped_data.get('Condição de Pagamento', 'N/A')}
Forma de pagamento: {mapped_data.get('Forma de Pagamento', 'N/A')}
Incoterms: {mapped_data.get('Incoterms', 'N/A')}
Preço frete: {display_frete_info}
Preço: {valor_str}
Código do material: {mapped_data.get('Código do Material', 'N/A')}
-- Cadência --
{cadencia_plain}"""

    output_lines = [line.strip() for line in output.split('\n')]
    return "\n".join(output_lines).strip()