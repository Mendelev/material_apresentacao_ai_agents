# src/utils/normalization.py
import re
from unidecode import unidecode
import logging # Adicionado para logar erros potenciais

logger = logging.getLogger(__name__)

def normalize_string(text: str | None, remove_hyphens: bool = False) -> str | None:
    if text is None:
        return None
    try:
        text_str = str(text)
        normalized = unidecode(text_str).lower()
        normalized = normalized.strip()
        normalized = re.sub(r'\s+', ' ', normalized)
        normalized = re.sub(r'(?<=[a-z])\.(?=[a-z])', ' ', normalized)

        if remove_hyphens:
            normalized = normalized.replace('-', ' ')
            normalized = re.sub(r'\s+', ' ', normalized).strip()

        # Adicionar tratamento específico para "avista" -> "a vista"
        if normalized == "avista":
            normalized = "a vista"

        return normalized
    except Exception as e:
        logger.error(f"Erro ao normalizar texto '{text}': {e}", exc_info=True)
        return str(text)