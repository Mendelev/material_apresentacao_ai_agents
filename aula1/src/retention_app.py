# -*- coding: utf-8 -*-
"""
retention_app.py ▸ Assistente de Retenção multi‑provedor (Azure OpenAI | Gemini)
---------------------------------------------------------------------------------
• Agente 1 gera **Markdown completo** com TODOS os campos presentes nos CRMs e ServiceNow.
• Agente 2 cria estratégia de retenção, já pronta para renderizar no Streamlit (sem cercas de código).

Instalação rápida::
    pip install -U openai google-genai python-dotenv streamlit

Variáveis de ambiente (.env)::
    # Azure
    AZURE_OPENAI_ENDPOINT=https://<end>.openai.azure.com
    AZURE_OPENAI_KEY=<key>
    AZURE_OPENAI_DEPLOYMENT_ID=gpt-4o

    # Gemini
    GOOGLE_GENAI_API_KEY=<key>
    GEMINI_MODEL=gemini-2.0-pro   # opcional

Execução::
    python retention_app.py clientes.json --provider gemini
    streamlit run retention_app.py -- --ui --provider azure
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv

# SDKs dinâmicos -------------------------------------------------------------
try:
    from openai import AzureOpenAI  # type: ignore
except ModuleNotFoundError:
    AzureOpenAI = None

try:
    from google import genai  # type: ignore
except ModuleNotFoundError:
    genai = None

# Helpers --------------------------------------------------------------------
load_dotenv()

def _env(name: str, *, default: str | None = None, required: bool = False) -> str | None:
    val = os.getenv(name, default)
    if required and not val:
        sys.exit(f"[ERRO] Defina {name} no ambiente ou .env")
    return val

# LLM Provider ---------------------------------------------------------------
class LLMProvider:
    """Wrapper para Azure OpenAI ou Google Gemini (google‑genai)."""

    def __init__(self, provider: str):
        self.provider = provider.lower()
        if self.provider == "azure":
            if AzureOpenAI is None:
                sys.exit("pip install openai")
            endpoint = _env("AZURE_OPENAI_ENDPOINT", required=True).rstrip("/")
            key = _env("AZURE_OPENAI_KEY", required=True)
            version = _env("AZURE_OPENAI_API_VERSION", default="2024-02-15-preview")
            self.model = _env("AZURE_OPENAI_DEPLOYMENT_ID", default="gpt-4o")
            self.client = AzureOpenAI(azure_endpoint=endpoint, api_key=key, api_version=version)
            self._complete = self._complete_azure
        elif self.provider == "gemini":
            if genai is None:
                sys.exit("pip install google-genai")
            api_key = _env("GOOGLE_GENAI_API_KEY", required=True)
            self.model = _env("GEMINI_MODEL", default="gemini-2.0-pro")
            self.client = genai.Client(api_key=api_key)
            self._complete = self._complete_gemini
        else:
            sys.exit("--provider deve ser 'azure' ou 'gemini'")

    # back‑end Azure
    def _complete_azure(self, messages: List[Dict[str, str]], temperature: float) -> str:
        resp = self.client.chat.completions.create(model=self.model, messages=messages, temperature=temperature)
        return resp.choices[0].message.content.strip()

    # back‑end Gemini
    def _messages_to_prompt(self, messages: List[Dict[str, str]]) -> str:
        return "\n".join(f"{m['role'].capitalize()}: {m['content']}" for m in messages)

    def _complete_gemini(self, messages: List[Dict[str, str]], temperature: float) -> str:
        prompt = self._messages_to_prompt(messages)
        resp = self.client.models.generate_content(model=self.model, contents=prompt)
        return getattr(resp, "text", str(resp)).strip()

    # API pública
    def chat(self, messages: List[Dict[str, str]], *, temperature: float = 0.3) -> str:
        return self._complete(messages, temperature)

# Agents ---------------------------------------------------------------------

def summarize_clients(llm: LLMProvider, raw_clients: Any) -> str:
    """Gera resumo detalhado em Markdown incluindo todos os campos presentes."""
    system = (
        "Você é o *Agente de Consolidação de Dados*. Para **cada** cliente receba os sub‑objetos CRM1, CRM2, CRM3 e ServiceNow e produza Markdown no formato:\n\n"
        "## <Nome do Cliente>\n"
        "### CRM1\n- contract_id: …\n- data_inicio: …\n…\n"
        "### CRM2\n…\n"
        "### CRM3\n…\n"
        "### ServiceNow\n- ticket_id: …\n- data_ticket: …\n- motivo: …\n- histórico:\n  * remetente – data: mensagem\n\n"
        "Inclua **todos** os pares chave/valor exatamente como aparecem (traduza apenas os rótulos fixes como 'histórico'). Se algum campo não existir, ignore‑o; não invente." 
    )
    user = f"JSON original:\n```json\n{json.dumps(raw_clients, ensure_ascii=False)}\n```"
    msgs = [{"role": "system", "content": system}, {"role": "user", "content": user}]
    return llm.chat(msgs)


def retention_strategy(llm: LLMProvider, summary_md: str) -> str:
    """Cria estratégia de retenção a partir do Markdown (sem cercas de código)."""
    system = (
        "Você é o *Especialista em Retenção*. Com base no resumo, elabore Markdown:\n"
        "## Estratégia para <Nome>\n### Pontos‑Chave\n1. …\n### Argumentos Persuasivos\n- …\n### Próximos Passos\n…"
    )
    user = f"Resumo consolidado em Markdown:\n{summary_md}"
    msgs = [{"role": "system", "content": system}, {"role": "user", "content": user}]
    return llm.chat(msgs, temperature=0.4)

# CLI & Streamlit ------------------------------------------------------------

def run_cli(llm: LLMProvider, path: Path) -> None:
    data = json.loads(path.read_text(encoding="utf-8"))
    summary = summarize_clients(llm, data)
    strategy = retention_strategy(llm, summary)
    print("\n# RESUMO\n", summary)
    print("\n# ESTRATÉGIA\n", strategy)


def run_streamlit(llm: LLMProvider):
    try:
        import streamlit as st
    except ModuleNotFoundError:
        sys.exit("pip install streamlit")

    st.set_page_config(page_title="Retenção de Clientes", layout="wide")
    st.title(f"Assistente de Retenção – {llm.provider.capitalize()}")

    raw = st.text_area("Cole o JSON de entrada", height=300)
    if st.button("Processar"):
        if not raw.strip():
            st.error("Entrada vazia"); st.stop()
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            st.error(f"JSON inválido: {e}"); st.stop()

        with st.spinner("Gerando resumo …"):
            summary = summarize_clients(llm, data)
        with st.spinner("Gerando estratégia …"):
            strategy = retention_strategy(llm, summary)

        col1, col2 = st.columns(2)
        col1.subheader("Resumo Consolidado"); col1.markdown(summary)
        col2.subheader("Estratégia de Retenção"); col2.markdown(strategy)

        st.download_button("Baixar resumo.md", summary, "resumo.md", "text/markdown")
        st.download_button("Baixar estrategia.md", strategy, "estrategia.md", "text/markdown")

# entrypoint -----------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Assistente de Retenção – multi‑provedor")
    parser.add_argument("input", nargs="?", help="Arquivo JSON de entrada (CLI)")
    parser.add_argument("--ui", action="store_true", help="Abrir UI Streamlit")
    parser.add_argument("--provider", choices=["azure", "gemini"], default="azure", help="LLM backend")
    args = parser.parse_args()

    llm = LLMProvider(args.provider)

    if args.ui:
        run_streamlit(llm)
    elif args.input:
        run_cli(llm, Path(args.input))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
