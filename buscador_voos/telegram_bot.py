import os
import json
import logging
import requests
from pathlib import Path

logger = logging.getLogger("buscador_voos.telegram")

ROOT_DIR = Path(__file__).resolve().parent.parent
CONFIG_FILE = ROOT_DIR / "telegram_config.json"


def carregar_credenciais_telegram():
    """Carrega bot_token e chat_id do arquivo telegram_config.json ou env vars."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")

    if (not token or not chat_id) and CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                token = token or data.get("bot_token")
                chat_id = chat_id or data.get("chat_id")
        except Exception as e:
            logger.error("Erro ao ler telegram_config.json: %s", e)

    return token, chat_id


def enviar_mensagem(texto: str, parse_mode: str = "HTML", disable_web_page_preview: bool = False) -> bool:
    """
    Envia uma mensagem para o Telegram do usuário.
    Tenta com o parse_mode solicitado; se falhar na formatação, tenta como texto simples.
    """
    token, chat_id = carregar_credenciais_telegram()
    if not token or not chat_id:
        logger.warning("Credenciais do Telegram não encontradas. Mensagem não enviada.")
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": texto,
        "parse_mode": parse_mode,
        "disable_web_page_preview": disable_web_page_preview,
    }

    try:
        resp = requests.post(url, json=payload, timeout=15)
        data = resp.json()
        if data.get("ok"):
            return True
        else:
            logger.warning("Telegram retornou erro: %s. Tentando sem formatação...", data.get("description"))
            # Fallback sem formatação
            payload.pop("parse_mode", None)
            resp2 = requests.post(url, json=payload, timeout=15)
            return bool(resp2.json().get("ok"))
    except Exception as e:
        logger.error("Exceção ao enviar mensagem Telegram: %s", e)
        return False
