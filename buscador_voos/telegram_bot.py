import os
import json
import logging
import requests
from pathlib import Path

logger = logging.getLogger("buscador_voos.telegram")

ROOT_DIR = Path(__file__).resolve().parent.parent
CONFIG_VOOS_FILE = ROOT_DIR / "telegram_voos_config.json"
CONFIG_LEGACY_FILE = ROOT_DIR / "telegram_config.json"


def carregar_credenciais_telegram():
    """Carrega bot_token e lista de chat_ids do arquivo telegram_voos_config.json ou fallback."""
    token = os.environ.get("TELEGRAM_VOOS_BOT_TOKEN")
    chat_ids = []

    target_file = CONFIG_VOOS_FILE if CONFIG_VOOS_FILE.exists() else CONFIG_LEGACY_FILE
    if target_file.exists():
        try:
            with open(target_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                token = token or data.get("bot_token")
                # Suporta tanto lista 'chat_ids' quanto único 'chat_id'
                if "chat_ids" in data and isinstance(data["chat_ids"], list):
                    chat_ids = [str(cid).strip() for cid in data["chat_ids"] if cid]
                elif data.get("chat_id"):
                    chat_ids = [str(data.get("chat_id")).strip()]
        except Exception as e:
            logger.error("Erro ao ler %s: %s", target_file, e)

    if not chat_ids:
        env_cid = os.environ.get("TELEGRAM_VOOS_CHAT_ID") or os.environ.get("TELEGRAM_CHAT_ID")
        if env_cid:
            chat_ids = [str(env_cid).strip()]

    return token, chat_ids


def enviar_mensagem(texto: str, parse_mode: str = "HTML", disable_web_page_preview: bool = False) -> bool:
    """
    Envia uma mensagem para todos os chat_ids configurados.
    Tenta com o parse_mode solicitado; se falhar na formatação, tenta como texto simples.
    """
    token, chat_ids = carregar_credenciais_telegram()
    if not token or not chat_ids:
        logger.warning("Credenciais do Telegram não encontradas. Mensagem não enviada.")
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    pelo_menos_um_sucesso = False

    for cid in chat_ids:
        payload = {
            "chat_id": cid,
            "text": texto,
            "parse_mode": parse_mode,
            "disable_web_page_preview": disable_web_page_preview,
        }

        try:
            resp = requests.post(url, json=payload, timeout=15)
            data = resp.json()
            if data.get("ok"):
                pelo_menos_um_sucesso = True
            else:
                logger.warning("Erro Telegram para chat %s: %s. Tentando sem formatação...", cid, data.get("description"))
                payload.pop("parse_mode", None)
                resp2 = requests.post(url, json=payload, timeout=15)
                if resp2.json().get("ok"):
                    pelo_menos_um_sucesso = True
        except Exception as e:
            logger.error("Exceção ao enviar mensagem Telegram para %s: %s", cid, e)

    return pelo_menos_um_sucesso
