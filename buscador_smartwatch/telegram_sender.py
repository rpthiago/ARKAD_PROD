"""
Módulo de Notificação e Formatação de Alertas no Telegram para Smartwatches.
Reutiliza as credenciais de telegram_voos_config.json e garante TLS OpenSSL no Task Scheduler.
"""
import os
import sys
import json
import logging
import requests
from pathlib import Path
from typing import List, Dict, Any, Optional
from buscador_smartwatch.pelando_scanner import OfertaSmartwatch

# OpenSSL DLL injection
def _configurar_dlls_anaconda():
    prefixos = [getattr(sys, "prefix", None), getattr(sys, "base_prefix", None)]
    for p in prefixos:
        if not p:
            continue
        lib_bin = os.path.join(p, "Library", "bin")
        if os.path.isdir(lib_bin):
            if hasattr(os, "add_dll_directory"):
                try:
                    os.add_dll_directory(lib_bin)
                except Exception:
                    pass
            if lib_bin not in os.environ.get("PATH", ""):
                os.environ["PATH"] = lib_bin + os.pathsep + os.environ.get("PATH", "")

_configurar_dlls_anaconda()

logger = logging.getLogger("smartwatch.telegram")
ROOT_DIR = Path(__file__).resolve().parent.parent
CONFIG_SMARTWATCH_FILE = ROOT_DIR / "telegram_smartwatch_config.json"
CONFIG_VOOS_FILE = ROOT_DIR / "telegram_voos_config.json"


def carregar_credenciais_telegram():
    token = os.environ.get("TELEGRAM_SMARTWATCH_BOT_TOKEN")
    chat_ids = []

    target_file = CONFIG_SMARTWATCH_FILE if CONFIG_SMARTWATCH_FILE.exists() else CONFIG_VOOS_FILE
    if target_file.exists():
        try:
            with open(target_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                token = token or data.get("bot_token")
                if "chat_ids" in data and isinstance(data["chat_ids"], list):
                    chat_ids = [str(cid).strip() for cid in data["chat_ids"] if cid]
                elif data.get("chat_id"):
                    chat_ids = [str(data.get("chat_id")).strip()]
        except Exception as e:
            logger.error("Erro ao ler credenciais do Telegram: %s", e)

    return token, chat_ids


def enviar_mensagem(texto: str) -> bool:
    token, chat_ids = carregar_credenciais_telegram()
    if not token or not chat_ids:
        logger.error("Telegram não configurado em telegram_voos_config.json")
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    sucesso = False

    for cid in chat_ids:
        payload = {
            "chat_id": cid,
            "text": texto,
            "parse_mode": "HTML",
            "disable_web_page_preview": False
        }
        try:
            r = requests.post(url, json=payload, timeout=12)
            if r.status_code == 200:
                sucesso = True
            else:
                logger.error(f"Erro Telegram ({r.status_code}): {r.text}")
        except Exception as e:
            logger.error(f"Exceção ao enviar mensagem Telegram: {e}")

    return sucesso


def formatar_alerta_smartwatch(oferta: OfertaSmartwatch, motivo: str, detalhes: str) -> str:
    cupom_bloco = ""
    if oferta.cupom:
        cupom_bloco = f"🎟️ <b>CUPOM DE DESCONTO:</b> <code>{oferta.cupom}</code>\n"

    safe_link = oferta.link.replace("&", "&amp;")

    msg = (
        f"🚨 🔥 <b>OPORTUNIDADE DE SMARTWATCH! {oferta.emoji}</b>\n\n"
        f"⌚ <b>{oferta.marca} {oferta.modelo_nome} ({oferta.tamanho_caixa})</b>\n"
        f"🏪 <b>Loja:</b> {oferta.loja}\n"
        f"🏷️ <b>Condição:</b> 🟢 Novo / Lacrado\n\n"
        f"💰 <b>Preço Promocional: R$ {oferta.preco:,.2f}</b>\n"
        f"📉 <b>Preço Normal:</b> R$ {oferta.preco_normal:,.2f}\n"
        f"🟢 <b>Economia Real: R$ {oferta.economia:,.2f} ({oferta.desconto_pct:.1f}% OFF)</b>\n\n"
        f"{cupom_bloco}"
        f"⚡ <b>Gatilho:</b> {detalhes}\n\n"
        f"🔗 <a href=\"{safe_link}\"><b>👉 Ver Oferta na Loja</b></a>"
    )
    return msg


def formatar_resumo_smartwatches(ranking_melhores: List[OfertaSmartwatch]) -> str:
    linhas = [
        "📊 ⌚ <b>RADAR DE SMARTWATCHES — RANKING ATUAL</b>\n"
        "<i>Monitorando Amazfit Balance, GTR, GTS e Huawei Watch GT</i>\n"
    ]

    for idx, of in enumerate(ranking_melhores, 1):
        cupom_txt = f" (🎟️ Cupom: <code>{of.cupom}</code>)" if of.cupom else ""
        safe_link = of.link.replace("&", "&amp;")
        status_promo = " 🎯 PROMOÇÃO!" if of.economia >= 150.0 else ""
        
        linha = (
            f"{idx}. {of.emoji} <b>{of.marca} {of.modelo_nome} ({of.tamanho_caixa})</b>{status_promo}\n"
            f"   💰 <b>R$ {of.preco:,.2f}</b> | Normal: R$ {of.preco_normal:,.2f} (-R$ {of.economia:,.2f})\n"
            f"   🏪 {of.loja}{cupom_txt}\n"
            f"   🔗 <a href=\"{safe_link}\">Acessar Oferta</a>"
        )
        linhas.append(linha)

    linhas.append("\n💡 <i>Alerta individual no Telegram dispara assim que o preço cair R$ 150+ ou surgir cupom!</i>")
    return "\n\n".join(linhas)
