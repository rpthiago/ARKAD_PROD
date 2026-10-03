import json
import time
import logging
from pathlib import Path
from typing import Dict, Any, Tuple, Optional
from datetime import datetime

logger = logging.getLogger("buscador_internacional.history")

HISTORICO_PATH = Path(__file__).resolve().parent / "historico_internacional.json"


def carregar_historico() -> Dict[str, Any]:
    if not HISTORICO_PATH.exists():
        return {}
    try:
        with open(HISTORICO_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error("Erro ao carregar historico_internacional.json: %s", e)
        return {}


def salvar_historico(dados: Dict[str, Any]):
    try:
        HISTORICO_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(HISTORICO_PATH, "w", encoding="utf-8") as f:
            json.dump(dados, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error("Erro ao salvar historico_internacional.json: %s", e)


def avaliar_alerta_internacional(
    cotacao,
    cfg_dest: Dict[str, Any],
    cfg_regras: Dict[str, Any],
    historico: Dict[str, Any],
) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Avalia se uma cotação internacional atinge os critérios agressivos de oportunidade.
    """
    if not cotacao.success or not cotacao.menor_voo:
        return False, None, None

    preco_atual = cotacao.menor_voo.price
    rota_chave = f"{cotacao.origem}_{cotacao.destino}_{cotacao.data_ida}"

    teto_alvo = cfg_dest.get("preco_teto_alerta", 999999)
    alerta_se_teto = cfg_regras.get("alerta_se_preco_abaixo_teto", True)
    alerta_se_baixo = cfg_regras.get("alerta_se_nivel_google_baixo", True)
    queda_minima = cfg_regras.get("queda_minima_alerta_reais", 400)
    cooldown_horas = cfg_regras.get("cooldown_horas", 12)

    hist_item = historico.get(rota_chave, {})
    ultimo_preco = hist_item.get("ultimo_preco")
    ultimo_alerta_ts = hist_item.get("ultimo_alerta_ts", 0)
    ultimo_alerta_preco = hist_item.get("ultimo_alerta_preco", 999999)

    deve_alertar = False
    motivo = ""
    detalhes = []

    # 1. Critério Principal: Furou o teto super agressivo
    if alerta_se_teto and preco_atual <= teto_alvo:
        deve_alertar = True
        motivo = "PRECO_AGRESSIVO"
        detalhes.append(f"Preço R$ {preco_atual:,} abaixo do teto agressivo de R$ {teto_alvo:,}")

    # 2. Critério Secundário: Google Flights marcou 'baixo', mas SOMENTE se estiver dentro do teto
    elif alerta_se_baixo and cotacao.price_level == "baixo" and preco_atual <= teto_alvo:
        deve_alertar = True
        motivo = "NIVEL_BAIXO"
        detalhes.append(f"Google Flights detectou nível de preço BAIXO (R$ {preco_atual:,})")

    # 3. Critério Queda Súbita: Queda violenta de mais de R$ 600 E próximo do teto
    elif ultimo_preco and (ultimo_preco - preco_atual) >= queda_minima and preco_atual <= (teto_alvo * 1.10):
        deve_alertar = True
        motivo = "QUEDA_SUBITA"
        diff = ultimo_preco - preco_atual
        detalhes.append(f"Queda expressiva de R$ {diff:,} (de R$ {ultimo_preco:,} para R$ {preco_atual:,})")

    if not deve_alertar:
        return False, None, None

    # Cooldown e Anti-Spam
    agora_ts = time.time()
    horas_desde_ultimo = (agora_ts - ultimo_alerta_ts) / 3600.0

    # Se o preço caiu ainda mais do que o último alertado (ao menos R$ 100), re-alerta
    if preco_atual < (ultimo_alerta_preco - 100):
        pass
    elif horas_desde_ultimo < cooldown_horas:
        return False, None, None

    return True, motivo, " | ".join(detalhes)


def registrar_cotacao_internacional(cotacao, historico: Dict[str, Any], foi_alertado: bool = False):
    if not cotacao.success or not cotacao.menor_voo:
        return

    rota_chave = f"{cotacao.origem}_{cotacao.destino}_{cotacao.data_ida}"
    item = historico.get(rota_chave, {
        "origem": cotacao.origem,
        "destino": cotacao.destino,
        "data_ida": cotacao.data_ida,
        "data_volta": cotacao.data_volta,
        "menor_historico": 999999,
        "consultas": []
    })

    preco_atual = cotacao.menor_voo.price
    menor_ant = item.get("menor_historico", 999999)
    item["menor_historico"] = min(menor_ant, preco_atual)
    item["ultimo_preco"] = preco_atual
    item["ultima_cia"] = cotacao.menor_voo.airline
    item["ultimas_paradas"] = cotacao.menor_voo.stops
    item["ultimo_nivel"] = cotacao.price_level
    item["ultima_atualizacao"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if cotacao.menor_direto:
        item["ultimo_preco_direto"] = cotacao.menor_direto.price

    if foi_alertado:
        item["ultimo_alerta_ts"] = time.time()
        item["ultimo_alerta_preco"] = preco_atual
        item["ultimo_alerta_data"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    consultas = item.get("consultas", [])
    consultas.append({
        "ts": time.time(),
        "data": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "preco": preco_atual,
        "cia": cotacao.menor_voo.airline,
        "stops": cotacao.menor_voo.stops,
    })
    item["consultas"] = consultas[-20:]

    historico[rota_chave] = item
    salvar_historico(historico)
