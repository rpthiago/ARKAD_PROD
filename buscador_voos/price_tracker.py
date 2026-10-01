import json
import time
import logging
from pathlib import Path
from typing import Dict, Any, Tuple, Optional
from datetime import datetime

logger = logging.getLogger("buscador_voos.tracker")

HISTORICO_PATH = Path(__file__).resolve().parent / "historico_precos.json"


def carregar_historico() -> Dict[str, Any]:
    if not HISTORICO_PATH.exists():
        return {}
    try:
        with open(HISTORICO_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error("Erro ao carregar historico_precos.json: %s", e)
        return {}


def salvar_historico(dados: Dict[str, Any]):
    try:
        HISTORICO_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(HISTORICO_PATH, "w", encoding="utf-8") as f:
            json.dump(dados, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error("Erro ao salvar historico_precos.json: %s", e)


def avaliar_alerta(
    res_busca,
    cfg_dest: Dict[str, Any],
    cfg_regras: Dict[str, Any],
    historico: Dict[str, Any],
) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Avalia se o resultado da busca constitui uma oportunidade de preço bom que justifique alerta.
    
    Retorna:
        (deve_alertar: bool, motivo: str, detalhe: str)
    """
    if not res_busca.success or not res_busca.menor_geral:
        return False, None, None

    # Considera o menor voo de até 1 parada (se configurado) ou o menor geral
    voo_referencia = res_busca.menor_ate_1_parada or res_busca.menor_geral
    preco_atual = voo_referencia.price
    dest_code = res_busca.destino

    preco_alvo = cfg_dest.get("preco_alvo", 999999)
    alerta_se_preco_alvo = cfg_regras.get("alerta_se_atingir_preco_alvo", True)
    alerta_se_nivel_baixo = cfg_regras.get("alerta_se_nivel_google_baixo", True)
    queda_subita_reais = cfg_regras.get("alerta_se_queda_subita_reais", 150)
    cooldown_horas = cfg_regras.get("cooldown_alerta_horas", 12)

    dest_hist = historico.get(dest_code, {})
    menor_anterior = dest_hist.get("menor_preco_historico")
    ultimo_preco = dest_hist.get("ultimo_preco")
    ultimo_alerta_ts = dest_hist.get("ultimo_alerta_ts", 0)
    ultimo_alerta_preco = dest_hist.get("ultimo_alerta_preco", 999999)

    deve_alertar = False
    motivo = ""
    detalhes = []

    # 1. Critério: Atingiu Preço Alvo
    if alerta_se_preco_alvo and preco_atual <= preco_alvo:
        deve_alertar = True
        motivo = "PRECO_ALVO"
        detalhes.append(f"Preço R$ {preco_atual:,} abaixo do teto estipulado de R$ {preco_alvo:,}")

    # 2. Critério: Google Flights classificou como "BAIXO" (low)
    if alerta_se_nivel_baixo and res_busca.price_level == "baixo":
        deve_alertar = True
        if not motivo:
            motivo = "NIVEL_BAIXO"
        detalhes.append("Google Flights detectou nível de preço BAIXO para a rota")

    # 3. Critério: Queda súbita em relação ao último preço conhecido
    if ultimo_preco and (ultimo_preco - preco_atual) >= queda_subita_reais:
        deve_alertar = True
        if not motivo:
            motivo = "QUEDA_SUBITA"
        diff = ultimo_preco - preco_atual
        detalhes.append(f"Queda de R$ {diff:,} em relação à última cotação (de R$ {ultimo_preco:,} para R$ {preco_atual:,})")

    if not deve_alertar:
        return False, None, None

    # Verifica Cooldown e Anti-Spam
    agora_ts = time.time()
    horas_desde_ultimo = (agora_ts - ultimo_alerta_ts) / 3600.0

    # Se o preço caiu ainda mais do que o último alertado (ao menos R$ 30), ignora cooldown
    if preco_atual < (ultimo_alerta_preco - 30):
        # Permite novo alerta pois o preço melhorou ainda mais!
        pass
    elif horas_desde_ultimo < cooldown_horas:
        logger.info(
            "Destino %s atingiu critério (%s), mas está em cooldown (alertado há %.1f horas por R$ %s).",
            dest_code, motivo, horas_desde_ultimo, ultimo_alerta_preco
        )
        return False, None, None

    return True, motivo, " | ".join(detalhes)


def registrar_cotacao(res_busca, historico: Dict[str, Any], foi_alertado: bool = False):
    """Atualiza o histórico persistente com os dados da última cotação."""
    if not res_busca.success or not res_busca.menor_geral:
        return

    dest_code = res_busca.destino
    dest_hist = historico.get(dest_code, {
        "menor_preco_historico": 999999,
        "maior_preco_historico": 0,
        "consultas": []
    })

    voo_ref = res_busca.menor_ate_1_parada or res_busca.menor_geral
    preco_atual = voo_ref.price

    menor_ant = dest_hist.get("menor_preco_historico", 999999)
    maior_ant = dest_hist.get("maior_preco_historico", 0)

    dest_hist["menor_preco_historico"] = min(menor_ant, preco_atual)
    dest_hist["maior_preco_historico"] = max(maior_ant, preco_atual)
    dest_hist["ultimo_preco"] = preco_atual
    dest_hist["ultimo_nivel"] = res_busca.price_level
    dest_hist["ultima_cia"] = voo_ref.airline
    dest_hist["ultimas_paradas"] = voo_ref.stops
    dest_hist["ultima_atualizacao"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if res_busca.menor_direto:
        dest_hist["ultimo_preco_direto"] = res_busca.menor_direto.price

    if foi_alertado:
        dest_hist["ultimo_alerta_ts"] = time.time()
        dest_hist["ultimo_alerta_preco"] = preco_atual
        dest_hist["ultimo_alerta_data"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Mantém os últimos 50 registros de histórico
    consultas = dest_hist.get("consultas", [])
    consultas.append({
        "ts": time.time(),
        "data": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "preco": preco_atual,
        "direto": res_busca.menor_direto.price if res_busca.menor_direto else None,
        "nivel": res_busca.price_level,
        "cia": voo_ref.airline,
        "stops": voo_ref.stops
    })
    dest_hist["consultas"] = consultas[-50:]

    historico[dest_code] = dest_hist
    salvar_historico(historico)
