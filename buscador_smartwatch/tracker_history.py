"""
Gestão de Histórico e Avaliação de Alertas para Smartwatches.
Controla quedas de preço a partir de R$ 150, tetos de oportunidade e cooldown anti-spam.
"""
import json
import time
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Tuple, Optional
from buscador_smartwatch.pelando_scanner import OfertaSmartwatch

logger = logging.getLogger("smartwatch.history")
HISTORICO_PATH = Path(__file__).resolve().parent / "historico_smartwatch.json"


def carregar_historico() -> Dict[str, Any]:
    if not HISTORICO_PATH.exists():
        return {}
    try:
        with open(HISTORICO_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Erro ao carregar histórico de smartwatch: {e}")
        return {}


def salvar_historico(historico: Dict[str, Any]) -> None:
    try:
        with open(HISTORICO_PATH, "w", encoding="utf-8") as f:
            json.dump(historico, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Erro ao salvar histórico de smartwatch: {e}")


def chave_oferta(oferta: OfertaSmartwatch) -> str:
    # Chave única e estável baseada no modelo e hash do link
    import hashlib
    link_hash = hashlib.md5(oferta.link.encode("utf-8")).hexdigest()[:12]
    return f"{oferta.modelo_id}_{link_hash}"



def avaliar_alerta_smartwatch(
    oferta: OfertaSmartwatch,
    cfg_modelo: Dict[str, Any],
    desconto_minimo_reais: float = 200.0,
    cooldown_horas: int = 12,
    historico: Optional[Dict[str, Any]] = None
) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Avalia se a oferta atende aos critérios do usuário:
    1. Preço com desconto de pelo menos R$ 200 em relação ao normal/referência.
    2. E preço estritamente abaixo do teto de oportunidade daquele modelo.
    3. OU cupom ativo expressivo (queda >= R$ 200).
    """
    if historico is None:
        historico = carregar_historico()

    teto = cfg_modelo.get("preco_teto_oportunidade")
    preco_normal = cfg_modelo.get("preco_normal_referencia", oferta.preco_normal)
    queda_vs_normal = round(preco_normal - oferta.preco, 2)
    desc_min = cfg_modelo.get("desconto_minimo_alerta_reais", desconto_minimo_reais)

    # Regra estrita: se houver teto estipulado para o modelo,
    # o preço deve ser estritamente menor que o teto ("só abaixo de 200 para me avisar")
    if teto is not None and oferta.preco >= teto:
        return False, None, None

    chave = chave_oferta(oferta)
    reg = historico.get(chave, {})
    ultimo_preco = reg.get("ultimo_preco")
    ultimo_alerta_ts = reg.get("ultimo_alerta_ts", 0)
    menor_historico = reg.get("menor_historico", oferta.preco)

    deve_alertar = False
    motivo = ""
    detalhes = []

    # Critério 1: Queda do preço normal atendendo ao desconto mínimo (R$ 200)
    if queda_vs_normal >= desc_min:
        deve_alertar = True
        motivo = "DESCONTO_200_REAIS"
        teto_txt = f" e abaixo do teto (R$ {teto:,.2f})" if teto else ""
        detalhes.append(f"Preço R$ {oferta.preco:,.2f} está R$ {queda_vs_normal:,.2f} abaixo da referência de hoje (R$ {preco_normal:,.2f}){teto_txt}")

    # Critério 2: Furou o teto de oportunidade
    elif teto is not None and oferta.preco < teto:
        deve_alertar = True
        motivo = "TETO_OPORTUNIDADE"
        detalhes.append(f"Preço R$ {oferta.preco:,.2f} abaixo do teto de oportunidade (R$ {teto:,.2f})")

    # Critério 3: Cupom de desconto ativo com economia relevante
    elif oferta.cupom and queda_vs_normal >= desc_min:
        deve_alertar = True
        motivo = "CUPOM_ATIVO"
        detalhes.append(f"Cupom ativo detectado: {oferta.cupom} (Economia de R$ {queda_vs_normal:,.2f})")

    if not deve_alertar:
        return False, None, None

    # Controle de Cooldown e Anti-Spam
    agora_ts = time.time()
    horas_desde_ultimo = (agora_ts - ultimo_alerta_ts) / 3600.0

    # Se o preço caiu ainda mais (ao menos R$ 50 a menos que o menor registrado), fura o cooldown
    if oferta.preco < (menor_historico - 50.0):
        detalhes.append(f"Preço caiu ainda mais em relação ao último alerta (-R$ {(menor_historico - oferta.preco):,.2f})")
    elif horas_desde_ultimo < cooldown_horas:
        return False, None, None

    return True, motivo, " | ".join(detalhes)


def registrar_oferta(oferta: OfertaSmartwatch, historico: Dict[str, Any], foi_alertado: bool = False) -> None:
    chave = chave_oferta(oferta)
    agora_iso = datetime.now().isoformat()
    agora_ts = time.time()

    reg = historico.get(chave, {
        "modelo_id": oferta.modelo_id,
        "titulo": oferta.titulo,
        "loja": oferta.loja,
        "link": oferta.link,
        "menor_historico": oferta.preco,
        "total_alertas": 0
    })

    reg["ultimo_preco"] = oferta.preco
    reg["ultima_atualizacao"] = agora_iso
    if oferta.preco < reg.get("menor_historico", oferta.preco):
        reg["menor_historico"] = oferta.preco

    if oferta.cupom:
        reg["ultimo_cupom"] = oferta.cupom

    if foi_alertado:
        reg["ultimo_alerta_ts"] = agora_ts
        reg["ultimo_alerta_data"] = agora_iso
        reg["total_alertas"] = reg.get("total_alertas", 0) + 1

    historico[chave] = reg
