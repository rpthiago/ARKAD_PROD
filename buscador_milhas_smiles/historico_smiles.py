"""
Gerenciamento de histórico e controle de frequência (cooldown) de alertas de milhas.
Evita envio repetido do mesmo voo dentro da janela de cooldown.
"""
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Tuple

logger = logging.getLogger("historico_smiles")
HISTORICO_PATH = Path(__file__).resolve().parent / "historico_smiles.json"


def carregar_historico() -> Dict[str, Any]:
    if not HISTORICO_PATH.exists():
        return {}
    try:
        with open(HISTORICO_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Erro ao ler histórico de milhas ({e}). Reiniciando histórico.")
        return {}


def salvar_historico(historico: Dict[str, Any]) -> None:
    try:
        with open(HISTORICO_PATH, "w", encoding="utf-8") as f:
            json.dump(historico, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Erro ao salvar histórico de milhas: {e}")


def chave_alerta(origem: str, destino: str, data_ida: str, data_volta: str = None) -> str:
    if data_volta:
        return f"{origem}_{destino}_{data_ida}_{data_volta}"
    return f"{origem}_{destino}_{data_ida}"


def avaliar_alerta_smiles(
    origem: str,
    destino: str,
    data_ida: str,
    data_volta: str,
    milhas: int,
    teto_milhas: int,
    cooldown_horas: int = 12,
) -> Tuple[bool, str, str]:
    """
    Avalia se a cotação em milhas deve gerar alerta no Telegram.
    Retorna: (deve_alertar, motivo, detalhes)
    """
    if milhas > teto_milhas:
        return False, "FORA_DO_TETO", f"Milhas ({milhas:,}) acima do teto agressivo ({teto_milhas:,})"

    historico = carregar_historico()
    chave = chave_alerta(origem, destino, data_ida, data_volta)
    registro = historico.get(chave)

    agora = datetime.now()

    if registro:
        ultimo_alerta_str = registro.get("ultimo_alerta")
        menor_milhas_anterior = registro.get("menor_milhas", milhas)

        if ultimo_alerta_str:
            try:
                ultimo_alerta_dt = datetime.fromisoformat(ultimo_alerta_str)
                horas_passadas = (agora - ultimo_alerta_dt).total_seconds() / 3600.0

                # Se as milhas caíram ainda mais (pelo menos 5.000 milhas a menos), fura o cooldown
                if milhas < menor_milhas_anterior - 5000:
                    detalhes = f"Milhas caíram de {menor_milhas_anterior:,} para {milhas:,}!"
                    return True, "QUEDA_EXTRA_MILHAS", detalhes

                # Se está dentro do período de cooldown, silencia
                if horas_passadas < cooldown_horas:
                    return False, "COOLDOWN", f"Alertado há {horas_passadas:.1f}h (cooldown: {cooldown_horas}h)"
            except Exception:
                pass

    detalhes = f"Milhas dentro do teto promocional: {milhas:,} (teto: {teto_milhas:,})"
    return True, "TETO_PROMOCIONAL", detalhes


def registrar_alerta_smiles(
    origem: str,
    destino: str,
    data_ida: str,
    data_volta: str,
    milhas: int,
    cia_aerea: str,
    custo_reais: float,
) -> None:
    """Grava o registro de envio de alerta no histórico."""
    historico = carregar_historico()
    chave = chave_alerta(origem, destino, data_ida, data_volta)
    agora_iso = datetime.now().isoformat()

    historico[chave] = {
        "origem": origem,
        "destino": destino,
        "data_ida": data_ida,
        "data_volta": data_volta,
        "menor_milhas": milhas,
        "cia_aerea": cia_aerea,
        "custo_reais": custo_reais,
        "ultimo_alerta": agora_iso,
        "total_alertas": historico.get(chave, {}).get("total_alertas", 0) + 1,
    }
    salvar_historico(historico)
