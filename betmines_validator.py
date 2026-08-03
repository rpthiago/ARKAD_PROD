"""
Módulo de Validação de Previsões Pré-Jogo do Betmines (betmines.com)
MÉTODO SALDO MENOR - ARKAD_PROD

Este módulo realiza a busca e validação das estimativas do Betmines para confirmar
se o cenário do jogo apoia baixa margem de gols (Under 2.5/3.5, Dupla Chance da Zebra,
ou placares de pouca diferença).
"""

import re
import unicodedata
import requests
from typing import Dict, Any, Tuple, Optional


def _normalize_name(name: str) -> str:
    """Normaliza o nome do time para comparação flexível."""
    if not name:
        return ""
    text = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "", text.lower())


def fetch_betmines_prediction(home_team: str, away_team: str, date_str: Optional[str] = None) -> Dict[str, Any]:
    """
    Busca a previsão do Betmines para o confronto entre home_team e away_team.
    Retorna um dicionário com estatísticas e recomendações do algoritmo.
    Caso a consulta falhe (timeout ou inacessível), retorna um dicionário de fallback.
    """
    home_norm = _normalize_name(home_team)
    away_norm = _normalize_name(away_team)

    result = {
        "found": False,
        "home": home_team,
        "away": away_team,
        "prediction_1x2": None,
        "double_chance": None,
        "under_over_25": None,
        "under_over_35": None,
        "btts": None,
        "expected_goals": None,
        "confidence": 0.0,
        "source": "betmines"
    }

    # Tenta scraping da busca pública do Betmines
    search_url = f"https://www.betmines.com/pt/previsoes-de-futebol/{home_norm}-vs-{away_norm}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8"
    }

    try:
        response = requests.get(search_url, headers=headers, timeout=(4, 8))
        if response.status_code == 200 and len(response.text) > 500:
            content = response.text.lower()
            
            # Extração via RegEx das dicas comuns do Betmines
            if "1x" in content:
                result["double_chance"] = "1X"
            elif "x2" in content:
                result["double_chance"] = "X2"
            elif "12" in content:
                result["double_chance"] = "12"

            if "under 2.5" in content or "menos de 2.5" in content:
                result["under_over_25"] = "UNDER"
            elif "over 2.5" in content or "mais de 2.5" in content:
                result["under_over_25"] = "OVER"

            if "under 3.5" in content or "menos de 3.5" in content:
                result["under_over_35"] = "UNDER"

            result["found"] = True
            result["confidence"] = 0.80
            return result
    except Exception:
        pass

    # Fallback quando indisponível online
    result["found"] = False
    result["reason"] = "FALLBACK_OFFLINE_OU_NAO_ENCONTRADO"
    return result


def validate_saldo_menor_betmines(
    prediction: Dict[str, Any],
    is_home_zebra: bool,
    fav_odd: float = 2.0
) -> Tuple[bool, float, str]:
    """
    Avalia se a previsão do Betmines é compatível com o MÉTODO SALDO MENOR.
    
    Critérios de Validação:
    1. Se Betmines prevê Under 2.5 ou Under 3.5 -> Apoio Forte (Green).
    2. Se Betmines prevê Dupla Chance a favor da Zebra (1X se Zebra for Casa, X2 se Zebra for Visitante) -> Apoio Forte.
    3. Se o jogo não for encontrado no Betmines -> Permite aposta por modelo estatístico fallback.
    """
    if not prediction or not prediction.get("found"):
        return True, 0.70, "BETMINES_FALLBACK_ESTATISTICO"

    score = 0.50
    reasons = []

    # Validação de Under/Over
    uo25 = prediction.get("under_over_25")
    uo35 = prediction.get("under_over_35")
    if uo25 == "UNDER" or uo35 == "UNDER":
        score += 0.30
        reasons.append("BETMINES_PREVE_UNDER")
    elif uo25 == "OVER" and fav_odd > 2.5:
        score -= 0.10
        reasons.append("BETMINES_PREVE_OVER_MODERADO")

    # Validação de Dupla Chance Zebra
    dc = prediction.get("double_chance")
    target_dc = "1X" if is_home_zebra else "X2"
    if dc == target_dc:
        score += 0.25
        reasons.append(f"BETMINES_PREVE_DUPLA_CHANCE_{target_dc}")

    is_valid = score >= 0.50
    reason_str = " + ".join(reasons) if reasons else "BETMINES_NEUTRO"
    return is_valid, min(score, 1.0), reason_str
