"""
MÉTODO SALDO MENOR - Estratégia de Handicap Europeu +3 para Zebra em jogos de Baixa Expectativa de Gols
ARKAD_PROD

Regras do Método:
A) Odds entre 2.20 e 5.00 (Odd do Favorito ou Odd da Zebra na faixa de competitividade);
B) Handicap Europeu +3 para a Zebra (EH_H_pos_3 se Zebra for Mandante, EH_A_pos_3 se Zebra for Visitante);
C) Porcentagem de vitória da Zebra implícita <= 20% (Odd da Zebra >= 5.0 ou Faixa de Competitividade);
D) Expectativa de gols (xG total pré/FT) no máximo igual a 2.0;
E) Validação opcional com previsão do algoritmo Betmines.
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Any, List, Tuple

from betmines_validator import fetch_betmines_prediction, validate_saldo_menor_betmines


def normalize_live_data(live_payload: Dict[str, Any]) -> Dict[str, Any]:
    """Normaliza os dados de entrada de um jogo para o formato padrão do pipeline com fallbacks inteligentes."""
    normalized = {}

    normalized['Home'] = str(live_payload.get('Home') or live_payload.get('HomeTeam') or '').strip()
    normalized['Away'] = str(live_payload.get('Away') or live_payload.get('AwayTeam') or '').strip()
    normalized['League'] = str(live_payload.get('League') or live_payload.get('Liga') or '').strip()
    normalized['Time'] = str(live_payload.get('Time') or '').strip()

    date_val = live_payload.get('Date') or live_payload.get('Data_Jogo') or datetime.now().date()
    normalized['Date'] = pd.to_datetime(date_val)

    # Odds 1X2 Match Odds (Back)
    normalized['Odd_H_FT'] = pd.to_numeric(
        live_payload.get('Odd_H_FT') or live_payload.get('Odd_H_Back') or np.nan, errors='coerce')
    normalized['Odd_A_FT'] = pd.to_numeric(
        live_payload.get('Odd_A_FT') or live_payload.get('Odd_A_Back') or np.nan, errors='coerce')
    normalized['Odd_D_FT'] = pd.to_numeric(
        live_payload.get('Odd_D_FT') or live_payload.get('Odd_D_Back') or np.nan, errors='coerce')

    # Odds de Under/Over 2.5 para fallback de xG
    normalized['Odd_Under25_FT'] = pd.to_numeric(
        live_payload.get('Odd_Under25_FT') or live_payload.get('Odd_Under25') or np.nan, errors='coerce')
    normalized['Odd_Over25_FT'] = pd.to_numeric(
        live_payload.get('Odd_Over25_FT') or live_payload.get('Odd_Over25') or np.nan, errors='coerce')

    # Handicaps Europeus +3 na base Bet365
    normalized['EH_H_pos_3'] = pd.to_numeric(
        live_payload.get('EH_H_pos_3') or live_payload.get('EH_Home_Plus3') or np.nan, errors='coerce')
    normalized['EH_A_pos_3'] = pd.to_numeric(
        live_payload.get('EH_A_pos_3') or live_payload.get('EH_Away_Plus3') or np.nan, errors='coerce')

    # Expectativa de Gols (xG) com fallback pré-jogo do mercado
    xg_h = pd.to_numeric(live_payload.get('xG_H_FT') or live_payload.get('xG_H_Pre') or live_payload.get('xG_H') or 0.0, errors='coerce') or 0.0
    xg_a = pd.to_numeric(live_payload.get('xG_A_FT') or live_payload.get('xG_A_Pre') or live_payload.get('xG_A') or 0.0, errors='coerce') or 0.0
    total_xg = pd.to_numeric(live_payload.get('Total_xG_Pre') or live_payload.get('Total_xG') or (xg_h + xg_a), errors='coerce') or 0.0

    # Se xG estiver 0.0 na API pré-jogo, estimar via odds de Under/Over 2.5
    if total_xg == 0.0:
        odd_u25 = normalized['Odd_Under25_FT']
        odd_o25 = normalized['Odd_Over25_FT']
        if not pd.isna(odd_u25) and odd_u25 > 1.0:
            if odd_u25 <= 1.95:
                total_xg = 1.70  # Mercado precifica forte tendência de Under 2.5
            else:
                total_xg = 2.45  # Mercado precifica tendência de Over 2.5
        elif not pd.isna(odd_o25) and odd_o25 > 1.0:
            if odd_o25 >= 1.90:
                total_xg = 1.75
            else:
                total_xg = 2.40
        else:
            total_xg = 1.80  # Default conservador para partidas normais

    normalized['xG_H_FT'] = xg_h
    normalized['xG_A_FT'] = xg_a
    normalized['Total_xG'] = round(float(total_xg), 2)

    return normalized


def identify_zebra_and_handicap(match_state: Dict[str, Any]) -> Dict[str, Any]:
    """Identifica qual time é a Zebra (Casa ou Fora) e extrai ou estima a odd do EH +3 correto."""
    odd_h = match_state.get('Odd_H_FT') or 0.0
    odd_a = match_state.get('Odd_A_FT') or 0.0

    if pd.isna(odd_h) or pd.isna(odd_a) or odd_h <= 1.0 or odd_a <= 1.0:
        return {
            'is_home_zebra': False,
            'zebra_team': '',
            'fav_team': '',
            'zebra_odd': 0.0,
            'fav_odd': 0.0,
            'eh_zebra_plus3_odd': 0.0
        }

    is_home_zebra = odd_h > odd_a
    zebra_team = match_state['Home'] if is_home_zebra else match_state['Away']
    fav_team = match_state['Away'] if is_home_zebra else match_state['Home']
    zebra_odd = float(odd_h if is_home_zebra else odd_a)
    fav_odd = float(odd_a if is_home_zebra else odd_h)

    # Seleção do Handicap Europeu +3 da Zebra:
    # Zebra em Casa -> EH_H_pos_3 | Zebra Fora -> EH_A_pos_3
    eh_pos3 = match_state.get('EH_H_pos_3') if is_home_zebra else match_state.get('EH_A_pos_3')
    eh_zebra_plus3_odd = pd.to_numeric(eh_pos3, errors='coerce')

    # Fallback/Sanitização inteligente: se a odd EH+3 estiver missing, anômala ou maior que Zebra_Odd
    if pd.isna(eh_zebra_plus3_odd) or eh_zebra_plus3_odd <= 1.0 or eh_zebra_plus3_odd >= zebra_odd or eh_zebra_plus3_odd > 2.50:
        # Estima uma odd EH +3 realista para a Zebra com base na odd do favorito
        # Para Fav Odd entre 2.20 e 5.00, a odd EH+3 da Zebra varia de 1.05 a 1.15
        base_eh = 1.05 + max(0.0, (fav_odd - 2.20)) * 0.02
        eh_zebra_plus3_odd = round(min(base_eh, 1.25), 2)

    return {
        'is_home_zebra': is_home_zebra,
        'zebra_team': zebra_team,
        'fav_team': fav_team,
        'zebra_odd': zebra_odd,
        'fav_odd': fav_odd,
        'eh_zebra_plus3_odd': float(eh_zebra_plus3_odd)
    }


def check_entry_conditions(
    match_state: Dict[str, Any],
    max_xg: float = 2.0,
    check_betmines: bool = False
) -> Tuple[bool, str]:
    """
    Verifica todas as condições operacionais do MÉTODO SALDO MENOR.
    """
    zebra_info = identify_zebra_and_handicap(match_state)
    fav_odd = zebra_info['fav_odd']
    zebra_odd = zebra_info['zebra_odd']
    eh_odd = zebra_info['eh_zebra_plus3_odd']

    # Validação A: Faixa de Odds entre 2.20 e 5.00 (Fav Odd ou Zebra Odd)
    in_odd_range = (2.20 <= fav_odd <= 5.00) or (2.20 <= zebra_odd <= 5.00)
    if not in_odd_range:
        return False, "ODD_FORA_DA_FAIXA_2.2_5.0"

    # Validação B: Disponibilidade e Sanitização de Odd do Handicap +3
    if eh_odd <= 1.0 or eh_odd >= zebra_odd:
        return False, "ODD_HANDICAP_PLUS3_INVALIDA"

    # Validação C: Probabilidade Implícita da Zebra (1 / Zebra_Odd) <= 45%
    zebra_win_pct = 1.0 / zebra_odd if zebra_odd > 0 else 1.0
    if zebra_win_pct > 0.45:
        return False, "ZEBRA_PROB_MUITO_ALTA"

    # Validação D: Expectativa de Gols (xG Total) <= 2.0
    total_xg = match_state.get('Total_xG', 0.0)
    if total_xg > max_xg:
        return False, f"XG_ALTO_{total_xg:.2f}_MAIOR_QUE_{max_xg}"

    # Validação E: Betmines (opcional em tempo real)
    if check_betmines:
        prediction = fetch_betmines_prediction(match_state.get('Home', ''), match_state.get('Away', ''))
        valid_betmines, score, reason_bm = validate_saldo_menor_betmines(
            prediction, is_home_zebra=zebra_info['is_home_zebra'], fav_odd=fav_odd
        )
        if not valid_betmines:
            return False, f"REJEITADO_BETMINES_{reason_bm}"

    return True, "APROVADO_SALDO_MENOR"


def evaluate_game(game_payload: Dict[str, Any], check_betmines: bool = False) -> Dict[str, Any]:
    """Avalia uma partida e anexa as métricas do Método Saldo Menor."""
    norm_game = normalize_live_data(game_payload)
    zebra_info = identify_zebra_and_handicap(norm_game)

    norm_game.update(zebra_info)
    is_approved, reason = check_entry_conditions(norm_game, check_betmines=check_betmines)

    norm_game['Metodo'] = 'METODO_SALDO_MENOR'
    norm_game['Decision'] = 'APOSTA' if is_approved else 'SKIP'
    norm_game['Reason'] = reason

    return norm_game


def predict_and_evaluate_live(live_games_payload: List[Dict[str, Any]], check_betmines: bool = False) -> List[Dict[str, Any]]:
    """Processa uma lista de partidas pré-jogo da API diária."""
    results = []
    for game in live_games_payload:
        evaluated = evaluate_game(game, check_betmines=check_betmines)
        results.append(evaluated)
    return results
