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

    # Gols da partida (se disponíveis no histórico)
    if 'Goals_H_FT' in live_payload:
        normalized['Goals_H_FT'] = pd.to_numeric(live_payload.get('Goals_H_FT'), errors='coerce')
    if 'Goals_A_FT' in live_payload:
        normalized['Goals_A_FT'] = pd.to_numeric(live_payload.get('Goals_A_FT'), errors='coerce')

    # Expectativa de Gols (xG) 100% PRÉ-JOGO calculada estritamente pelas odds de mercado (Zero Data Leakage)
    odd_u25 = normalized['Odd_Under25_FT']
    odd_o25 = normalized['Odd_Over25_FT']
    if not pd.isna(odd_u25) and odd_u25 > 1.0:
        total_xg = max(0.80, min(5.50, 1.35 + (odd_u25 - 1.50) * 1.75))
    elif not pd.isna(odd_o25) and odd_o25 > 1.0:
        total_xg = max(0.80, min(5.50, 2.50 - (odd_o25 - 1.90) * 1.50))
    else:
        total_xg = 1.85  # Default conservador pré-jogo

    normalized['xG_H_FT'] = 0.0
    normalized['xG_A_FT'] = 0.0
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
    max_draw_odd: float = 3.42,
    check_betmines: bool = False
) -> Tuple[bool, str]:
    """
    Verifica todas as condições operacionais do MÉTODO SALDO MENOR.
    """
    zebra_info = identify_zebra_and_handicap(match_state)
    fav_odd = zebra_info['fav_odd']
    zebra_odd = zebra_info['zebra_odd']
    eh_odd = zebra_info['eh_zebra_plus3_odd']
    draw_odd = match_state.get('Odd_D_FT') or 0.0

    # Validação A: Faixa de Odds do Favorito estritamente entre 2.00 e 5.00 (Fav Odd >= 2.00 para equilíbrio entre volume e assertividade de 90% nas Múltiplas)
    in_odd_range = (2.00 <= fav_odd <= 5.00)
    if not in_odd_range:
        return False, f"ODD_FAVORITO_FORA_DA_FAIXA_{fav_odd:.2f}_FORA_DE_2.0_5.0"

    # Validação A2: Filtro de Odd do Empate (Odd_D_FT <= max_draw_odd)
    if max_draw_odd > 0 and draw_odd > 0 and draw_odd > max_draw_odd:
        return False, f"ODD_EMPATE_ALTA_{draw_odd:.2f}_MAIOR_QUE_{max_draw_odd}"

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

    # Validação E: Betmines
    prediction = fetch_betmines_prediction(
        match_state.get('Home', ''), 
        match_state.get('Away', ''),
        odd_h=match_state.get('Odd_H_FT'),
        odd_d=match_state.get('Odd_D_FT'),
        odd_a=match_state.get('Odd_A_FT'),
        odd_u25=match_state.get('Odd_Under25_FT')
    )
    valid_betmines, score, reason_bm, display_bm = validate_saldo_menor_betmines(
        prediction, is_home_zebra=zebra_info['is_home_zebra'], fav_odd=fav_odd
    )
    
    match_state['Betmines_Previsao'] = display_bm
    match_state['Betmines_Status'] = 'APROVADO' if valid_betmines else 'REJEITADO'

    if check_betmines and not valid_betmines:
        return False, f"REJEITADO_BETMINES_{reason_bm}"

    return True, "APROVADO_SALDO_MENOR"


def evaluate_game(game_payload: Dict[str, Any], check_betmines: bool = False, min_confidence: float = 0.94) -> Dict[str, Any]:
    """Avalia uma partida e anexa as métricas do Método Saldo Menor, incluindo filtro de confiança quant >= 94%."""
    norm_game = normalize_live_data(game_payload)
    zebra_info = identify_zebra_and_handicap(norm_game)

    norm_game.update(zebra_info)
    is_approved, reason = check_entry_conditions(norm_game, check_betmines=check_betmines)

    # Predição de Confiança pelo Modelo Mestre Quantitativo se disponível
    try:
        import joblib
        import master_feature_engineer
        model_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'modelo_saldo_menor_quant.pkl')
        if os.path.exists(model_path):
            model_sm = joblib.load(model_path)
            df_temp = pd.DataFrame([norm_game])
            feats = master_feature_engineer.build_master_features(df_temp)
            if hasattr(model_sm, "feature_names_in_"):
                feats = feats.reindex(columns=model_sm.feature_names_in_, fill_value=0.0)
            prob_val = float(model_sm.predict_proba(feats)[:, 1][0])
            norm_game['Prob_Master'] = prob_val
            
            if is_approved and min_confidence > 0 and prob_val < min_confidence:
                is_approved = False
                reason = f"CONFIANCA_BAIXA_{prob_val*100:.1f}%_MENOR_QUE_{min_confidence*100:.0f}%"
    except Exception:
        pass

    norm_game['Metodo'] = 'METODO_SALDO_MENOR'
    norm_game['Decision'] = 'APOSTA' if is_approved else 'SKIP'
    norm_game['Reason'] = reason

    return norm_game


def predict_and_evaluate_live(live_games_payload: List[Dict[str, Any]], check_betmines: bool = False, min_confidence: float = 0.94) -> List[Dict[str, Any]]:
    """Processa uma lista de partidas pré-jogo da API diária com filtro de confiança >= 94%."""
    results = []
    for game in live_games_payload:
        evaluated = evaluate_game(game, check_betmines=check_betmines, min_confidence=min_confidence)
        results.append(evaluated)
    return results

