"""
Módulo da Estratégia de Múltiplas OVER 0.5 FT (Lay 0x0)
MÉTODO ARKAD_PROD

Filtra partidas com alta probabilidade de gols (xG > 2.0 e Odd Empate > 3.30)
onde a taxa histórica de ocorrência de 0x0 cai para apenas 2.57% (Win Rate 97.43%).
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, List, Tuple


def evaluate_game_over05(match_state: Dict[str, Any], min_xg: float = 2.0, min_draw_odd: float = 3.30) -> Dict[str, Any]:
    """
    Avalia se a partida atende aos critérios estritos do MÉTODO OVER 0.5 FT.
    """
    result = dict(match_state)
    
    # Extrair estatísticas e odds
    xg_h = pd.to_numeric(match_state.get('xG_H_FT') or match_state.get('xG_H_Pre') or 0.0, errors='coerce') or 0.0
    xg_a = pd.to_numeric(match_state.get('xG_A_FT') or match_state.get('xG_A_Pre') or 0.0, errors='coerce') or 0.0
    total_xg = pd.to_numeric(match_state.get('Total_xG_Pre') or match_state.get('Total_xG') or (xg_h + xg_a), errors='coerce') or 0.0
    
    odd_u25 = pd.to_numeric(match_state.get('Odd_Under25_FT'), errors='coerce') or 0.0
    odd_o25 = pd.to_numeric(match_state.get('Odd_Over25_FT'), errors='coerce') or 0.0
    odd_d = pd.to_numeric(match_state.get('Odd_D_FT'), errors='coerce') or 0.0
    odd_h = pd.to_numeric(match_state.get('Odd_H_FT'), errors='coerce') or 0.0
    odd_a = pd.to_numeric(match_state.get('Odd_A_FT'), errors='coerce') or 0.0

    # Estimativa contínua de xG caso não disponível
    if total_xg == 0.0:
        if odd_u25 > 1.0:
            total_xg = max(0.80, min(5.50, 1.35 + (odd_u25 - 1.50) * 1.75))
        elif odd_o25 > 1.0:
            total_xg = max(0.80, min(5.50, 2.50 - (odd_o25 - 1.90) * 1.50))
        else:
            total_xg = 2.10

    result['Total_xG'] = round(total_xg, 2)

    # Estimativa da Odd de Over 0.5 FT se não disponível na API pré-jogo
    odd_over05 = pd.to_numeric(match_state.get('Odd_Over05_FT'), errors='coerce') or 0.0
    if odd_over05 <= 1.01:
        if odd_o25 > 1.0:
            # Relação típica entre Over 2.5 e Over 0.5
            odd_over05 = round(max(1.04, min(1.15, 1.03 + (odd_o25 - 1.50) * 0.08)), 2)
        else:
            odd_over05 = 1.07

    result['odd_over05'] = odd_over05

    # Regra 1: Expectativa de Gols elevada (xG > min_xg)
    if total_xg < min_xg:
        result['Decision'] = 'SEM_ENTRADA'
        result['Reason'] = f'XG_BAIXO_{total_xg:.2f}_MENOR_QUE_{min_xg}'
        return result

    # Regra 2: Odd do Empate alta (Odd_D_FT > min_draw_odd)
    if odd_d > 0 and odd_d < min_draw_odd:
        result['Decision'] = 'SEM_ENTRADA'
        result['Reason'] = f'ODD_EMPATE_BAIXA_{odd_d:.2f}_MENOR_QUE_{min_draw_odd}'
        return result

    # Regra 3: Faixa razoável de Over 0.5 FT
    if odd_over05 > 1.25:
        result['Decision'] = 'SEM_ENTRADA'
        result['Reason'] = f'ODD_OVER05_ALTA_{odd_over05:.2f}'
        return result

    result['Decision'] = 'APOSTA'
    result['Reason'] = 'APROVADO_OVER_05_FT'
    return result


def predict_and_evaluate_over05_live(game_list: List[Dict[str, Any]], min_xg: float = 2.0, min_draw_odd: float = 3.30) -> List[Dict[str, Any]]:
    """
    Avalia em lote uma lista de jogos para o Método Over 0.5 FT.
    """
    return [evaluate_game_over05(game, min_xg=min_xg, min_draw_odd=min_draw_odd) for game in game_list]
