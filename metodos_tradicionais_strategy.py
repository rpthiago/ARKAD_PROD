import pandas as pd
import numpy as np

def evaluate_game_traditional(game):
    """
    Avalia uma partida da grade Bet365/Betfair para as 6 estratégias tradicionais:
    1. Over 2.5 FT
    2. BTTS Yes (Ambas Marcam Sim)
    3. Lay Home (Dupla Chance X2 contra o Mandante)
    4. Lay Away (Dupla Chance 1X contra o Visitante)
    5. Over 0.5 HT
    6. Under 2.5 FT
    """
    odd_h = float(game.get('Odd_H_FT') or game.get('Odd_H_Back') or game.get('Odd_H') or 2.0)
    odd_d = float(game.get('Odd_D_FT') or game.get('Odd_D_Back') or game.get('Odd_D') or 3.2)
    odd_a = float(game.get('Odd_A_FT') or game.get('Odd_A_Back') or game.get('Odd_A') or 2.0)

    odd_o25 = float(game.get('Odd_Over25_FT_Back') or game.get('Odd_Over25_FT') or game.get('Odd_Over25') or 2.0)
    odd_u25 = float(game.get('Odd_Under25_FT_Back') or game.get('Odd_Under25_FT') or game.get('Odd_Under25') or 1.8)
    odd_o05ht = float(game.get('Odd_Over05_HT') or 1.35)

    odd_btts_y = float(game.get('Odd_BTTS_Yes_Back') or game.get('Odd_BTTS_Yes') or 1.9)
    odd_1x = float(game.get('Odd_DC_1X') or (1.0 / ((1.0/max(odd_h, 1.01)) + (1.0/max(odd_d, 1.01)))))
    odd_x2 = float(game.get('Odd_DC_X2') or (1.0 / ((1.0/max(odd_d, 1.01)) + (1.0/max(odd_a, 1.01)))))

    xg_h = float(game.get('xG_H_FT') or 1.2)
    xg_a = float(game.get('xG_A_FT') or 1.0)
    total_xg = float(game.get('Total_xG') or (xg_h + xg_a))

    results = {}

    # 1. OVER 2.5 FT
    o25_aprovado = (odd_o25 >= 1.75) and (total_xg >= 2.40) and (odd_d >= 3.30)
    results['Over_25_FT'] = {
        'Decision': 'APOSTA' if o25_aprovado else 'FORA',
        'Odd': round(odd_o25, 2),
        'Reason': 'APROVADO_OVER_25' if o25_aprovado else 'REJEITADO_XG_OU_ODD'
    }

    # 2. BTTS YES
    btts_aprovado = (odd_btts_y >= 1.70) and (xg_h >= 1.0) and (xg_a >= 1.0) and (total_xg >= 2.30)
    results['BTTS_Yes'] = {
        'Decision': 'APOSTA' if btts_aprovado else 'FORA',
        'Odd': round(odd_btts_y, 2),
        'Reason': 'APROVADO_BTTS_YES' if btts_aprovado else 'REJEITADO_XG_INSUFICIENTE'
    }

    # 3. LAY HOME (DC X2)
    lh_aprovado = (odd_h >= 2.20) and (odd_h <= 4.00) and (odd_a < odd_h) and (odd_d <= 3.60)
    results['Lay_Home'] = {
        'Decision': 'APOSTA' if lh_aprovado else 'FORA',
        'Odd': round(odd_x2, 2),
        'Reason': 'APROVADO_LAY_HOME_X2' if lh_aprovado else 'REJEITADO_FAVORITISMO_MANDANTE'
    }

    # 4. LAY AWAY (DC 1X)
    la_aprovado = (odd_a >= 2.20) and (odd_a <= 4.00) and (odd_h < odd_a) and (odd_d <= 3.60)
    results['Lay_Away'] = {
        'Decision': 'APOSTA' if la_aprovado else 'FORA',
        'Odd': round(odd_1x, 2),
        'Reason': 'APROVADO_LAY_AWAY_1X' if la_aprovado else 'REJEITADO_FAVORITISMO_VISITANTE'
    }

    # 5. OVER 0.5 HT
    o05ht_aprovado = (odd_o05ht >= 1.25) and (total_xg >= 2.20) and (odd_d >= 3.25)
    results['Over_05_HT'] = {
        'Decision': 'APOSTA' if o05ht_aprovado else 'FORA',
        'Odd': round(odd_o05ht, 2),
        'Reason': 'APROVADO_OVER_05_HT' if o05ht_aprovado else 'REJEITADO_PRESSAO_HT_BAIXA'
    }

    # 6. UNDER 2.5 FT
    u25_aprovado = (odd_u25 >= 1.65) and (total_xg <= 2.10) and (odd_d <= 3.40)
    results['Under_25_FT'] = {
        'Decision': 'APOSTA' if u25_aprovado else 'FORA',
        'Odd': round(odd_u25, 2),
        'Reason': 'APROVADO_UNDER_25' if u25_aprovado else 'REJEITADO_ALTO_XG'
    }

    return results
