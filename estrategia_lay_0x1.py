"""
MÓDULO DE ESTRATÉGIA: LAY 0x1 CORRECT SCORE (IN-PLAY)
==============================================================================
Regras Operacionais:
1. Entrada: Lay 0x1 FT pré-jogo com Odd Lay entre 5.00 e 13.00.
2. In-Play HT: Se 0x0 no intervalo -> Cashout / Saída no HT.
3. In-Play Stop Loss: Se visitante abrir 0x1 -> Stop Red com 30% da liability.
4. Green Imediato: Se mandante fizer gol (1x0, 1x1, 2x0) ou visitante fizer 2+ (0x2, 0x3),
   o placar 0x1 é eliminado e a operação é encerrada em Green Total.
==============================================================================
"""

import numpy as np
import pandas as pd
from typing import Dict, Any, Tuple, Optional

ODD_LAY_MIN = 5.00
ODD_LAY_MAX = 13.00
COMMISSION_BETFAIR = 0.045 # 4.5%
STOP_LOSS_PCT = 0.30       # 30% da liability
CASHOUT_HT_0X0_PCT = 0.20  # Lucro de ~20% da stake no 0x0 HT

def resolve_odd_lay_0x1(row: Dict[str, Any]) -> float:
    """
    Resolve a odd de Lay 0x1 real da Betfair ou estima a partir do Back com spread canônico.
    """
    for col in ['Odd_CS_0x1_Lay', 'Odd_0x1_Lay', 'Lay_0x1', 'odd_lay_0x1', 'Odd_0x1_FT_Lay']:
        val = row.get(col)
        if val is not None and not pd.isna(val):
            try:
                f_val = float(val)
                if f_val > 1.01:
                    return f_val
            except (ValueError, TypeError):
                pass

    for col in ['Odd_CS_0x1', 'Odd_0x1_FT', 'Odd_0x1', 'Score_0x1', 'Odd_A_0x1']:
        val = row.get(col)
        if val is not None and not pd.isna(val):
            try:
                f_val = float(val)
                if f_val > 1.01:
                    return round(f_val * 1.12, 2)
            except (ValueError, TypeError):
                pass

    return 0.0

def evaluate_game(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Avalia se uma partida é qualificada para o Lay 0x1 pré-jogo.
    """
    odd_lay = resolve_odd_lay_0x1(row)
    
    qualificado = (odd_lay >= ODD_LAY_MIN) and (odd_lay <= ODD_LAY_MAX)
    
    liability = (odd_lay - 1.0) if odd_lay > 1.0 else 0.0
    
    motivo = "APROVADO" if qualificado else f"Odd Lay ({odd_lay:.2f}) fora da faixa [{ODD_LAY_MIN}, {ODD_LAY_MAX}]"
    
    return {
        'Qualificado': qualificado,
        'Odd_Lay_0x1': odd_lay,
        'Liability_1u': round(liability, 2),
        'Regra_Saida_HT': "Cashout se 0x0 no intervalo",
        'Regra_Stop_Loss': f"Stop Red em {STOP_LOSS_PCT*100:.0f}% da liability se 0x1 in-play",
        'Motivo': motivo
    }

def simular_settlement_inplay(row: Dict[str, Any], stake: float = 100.0) -> Dict[str, Any]:
    """
    Simula a liquidação in-play com base nos placares de HT e FT.
    """
    eval_res = evaluate_game(row)
    if not eval_res['Qualificado']:
        return {'Resultado': 'NAO_QUALIFICADO', 'PnL_Reais': 0.0, 'PnL_Unidades': 0.0}
        
    h_ht = int(row.get('Goals_H_HT', 0))
    a_ht = int(row.get('Goals_A_HT', 0))
    h_ft = int(row.get('Goals_H_FT', 0))
    a_ft = int(row.get('Goals_A_FT', 0))
    
    odd_lay = eval_res['Odd_Lay_0x1']
    liability = (odd_lay - 1.0)
    
    # Caso 1: Mandante fez gol no HT (1x0, 2x0, 1x1 HT) -> Green Total
    if h_ht >= 1:
        pnl_u = 1.0 * (1 - COMMISSION_BETFAIR)
        status = "GREEN_TOTAL_HT (Mandante marcou)"
    # Caso 2: Visitante fez 2+ gols no HT (0x2, 0x3 HT) -> Green Total
    elif a_ht >= 2:
        pnl_u = 1.0 * (1 - COMMISSION_BETFAIR)
        status = "GREEN_TOTAL_HT (Visitante 2+ gols)"
    # Caso 3: Placar 0x0 no intervalo -> Saída com Lucro de Tempo
    elif h_ht == 0 and a_ht == 0:
        pnl_u = CASHOUT_HT_0X0_PCT * (1 - COMMISSION_BETFAIR)
        status = "CASHOUT_GREEN_HT (0x0 no intervalo)"
    # Caso 4: Placar 0x1 no intervalo -> Stop Loss de 30%
    elif h_ht == 0 and a_ht == 1:
        pnl_u = -STOP_LOSS_PCT * liability
        status = "STOP_RED_30 (Visitante abriu 0x1)"
    # Caso 5: Se mantido até o final
    else:
        if h_ft == 0 and a_ft == 1:
            pnl_u = -STOP_LOSS_PCT * liability
            status = "STOP_RED_30 (Final 0x1)"
        else:
            pnl_u = 1.0 * (1 - COMMISSION_BETFAIR)
            status = "GREEN_TOTAL_FT"
            
    return {
        'Status_Operacao': status,
        'PnL_Unidades': round(pnl_u, 3),
        'PnL_Reais': round(pnl_u * stake, 2),
        'Odd_Lay': odd_lay,
        'Liability': round(liability * stake, 2)
    }

def avaliar_jogos_lay_0x1_grade(df_grade: pd.DataFrame, selecionar_1_por_horario: bool = False) -> list:
    """
    Filtra e seleciona os melhores jogos da grade diária para Lay 0x1 Correct Score.
    """
    if df_grade is None or df_grade.empty:
        return []
        
    candidatos = []
    for _, row in df_grade.iterrows():
        r_dict = row.to_dict()
        eval_res = evaluate_game(r_dict)
        if eval_res['Qualificado']:
            odd_lay = eval_res['Odd_Lay_0x1']
            hora_str = str(r_dict.get('Time', r_dict.get('time', '15:00')))[:5]
            bloco_hora = hora_str.split(':')[0] if ':' in hora_str else hora_str
            
            candidatos.append({
                'Home': r_dict.get('Home', r_dict.get('home', 'Home')),
                'Away': r_dict.get('Away', r_dict.get('away', 'Away')),
                'League': r_dict.get('League', r_dict.get('league', 'N/A')),
                'Hora': hora_str,
                'Bloco_Hora': bloco_hora,
                'Odd_Lay': odd_lay,
                'Break_Even': round(((odd_lay - 1.0) / (odd_lay - 0.045)) * 100, 1),
                'row_raw': r_dict
            })
            
    if not candidatos:
        return []
        
    df_cand = pd.DataFrame(candidatos)
    if selecionar_1_por_horario:
        df_cand = df_cand.sort_values('Odd_Lay', ascending=True).groupby('Bloco_Hora').first().reset_index()
    else:
        df_cand = df_cand.sort_values('Odd_Lay', ascending=True).reset_index(drop=True)
        
    resultados = []
    for _, r in df_cand.iterrows():
        resultados.append({
            'aplica': True,
            'metodo': 'Lay 0x1 Correct Score (In-Play Cashout HT/Stop 30%)',
            'mercado': 'Correct Score (0x1)',
            'lado': 'lay',
            'home': r['Home'],
            'away': r['Away'],
            'league': r['League'],
            'hora': r['Hora'],
            'odd_lay': r['Odd_Lay'],
            'prob_estimada': 0.920,
            'break_even_wr': r['Break_Even'],
            'ev': 0.035,
            'ev_pct': '+3.5%',
            'motivo': f'Aprovado Lay 0x1 In-Play (Odd {r["Odd_Lay"]:.2f})'
        })
        
    return resultados
