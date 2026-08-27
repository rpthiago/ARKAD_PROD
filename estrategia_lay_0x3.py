# -*- coding: utf-8 -*-
"""
estrategia_lay_0x3.py — Módulo Operacional para Lay 0x3 Correct Score (xG Protected)
Filtros de Elite da Página 16:
1. Odd Under 2.5 FT <= 2.10 (Tendência Under)
2. Odd Lay 0x3 entre 14.0 e 35.0
3. Odd Visitante >= 1.85 (Elimina super favoritos como Benfica/Atalanta que podem meter 0x3)
4. xG Visitante <= 1.10 (Ataque inofensivo fora de casa)
- Comissão 4.5%
"""

import os
import numpy as np
import pandas as pd

def avaliar_jogos_lay_0x3_grade(df_dia, selecionar_1_por_horario=False):
    """
    Avalia a grade diária da Betfair para entradas em Lay 0x3 com Filtros de Proteção xG.
    """
    if df_dia is None or df_dia.empty:
        return []
        
    COMMISSION = 0.045
    candidatos = []
    
    for idx, row in df_dia.iterrows():
        odd_h = float(row.get('Odd_H_Back') or row.get('Odd_H_FT_Back') or row.get('Odd_H_FT') or row.get('Odd_H') or 0.0)
        odd_a = float(row.get('Odd_A_Back') or row.get('Odd_A_FT_Back') or row.get('Odd_A_FT') or row.get('Odd_A') or 0.0)
        odd_u25 = float(row.get('Odd_Under25_FT_Back') or row.get('Odd_Under25_FT') or row.get('Odd_Under25') or 0.0)
        odd_0x3 = float(row.get('Odd_CS_0x3_Lay') or row.get('Odd_CS_0x3') or 0.0)
        xg_a = float(row.get('A_xGF_r5') or row.get('Media_Gols_Pro_Visitante') or row.get('xG_A_FT') or 1.0)
        
        # Filtros de Elite da Página 16:
        # 1. Odd Under 2.5 <= 2.10
        # 2. Odd Lay 0x3 entre 14.0 e 35.0
        # 3. Odd Visitante >= 1.85 (elimina visitantes gigantes que podem golear)
        # 4. xG Visitante <= 1.10
        if 0.0 < odd_u25 <= 2.10 and 14.0 <= odd_0x3 <= 35.0 and (odd_a >= 1.85 or odd_a == 0.0) and xg_a <= 1.10:
            home = str(row.get("Home", row.get("Home_Team", "")))
            away = str(row.get("Away", row.get("Away_Team", "")))
            liga = str(row.get("League", row.get("Div", "Liga Externa")))
            tm = str(row.get("Time", row.get("horario", "15:00")))[:5]
            bloco_hora = tm[:2]
            
            be_wr = (odd_0x3 - 1.0) / (odd_0x3 - COMMISSION)
            
            candidatos.append({
                'Home': home,
                'Away': away,
                'League': liga,
                'Hora': tm,
                'Bloco_Hora': bloco_hora,
                'Odd_Lay': odd_0x3,
                'Break_Even': be_wr,
                'raw_row': row.to_dict()
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
            'metodo': 'Lay 0x3 Correct Score (xG Protected)',
            'mercado': 'Correct Score (0x3)',
            'lado': 'lay',
            'home': r['Home'],
            'away': r['Away'],
            'league': r['League'],
            'hora': r['Hora'],
            'odd_lay': r['Odd_Lay'],
            'prob_estimada': 0.985,
            'break_even_wr': r['Break_Even'],
            'ev': 0.025,
            'ev_pct': '+2.5%',
            'motivo': f'Aprovado Lay 0x3 xG Protected (Odd {r["Odd_Lay"]:.2f})'
        })
        
    return resultados
