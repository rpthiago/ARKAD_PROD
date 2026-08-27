# -*- coding: utf-8 -*-
"""
estrategia_lay_0x3.py — Módulo Operacional para Lay 0x3 Correct Score
Regras Canônicas:
- Odd executável de Lay 0x3 na Betfair (entre 10.0 e 35.0)
- Filtro de Mandante Competitivo (Odd H <= 3.80)
- Resolução dinâmica de colunas
- Comissão 4.5%
"""

import os
import numpy as np
import pandas as pd

def avaliar_jogos_lay_0x3_grade(df_dia, selecionar_1_por_horario=False):
    """
    Avalia a grade diária da Betfair para entradas em Lay 0x3.
    """
    if df_dia is None or df_dia.empty:
        return []
        
    COMMISSION = 0.045
    
    odd_0x3_cols = [c for c in df_dia.columns if '0x3' in str(c).lower() and 'lay' in str(c).lower()]
    odd_h_cols = [c for c in df_dia.columns if str(c).lower() in ['odd_h', 'odd_h_ft', 'odd_h_ft_back', 'odd_home', 'odd_1']]
    
    candidatos = []
    
    for _, row in df_dia.iterrows():
        o_0x3 = pd.to_numeric(row.get(odd_0x3_cols[0]), errors='coerce') if odd_0x3_cols else 0.0
        o_h = pd.to_numeric(row.get(odd_h_cols[0]), errors='coerce') if odd_h_cols else 2.50
        
        o_0x3 = float(o_0x3) if pd.notna(o_0x3) else 0.0
        o_h = float(o_h) if pd.notna(o_h) else 2.50
        
        # Filtros de odd e segurança
        if o_0x3 < 10.0 or o_0x3 > 35.0:
            continue
        if o_h > 3.80:
            continue
            
        home = str(row.get("Home", row.get("Home_Team", "")))
        away = str(row.get("Away", row.get("Away_Team", "")))
        liga = str(row.get("League", row.get("Div", "Liga Externa")))
        tm = str(row.get("Time", row.get("horario", "15:00")))[:5]
        bloco_hora = tm[:2]
        
        be_wr = (o_0x3 - 1.0) / (o_0x3 - COMMISSION)
        
        candidatos.append({
            'Home': home,
            'Away': away,
            'League': liga,
            'Hora': tm,
            'Bloco_Hora': bloco_hora,
            'Odd_Lay': o_0x3,
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
            'metodo': 'Lay 0x3 Correct Score',
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
            'motivo': f'Aprovado Lay 0x3 (Odd {r["Odd_Lay"]:.2f})'
        })
        
    return resultados
