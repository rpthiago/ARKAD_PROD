# -*- coding: utf-8 -*-
"""
estrategia_lay_2x2.py — Módulo Operacional para Lay 2x2 Correct Score (1 por Horário)
Regras Canônicas:
- Odd executável de Lay 2x2 na Betfair (Odd_CS_2x2_Lay) entre 8.0 e 15.0
- Filtro Anti-Caos: Odd Over 2.5 >= 1.55
- Desempate por Horário: Escolhe o jogo com menor Odd de Lay (menor liability / risco de cauda)
- Comissão 4.5%
"""

import os
import numpy as np
import pandas as pd

def avaliar_jogos_lay_2x2_grade(df_dia):
    """
    Avalia a grade diária e seleciona exatamente 1 jogo por horário no Lay 2x2
    seguindo a regra de menor odd de lay (menor liability).
    """
    if df_dia is None or df_dia.empty:
        return []
        
    candidatos = []
    COMMISSION = 0.045
    
    for _, row in df_dia.iterrows():
        odd_lay_2x2 = float(row.get('Odd_CS_2x2_Lay', 0.0) or 0.0)
        odd_o25 = float(row.get('Odd_Over25_FT_Back', 2.0) or 2.0)
        
        # Filtros canônicos
        if odd_lay_2x2 < 8.0 or odd_lay_2x2 > 15.0:
            continue
        if odd_o25 < 1.55: # Descarta jogos com tendência extrema de chuva de gols
            continue
            
        hora = str(row.get('Time', row.get('Hora', '15:00')))[:2] # Bloco de hora
        be_wr = (odd_lay_2x2 - 1.0) / (odd_lay_2x2 - COMMISSION)
        
        candidatos.append({
            'Home': row.get('Home'),
            'Away': row.get('Away'),
            'League': row.get('League', 'N/A'),
            'Hora': str(row.get('Time', row.get('Hora', '15:00'))),
            'Bloco_Hora': hora,
            'Odd_Lay': odd_lay_2x2,
            'Break_Even': be_wr,
            'raw_row': row.to_dict()
        })
        
    if not candidatos:
        return []
        
    df_cand = pd.DataFrame(candidatos)
    # Selecionar exatamente 1 jogo por bloco de horário com a MENOR ODD DE LAY
    selecionados = df_cand.sort_values('Odd_Lay', ascending=True).groupby('Bloco_Hora').first().reset_index()
    
    resultados = []
    for _, r in selecionados.iterrows():
        resultados.append({
            'aplica': True,
            'metodo': 'Lay 2x2 Correct Score (Menor Liability)',
            'mercado': 'Correct Score (2x2)',
            'lado': 'lay',
            'home': r['Home'],
            'away': r['Away'],
            'league': r['League'],
            'hora': r['Hora'],
            'odd_lay': r['Odd_Lay'],
            'prob_estimada': 0.953, # WR histórica 95.35%
            'break_even_wr': r['Break_Even'],
            'ev': 0.0386,
            'ev_pct': '+3.9%',
            'motivo': 'Jogo de menor liability selecionado para o horário'
        })
        
    return resultados
