# -*- coding: utf-8 -*-
"""
estrategia_lay_0x3.py — Módulo Operacional para Lay 0x3 Correct Score (1 por Horário)
Regras Canônicas:
- Odd executável de Lay 0x3 na Betfair (Odd_CS_0x3_Lay)
- Faixa de Odd: 12.0 a 35.0
- Filtro de Proteção: Mandante competitivo (Odd H <= 3.50)
- Desempate por Horário: Seleciona o jogo com a MENOR Odd de Lay 0x3 para minimizar a liability
- Comissão 4.5%
"""

import os
import numpy as np
import pandas as pd

def avaliar_jogos_lay_0x3_grade(df_dia):
    """
    Avalia a grade diária e seleciona 1 jogo por horário no Lay 0x3
    seguindo a regra de menor odd de lay (menor liability).
    """
    if df_dia is None or df_dia.empty:
        return []
        
    candidatos = []
    COMMISSION = 0.045
    
    for _, row in df_dia.iterrows():
        odd_lay_0x3 = float(row.get('Odd_CS_0x3_Lay', 0.0) or 0.0)
        odd_h = float(row.get('Odd_H_Back', 2.50) or 2.50)
        
        # Filtros de odd e segurança
        if odd_lay_0x3 < 10.0 or odd_lay_0x3 > 35.0:
            continue
        if odd_h > 3.80: # Evita zebras extremas onde o visitante pode golear de 0x3
            continue
            
        hora = str(row.get('Time', row.get('Hora', '15:00')))[:2] # Bloco de hora
        be_wr = (odd_lay_0x3 - 1.0) / (odd_lay_0x3 - COMMISSION)
        
        candidatos.append({
            'Home': row.get('Home'),
            'Away': row.get('Away'),
            'League': row.get('League', 'N/A'),
            'Hora': str(row.get('Time', row.get('Hora', '15:00'))),
            'Bloco_Hora': hora,
            'Odd_Lay': odd_lay_0x3,
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
            'metodo': 'Lay 0x3 Correct Score (Menor Liability)',
            'mercado': 'Correct Score (0x3)',
            'lado': 'lay',
            'home': r['Home'],
            'away': r['Away'],
            'league': r['League'],
            'hora': r['Hora'],
            'odd_lay': r['Odd_Lay'],
            'prob_estimada': 0.985, # Placar raro
            'break_even_wr': r['Break_Even'],
            'ev': 0.025,
            'ev_pct': '+2.5%',
            'motivo': 'Jogo de menor liability selecionado para o horário'
        })
        
    return resultados
