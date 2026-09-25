# -*- coding: utf-8 -*-
"""
estrategia_lay_3x0.py — Módulo Operacional para Lay 3x0 Correct Score (Espelho Assimétrico do Lay 0x3 Top 3)
Filtros Estruturais (Validados em 3 Anos Limpos N=1.444, WR 97.16%, IC95% [+0.09%, +1.87%] @ 5% comm):
1. Odd Under 2.5 FT <= 1.75 (Ajuste de assimetria de mando de campo vs 2.10 do 0x3)
2. Odd Lay 3x0 entre 14.0 e 35.0
3. Odd Mandante >= 1.80 (Elimina favoritos mandantes que podem aplicar 3x0)
- Trava Top 3 Menor Odd do Dia com desempate por horários distintos
- Comissão 5.0%
"""

import numpy as np
import pandas as pd


def avaliar_jogos_lay_3x0_grade(df_dia, selecionar_1_por_horario=False, top_n=3, u25_max=1.75, odd_h_min=1.80):
    """
    Avalia a grade diária da Betfair para entradas em Lay 3x0 com Trava Top 3 Menor Odd e Horário Distinto.
    """
    if df_dia is None or df_dia.empty:
        return []

    COMMISSION = 0.05
    candidatos = []

    for _, row in df_dia.iterrows():
        odd_h = float(row.get('Odd_H_Back') or row.get('Odd_H_FT_Back') or row.get('Odd_H_FT') or row.get('Odd_H') or 0.0)
        odd_u25 = float(row.get('Odd_Under25_FT_Back') or row.get('Odd_Under25_FT') or row.get('Odd_Under25') or 0.0)
        odd_3x0 = float(row.get('Odd_CS_3x0_Lay') or row.get('Odd_CS_3x0') or 0.0)

        if 0.0 < odd_u25 <= u25_max and 14.0 <= odd_3x0 <= 35.0 and odd_h >= odd_h_min:
            home = str(row.get("Home", row.get("Home_Team", "")))
            away = str(row.get("Away", row.get("Away_Team", "")))
            liga = str(row.get("League", row.get("Div", "")))
            tm = str(row.get("Time", row.get("horario", "15:00")))[:5]
            bloco_hora = tm[:2]

            be_wr = (odd_3x0 - 1.0) / (odd_3x0 - COMMISSION)

            candidatos.append({
                'Home': home,
                'Away': away,
                'League': liga,
                'Hora': tm,
                'Bloco_Hora': bloco_hora,
                'Odd_Lay': odd_3x0,
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

    if top_n is not None and top_n > 0 and len(df_cand) > top_n:
        df_sorted = df_cand.sort_values('Odd_Lay', ascending=True).copy()
        selecionados = []
        horarios_usados = set()

        odds_unicas = sorted(df_sorted['Odd_Lay'].unique())
        for o in odds_unicas:
            grupo_odd = df_sorted[df_sorted['Odd_Lay'] == o]

            for _, row_item in grupo_odd.iterrows():
                if len(selecionados) == top_n:
                    break
                h = row_item['Hora']
                if h not in horarios_usados:
                    selecionados.append(row_item)
                    horarios_usados.add(h)
            if len(selecionados) == top_n:
                break

            for _, row_item in grupo_odd.iterrows():
                if len(selecionados) == top_n:
                    break
                ja_add = any(s.name == row_item.name for s in selecionados)
                if not ja_add:
                    selecionados.append(row_item)
            if len(selecionados) == top_n:
                break

        df_cand = pd.DataFrame(selecionados).reset_index(drop=True)

    saida = []
    for _, r in df_cand.iterrows():
        saida.append({
            "hora": r["Hora"],
            "league": r["League"],
            "home": r["Home"],
            "away": r["Away"],
            "odd_lay": float(r["Odd_Lay"]),
            "break_even": float(r["Break_Even"]),
        })
    return saida
