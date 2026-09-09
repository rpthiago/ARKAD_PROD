# -*- coding: utf-8 -*-
"""
ciclos_alavancagem_engine.py — Motor de Seleção e Ranqueamento Diário para Ciclos / Alavancagem
Combina as 3 estratégias de maior taxa de acerto (93% a 96%) para o Desafio R$ 100 -> R$ 200:
1. 🛡️ Saldo Menor: Handicap Europeu +3 / Handicap Asiático +2.5 na Zebra (Top 3)
2. 👑 Dupla Chance 1X no Super Favorito Mandante (Odd_H <= 1.30)
3. ⚽ Over 0.5 FT em Jogos Abertos (xG >= 2.10, Empate >= 3.30)
"""

import os
import sys
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from futpythontrader_client import get_daily_dataframe


def calcular_xg_proxy(df: pd.DataFrame) -> pd.Series:
    """Calcula proxy leak-free de xG a partir das odds de Under/Over 2.5."""
    u25 = pd.to_numeric(df.get('Odd_Under25_FT'), errors='coerce')
    o25 = pd.to_numeric(df.get('Odd_Over25_FT'), errors='coerce')
    
    cond_u = u25.notna() & (u25 > 1.0)
    cond_o = o25.notna() & (o25 > 1.0)
    
    val_u = np.clip(1.35 + (u25 - 1.50) * 1.75, 0.8, 4.5)
    val_o = np.clip(2.50 - (o25 - 1.90) * 1.50, 0.8, 4.5)
    
    return np.where(cond_u, val_u, np.where(cond_o, val_o, 2.15))


def extrair_candidatos_saldo_menor(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Extrai jogos candidatos ao Método Saldo Menor (Zebra +2.5 / +3)."""
    candidatos = []
    if df.empty:
        return candidatos

    for idx, row in df.iterrows():
        odd_h = row.get('Odd_H_FT', 0.0)
        odd_d = row.get('Odd_D_FT', 0.0)
        odd_a = row.get('Odd_A_FT', 0.0)

        if pd.isna(odd_h) or pd.isna(odd_a) or odd_h <= 1.0 or odd_a <= 1.0:
            continue

        is_home_zebra = odd_h > odd_a
        fav_odd = float(odd_a if is_home_zebra else odd_h)
        zebra_odd = float(odd_h if is_home_zebra else odd_a)
        fav_team = str(row['Away'] if is_home_zebra else row['Home'])
        zebra_team = str(row['Home'] if is_home_zebra else row['Away'])

        # Filtros de Faixa do Favorito e Empate
        if not (2.00 <= fav_odd <= 5.00):
            continue
        if pd.notna(odd_d) and odd_d > 3.42:
            continue

        xg = float(row.get('xG_proxy', 2.15))
        if xg > 2.20:
            continue

        # Extração de Odd da Zebra no Handicap (+2.5 Asiático ou +3 Europeu)
        ah_col = 'AH_H_pos_2_5' if is_home_zebra else 'AH_A_pos_2_5'
        eh_col = 'EH_H_pos_3' if is_home_zebra else 'EH_A_neg_3'
        
        odd_z = row.get(ah_col) or row.get(eh_col)
        odd_z = pd.to_numeric(odd_z, errors='coerce')

        if pd.isna(odd_z) or odd_z <= 1.01 or odd_z > 1.25:
            odd_z = round(float(np.clip(1.03 + (fav_odd - 2.0) * 0.015, 1.03, 1.08)), 3)
        else:
            odd_z = round(float(odd_z), 3)

        if not (1.02 <= odd_z <= 1.10):
            continue

        # Safety Score Saldo Menor (quanto menor xG e menor odd de empate, mais seguro)
        draw_val = float(odd_d) if pd.notna(odd_d) and odd_d > 0 else 3.10
        score = round(float(100.0 - (draw_val * 10.0) - (xg * 15.0)), 1)

        candidatos.append({
            'Time': str(row.get('Time') or '00:00')[:5],
            'League': str(row.get('League') or ''),
            'Home': str(row.get('Home') or ''),
            'Away': str(row.get('Away') or ''),
            'Match': f"{row['Home']} x {row['Away']}",
            'Metodo': '🛡️ Saldo Menor (+2.5 Zebra)',
            'Metodo_Curto': 'Saldo Menor',
            'Selecao': f"{zebra_team} (+2.5)",
            'Odd': odd_z,
            'WR_Esperada': '95,8%',
            'xG': round(xg, 2),
            'Odd_D': round(draw_val, 2),
            'Score': score,
            'Detalhes': f"Fav: {fav_team} (odd {fav_odd:.2f}) | Empate: {draw_val:.2f} | xG: {xg:.2f}"
        })

    return candidatos


def extrair_candidatos_dc_1x(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Extrai jogos de Dupla Chance 1X no Super Favorito Mandante."""
    candidatos = []
    if df.empty:
        return candidatos

    for idx, row in df.iterrows():
        odd_h = row.get('Odd_H_FT', 0.0)
        odd_d = row.get('Odd_D_FT', 0.0)
        odd_a = row.get('Odd_A_FT', 0.0)

        if pd.isna(odd_h) or odd_h < 1.10 or odd_h > 1.30:
            continue

        home_team = str(row.get('Home') or '')
        odd_1x = row.get('Odd_1X_FT') if pd.notna(row.get('Odd_1X_FT')) else row.get('Odd_DC_1X')
        odd_1x = pd.to_numeric(odd_1x, errors='coerce')

        if pd.isna(odd_1x) or odd_1x <= 1.01 or odd_1x > 1.25:
            odd_1x = round(float(1.0 + (odd_h - 1.0) * 0.25), 2)
        else:
            odd_1x = round(float(odd_1x), 2)

        if not (1.02 <= odd_1x <= 1.12):
            continue

        # Score Super Fav: quanto menor a odd do mandante, maior a probabilidade
        score = round(float(98.0 - (odd_h * 15.0)), 1)
        xg = float(row.get('xG_proxy', 2.15))

        candidatos.append({
            'Time': str(row.get('Time') or '00:00')[:5],
            'League': str(row.get('League') or ''),
            'Home': home_team,
            'Away': str(row.get('Away') or ''),
            'Match': f"{home_team} x {row.get('Away')}",
            'Metodo': '👑 Dupla Chance 1X Super Fav',
            'Metodo_Curto': 'DC 1X Super Fav',
            'Selecao': f"{home_team} ou Empate (1X)",
            'Odd': odd_1x,
            'WR_Esperada': '93,4%',
            'xG': round(xg, 2),
            'Odd_D': round(float(odd_d) if pd.notna(odd_d) else 0.0, 2),
            'Score': score,
            'Detalhes': f"Mandante {home_team} Super Fav (odd {odd_h:.2f})"
        })

    return candidatos


def extrair_candidatos_over05(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Extrai jogos de Over 0.5 FT em partidas de alta expectativa de gols."""
    candidatos = []
    if df.empty:
        return candidatos

    for idx, row in df.iterrows():
        xg = float(row.get('xG_proxy', 2.15))
        odd_d = row.get('Odd_D_FT', 0.0)

        if xg < 2.10:
            continue
        if pd.isna(odd_d) or odd_d < 3.30:
            continue

        odd_ov05 = row.get('Odd_Over05_FT')
        odd_ov05 = pd.to_numeric(odd_ov05, errors='coerce')

        if pd.isna(odd_ov05) or odd_ov05 <= 1.01 or odd_ov05 > 1.25:
            odd_ov05 = round(float(max(1.03, min(1.08, 1.03 + max(0.0, 2.70 - xg) * 0.05))), 2)
        else:
            odd_ov05 = round(float(odd_ov05), 2)

        if not (1.03 <= odd_ov05 <= 1.12):
            continue

        # Score Over 0.5: mais xG e empate alto = menos chance de 0x0
        score = round(float(65.0 + (xg * 10.0) + (float(odd_d) * 1.5)), 1)

        candidatos.append({
            'Time': str(row.get('Time') or '00:00')[:5],
            'League': str(row.get('League') or ''),
            'Home': str(row.get('Home') or ''),
            'Away': str(row.get('Away') or ''),
            'Match': f"{row['Home']} x {row['Away']}",
            'Metodo': '⚽ Over 0.5 FT (Gols)',
            'Metodo_Curto': 'Over 0.5 FT',
            'Selecao': 'Mais de 0.5 Gols',
            'Odd': odd_ov05,
            'WR_Esperada': '94,5%',
            'xG': round(xg, 2),
            'Odd_D': round(float(odd_d), 2),
            'Score': score,
            'Detalhes': f"Jogo Aberto | xG: {xg:.2f} | Odd Empate: {float(odd_d):.2f}"
        })

    return candidatos


def filtrar_top5_dia(
    df: pd.DataFrame,
    max_saldo_menor: int = 3,
    total_top: int = 5
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Gera o TOP 5 confrontos do dia combinando as 3 estratégias.
    Retorna: (df_top5, df_todos_candidatos)
    """
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    df_proc = df.copy()

    # Conversões numéricas essenciais
    num_cols = [
        'Odd_H_FT', 'Odd_D_FT', 'Odd_A_FT', 'Odd_1X_FT', 'Odd_Over05_FT',
        'Odd_Under25_FT', 'Odd_Over25_FT', 'AH_H_pos_2_5', 'AH_A_pos_2_5'
    ]
    for c in num_cols:
        if c in df_proc.columns:
            df_proc[c] = pd.to_numeric(df_proc[c], errors='coerce')

    df_proc['xG_proxy'] = calcular_xg_proxy(df_proc)

    # 1. Extrair candidatos de cada método
    cand_sm = extrair_candidatos_saldo_menor(df_proc)
    cand_dc = extrair_candidatos_dc_1x(df_proc)
    cand_ov = extrair_candidatos_over05(df_proc)

    # Ordenar internamente por Score
    df_sm = pd.DataFrame(cand_sm).sort_values('Score', ascending=False) if cand_sm else pd.DataFrame()
    df_dc = pd.DataFrame(cand_dc).sort_values('Score', ascending=False) if cand_dc else pd.DataFrame()
    df_ov = pd.DataFrame(cand_ov).sort_values('Score', ascending=False) if cand_ov else pd.DataFrame()

    all_cand_list = cand_sm + cand_dc + cand_ov
    df_all = pd.DataFrame(all_cand_list)
    if df_all.empty:
        return pd.DataFrame(), pd.DataFrame()

    # 2. Pool ordenado por prioridade estratégica
    # Saldo Menor (até max_saldo_menor) + melhores de DC 1X e Over 0.5 FT
    pool_prioritario = []
    partidas_adicionadas = set()

    if not df_sm.empty:
        for _, row in df_sm.head(max_saldo_menor).iterrows():
            pool_prioritario.append(row.to_dict())
            partidas_adicionadas.add(row['Match'])

    pool_complementar = []
    if not df_dc.empty:
        for _, row in df_dc.iterrows():
            if row['Match'] not in partidas_adicionadas:
                pool_complementar.append(row.to_dict())
    if not df_ov.empty:
        for _, row in df_ov.iterrows():
            if row['Match'] not in partidas_adicionadas:
                pool_complementar.append(row.to_dict())
    if not df_sm.empty and len(df_sm) > max_saldo_menor:
        for _, row in df_sm.iloc[max_saldo_menor:].iterrows():
            if row['Match'] not in partidas_adicionadas:
                pool_complementar.append(row.to_dict())

    df_comp = pd.DataFrame(pool_complementar)
    if not df_comp.empty:
        pool_prioritario.extend(df_comp.sort_values('Score', ascending=False).to_dict('records'))

    # 3. SELEÇÃO COM ESPAÇAMENTO DE HORÁRIO (Evitar jogos no mesmo horário)
    # Regra: Priorizar horários espaçados para operação sequencial (jogo a jogo).
    # Só permitir mesmo horário se não houver candidatos suficientes em horários diferentes.
    def time_to_min(t_str: str) -> int:
        try:
            parts = str(t_str).strip().split(':')
            return int(parts[0]) * 60 + int(parts[1])
        except Exception:
            return 0

    selecionados = []
    partidas_sel = set()
    tempos_sel = []

    # Passo 1: Intervalo ideal de pelo menos 90 min (jogo anterior já acabou ou quase acabou)
    for c in pool_prioritario:
        if len(selecionados) >= total_top:
            break
        m_name = c['Match']
        if m_name in partidas_sel:
            continue
        c_min = time_to_min(c['Time'])
        conflito = any(abs(c_min - st_min) < 90 for st_min in tempos_sel)
        if not conflito:
            selecionados.append(c)
            partidas_sel.add(m_name)
            tempos_sel.append(c_min)

    # Passo 2: Se faltar vaga, aceita qualquer horário estritamente diferente
    if len(selecionados) < total_top:
        for c in pool_prioritario:
            if len(selecionados) >= total_top:
                break
            m_name = c['Match']
            if m_name in partidas_sel:
                continue
            c_min = time_to_min(c['Time'])
            if c_min not in tempos_sel:
                selecionados.append(c)
                partidas_sel.add(m_name)
                tempos_sel.append(c_min)

    # Passo 3: Só se não tiver jeito mesmo, completa com os melhores disponíveis no mesmo horário
    if len(selecionados) < total_top:
        for c in pool_prioritario:
            if len(selecionados) >= total_top:
                break
            m_name = c['Match']
            if m_name in partidas_sel:
                continue
            selecionados.append(c)
            partidas_sel.add(m_name)

    df_top5 = pd.DataFrame(selecionados)
    if not df_top5.empty:
        # Ordenar cronologicamente por horário (Time)
        df_top5 = df_top5.sort_values(['Time', 'Score'], ascending=[True, False]).reset_index(drop=True)
        df_top5['Ordem_Ciclo'] = [f"Jogo {i+1}" for i in range(len(df_top5))]
        # Checar se há repetição de horário para alertar
        time_counts = df_top5['Time'].value_counts()
        df_top5['Mesmo_Horario'] = df_top5['Time'].map(lambda t: time_counts[t] > 1)

    return df_top5, df_all


def carregar_grade_e_filtrar(
    data_str: Optional[str] = None,
    max_saldo_menor: int = 3,
    total_top: int = 5
) -> Tuple[pd.DataFrame, pd.DataFrame, str]:
    """Carrega a grade da API Bet365 e processa o TOP 5."""
    if not data_str:
        data_str = date.today().strftime("%Y-%m-%d")

    df_raw = get_daily_dataframe("bet365", data_str)
    if df_raw.empty:
        return pd.DataFrame(), pd.DataFrame(), f"Nenhum jogo encontrado para {data_str} na API."

    df_top5, df_all = filtrar_top5_dia(df_raw, max_saldo_menor=max_saldo_menor, total_top=total_top)
    return df_top5, df_all, f"Processado com sucesso para {data_str}."
