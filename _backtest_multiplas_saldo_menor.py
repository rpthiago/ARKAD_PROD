"""
SCRIPT DE BACKTEST DEDICADO - MÚLTIPLAS TRIPLAS (3 JOGOS) - MÉTODO SALDO MENOR
ARKAD_PROD

Simula o desempenho histórico de agrupar partidas elegíveis do Método Saldo Menor em bilhetes
múltiplos de 3 jogos (Triplas) sobre a base histórica (Bases_de_Dados_API_FutPythonTrader_Bet365.csv).
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Tuple, Dict, Any, List


def load_and_sanitize_data() -> pd.DataFrame:
    """Carrega a base histórica e filtra os jogos com a sanitização das odds de EH +3."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(base_dir, "Bases_de_Dados_API_FutPythonTrader_Bet365.csv")

    if not os.path.exists(csv_path):
        csv_path = os.path.join(base_dir, "Resultados_2026_Full.csv")

    if not os.path.exists(csv_path):
        raise FileNotFoundError("Base de dados histórica não encontrada.")

    print(f"[+] Lendo base de dados: {os.path.basename(csv_path)}...")
    df = pd.read_csv(csv_path, low_memory=False)

    # Datas
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df = df.sort_values('Date').reset_index(drop=True)

    # Odds 1X2
    df['Odd_H_FT'] = pd.to_numeric(df.get('Odd_H_FT'), errors='coerce')
    df['Odd_A_FT'] = pd.to_numeric(df.get('Odd_A_FT'), errors='coerce')
    df = df.dropna(subset=['Odd_H_FT', 'Odd_A_FT']).copy()

    df['Is_Home_Zebra'] = df['Odd_H_FT'] > df['Odd_A_FT']
    df['Zebra_Odd'] = df['Odd_H_FT'].where(df['Is_Home_Zebra'], df['Odd_A_FT'])
    df['Fav_Odd'] = df['Odd_A_FT'].where(df['Is_Home_Zebra'], df['Odd_H_FT'])

    # EH +3 Zebra
    eh_h = pd.to_numeric(df.get('EH_H_pos_3'), errors='coerce').fillna(0.0)
    eh_a = pd.to_numeric(df.get('EH_A_pos_3'), errors='coerce').fillna(0.0)
    df['EH_Zebra_Plus3_Odd'] = eh_h.where(df['Is_Home_Zebra'], eh_a)

    # xG
    col_h = 'xG_H_FT' if 'xG_H_FT' in df.columns else ('xG_H_Pre' if 'xG_H_Pre' in df.columns else None)
    col_a = 'xG_A_FT' if 'xG_A_FT' in df.columns else ('xG_A_Pre' if 'xG_A_Pre' in df.columns else None)
    xg_h = pd.to_numeric(df[col_h], errors='coerce').fillna(0.0) if col_h else pd.Series(0.0, index=df.index)
    xg_a = pd.to_numeric(df[col_a], errors='coerce').fillna(0.0) if col_a else pd.Series(0.0, index=df.index)
    df['Total_xG'] = xg_h + xg_a

    # Filtros do Método Saldo Menor (Com Sanitização Real de Odds)
    cond_a = (df['Fav_Odd'] >= 2.20) & (df['Fav_Odd'] <= 5.00) | (df['Zebra_Odd'] >= 2.20) & (df['Zebra_Odd'] <= 5.00)
    cond_b = (df['EH_Zebra_Plus3_Odd'] > 1.0) & (df['EH_Zebra_Plus3_Odd'] < df['Zebra_Odd']) & (df['EH_Zebra_Plus3_Odd'] <= 2.50)
    cond_c = (1.0 / df['Zebra_Odd']) <= 0.45
    cond_d = (df['Total_xG'] > 0) & (df['Total_xG'] <= 2.0)

    filtrado = df[cond_a & cond_b & cond_c & cond_d].copy()

    # Resultado individual do jogo
    gols_h = pd.to_numeric(filtrado.get('Goals_H_FT'), errors='coerce').fillna(0)
    gols_a = pd.to_numeric(filtrado.get('Goals_A_FT'), errors='coerce').fillna(0)
    filtrado['Gols_Zebra'] = gols_h.where(filtrado['Is_Home_Zebra'], gols_a)
    filtrado['Gols_Fav'] = gols_a.where(filtrado['Is_Home_Zebra'], gols_h)
    filtrado['Diff_Gols_Fav'] = filtrado['Gols_Fav'] - filtrado['Gols_Zebra']
    filtrado['Green_Individual'] = filtrado['Diff_Gols_Fav'] < 3

    print(f"[i] Total de jogos aprovados no modelo: {len(filtrado):,}")
    return filtrado


def run_triples_backtest(df_games: pd.DataFrame, stake_por_bilhete: float = 100.0) -> Tuple[pd.DataFrame, dict]:
    """Agrupa os jogos cronologicamente em bilhetes triplos (3 jogos) e simula o desempenho."""
    num_jogos = len(df_games)
    num_bilhetes = num_jogos // 3

    bilhetes = []
    lucro_acumulado = 0.0
    banca_historica = []

    streak_atual_green = 0
    max_streak_green = 0
    streak_atual_red = 0
    max_streak_red = 0

    for i in range(num_bilhetes):
        chunk = df_games.iloc[i * 3 : (i + 1) * 3]
        
        data_bilhete = chunk['Date'].iloc[-1] if 'Date' in chunk.columns else None
        odd_combinada = chunk['EH_Zebra_Plus3_Odd'].prod()
        green_bilhete = chunk['Green_Individual'].all()

        if green_bilhete:
            lucro_bilhete = stake_por_bilhete * (odd_combinada - 1.0)
            resultado_str = "GREEN"
            streak_atual_green += 1
            max_streak_green = max(max_streak_green, streak_atual_green)
            streak_atual_red = 0
        else:
            lucro_bilhete = -stake_por_bilhete
            resultado_str = "RED"
            streak_atual_red += 1
            max_streak_red = max(max_streak_red, streak_atual_red)
            streak_atual_green = 0

        lucro_acumulado += lucro_bilhete
        banca_historica.append(lucro_acumulado)

        jogos_str = " | ".join([f"{r['Home']} x {r['Away']}" for _, r in chunk.iterrows()])

        bilhetes.append({
            'Bilhete_ID': i + 1,
            'Data': data_bilhete,
            'Jogos': jogos_str,
            'Odd_Combinada': odd_combinada,
            'Resultado': resultado_str,
            'Lucro': lucro_bilhete,
            'Banca_Acumulada': lucro_acumulado
        })

    df_bilhetes = pd.DataFrame(bilhetes)

    # Métricas Globais
    total_bilhetes = len(df_bilhetes)
    greens = int((df_bilhetes['Resultado'] == 'GREEN').sum())
    reds = total_bilhetes - greens
    win_rate = (greens / total_bilhetes) * 100 if total_bilhetes > 0 else 0.0
    odd_media = df_bilhetes['Odd_Combinada'].mean() if total_bilhetes > 0 else 0.0
    lucro_total = df_bilhetes['Lucro'].sum() if total_bilhetes > 0 else 0.0
    total_investido = total_bilhetes * stake_por_bilhete
    roi = (lucro_total / total_investido) * 100 if total_investido > 0 else 0.0

    # Drawdown Máximo
    df_bilhetes['Peak'] = df_bilhetes['Banca_Acumulada'].cummax()
    df_bilhetes['Drawdown'] = df_bilhetes['Banca_Acumulada'] - df_bilhetes['Peak']
    max_dd = df_bilhetes['Drawdown'].min()

    summary = {
        'total_bilhetes': total_bilhetes,
        'greens': greens,
        'reds': reds,
        'win_rate_pct': win_rate,
        'odd_media_tripla': odd_media,
        'stake_por_bilhete': stake_por_bilhete,
        'total_investido_rs': total_investido,
        'lucro_total_rs': lucro_total,
        'roi_pct': roi,
        'max_drawdown_rs': max_dd,
        'max_streak_green': max_streak_green,
        'max_streak_red': max_streak_red
    }

    return df_bilhetes, summary


def main():
    print("====================================================================")
    print("    BACKTEST DEDICADO - MÚLTIPLAS TRIPLAS (3 JOGOS EH +3 ZEBRA)     ")
    print("====================================================================")

    df_games = load_and_sanitize_data()
    df_bilhetes, summary = run_triples_backtest(df_games, stake_por_bilhete=100.0)

    print("\n---------------- RELATÓRIO DE DESEMPENHO TRIPLO ----------------")
    print(f"Total de Bilhetes Triplos Executados : {summary['total_bilhetes']:,}")
    print(f"Bilhetes GREEN                        : {summary['greens']:,} ({summary['win_rate_pct']:.2f}%)")
    print(f"Bilhetes RED                          : {summary['reds']:,}")
    print(f"Odd Média da Múltipla Tripla          : {summary['odd_media_tripla']:.2f}")
    print(f"Stake por Bilhete Triplo              : R$ {summary['stake_por_bilhete']:.2f}")
    print(f"Total Investido Acumulado             : R$ {summary['total_investido_rs']:,.2f}")
    print(f"Lucro Líquido Acumulado               : R$ {summary['lucro_total_rs']:,.2f}")
    print(f"ROI sobre Capital Investido           : {summary['roi_pct']:.2f}%")
    print(f"Drawdown Máximo Acumulado             : R$ {summary['max_drawdown_rs']:,.2f}")
    print(f"Maior Sequência de Greens Consecutive : {summary['max_streak_green']} bilhetes")
    print(f"Maior Sequência de Reds Consecutive   : {summary['max_streak_red']} bilhetes")
    print("--------------------------------------------------------------------")

    # Salvar resultados em CSV
    output_ops = "backtest_multiplas_saldo_menor_ops.csv"
    output_summary = "backtest_multiplas_saldo_menor_resumo.csv"

    df_bilhetes.to_csv(output_ops, index=False)
    pd.DataFrame([summary]).to_csv(output_summary, index=False)

    print(f"[OK] Bilhetes salvos em: {output_ops}")
    print(f"[OK] Resumo salvo em   : {output_summary}")


if __name__ == "__main__":
    main()
