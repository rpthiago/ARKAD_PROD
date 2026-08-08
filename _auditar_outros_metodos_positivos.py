"""
AUDITORIA TEMPORAL ANO A ANO - OTHER POOLED POSITIVE METHODS (Under 0.5 HT & Under 2.5 HT)
ARKAD_PROD

Verifica se o Under 0.5 HT e o Under 2.5 HT sofreram a mesma degradação temporal
de edge (morte de EV a partir de 2023) observada no Under 1.5 HT.
"""

import os
import pandas as pd
import numpy as np

def run_temporal_audit():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    primary_csv = os.path.join(base_dir, "Bases_de_Dados_API_FutPythonTrader_Bet365.csv")

    if not os.path.exists(primary_csv):
        raise FileNotFoundError("Base Bet365 não encontrada.")

    print("[+] Lendo base Bet365...")
    df = pd.read_csv(primary_csv, low_memory=False)

    cols = ['Odd_H_FT', 'Odd_A_FT', 'Goals_H_HT', 'Goals_A_HT', 'Odd_Under25_FT', 'Odd_Under05_HT', 'Odd_Under25_HT']
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors='coerce')

    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df = df.dropna(subset=['Odd_H_FT', 'Odd_A_FT', 'Goals_H_HT', 'Goals_A_HT', 'Odd_Under25_FT', 'Date']).copy()

    df['fav'] = df[['Odd_H_FT', 'Odd_A_FT']].min(axis=1)
    df['zeb'] = df[['Odd_H_FT', 'Odd_A_FT']].max(axis=1)
    df['htg'] = df['Goals_H_HT'] + df['Goals_A_HT']
    df['ano'] = df['Date'].dt.year
    df['mes'] = df['Date'].dt.to_period('M').astype(str)

    # Filtro leak-free pré-jogo
    filt = df[(df['fav'] >= 2.20) & (df['zeb'] <= 5.00) & ((1 / df['zeb']) <= 0.45) & (df['Odd_Under25_FT'] <= 2.05)].copy()

    stake = 100.0

    print("=========================================================================================================")
    print("                 AUDITORIA TEMPORAL COMPLETA: UNDER 0.5 HT E UNDER 2.5 HT                                 ")
    print("=========================================================================================================\n")

    for mkt_name, mkt_col, target_gols in [('Under 0.5 HT', 'Odd_Under05_HT', 0), ('Under 2.5 HT', 'Odd_Under25_HT', 2)]:
        sub = filt.dropna(subset=[mkt_col]).copy()
        sub = sub[sub[mkt_col] > 1.0].copy()
        sub['green'] = sub['htg'] <= target_gols
        sub['pnl'] = np.where(sub['green'], stake * (sub[mkt_col] - 1.0), -stake)

        print(f"--- ANÁLISE DE ESTABILIDADE ANO A ANO: {mkt_name} ---")
        for ano in sorted(sub['ano'].unique()):
            s_ano = sub[sub['ano'] == ano]
            n_ano = len(s_ano)
            if n_ano < 200: continue
            wr = s_ano['green'].mean() * 100.0
            lucro = s_ano['pnl'].sum()
            roi = (lucro / (n_ano * stake)) * 100.0
            status_str = "[POSITIVO]" if roi > 0 else "[MORTO -EV]"
            print(f"  Ano {ano}: n={n_ano:>5} | WinRate={wr:>6.2f}% | Lucro=R$ {lucro:>10.2f} | ROI={roi:>+6.2f}% {status_str}")

        # Período Recente (2024-2026) e OOS (2025-08+)
        rec = sub[sub['ano'] >= 2024]
        oos = sub[sub['mes'] >= '2025-08']

        roi_rec = (rec['pnl'].sum() / (len(rec) * stake) * 100.0) if len(rec) > 0 else 0
        roi_oos = (oos['pnl'].sum() / (len(oos) * stake) * 100.0) if len(oos) > 0 else 0

        status_rec = "[+EV]" if roi_rec > 0 else "[-EV MORTO]"
        status_oos = "[+EV]" if roi_oos > 0 else "[-EV MORTO]"

        print(f"\n  >> PERÍODO RECENTE (2024-2026) : n={len(rec):>6} | ROI={roi_rec:>+6.2f}% {status_rec}")
        print(f"  >> OOS RECENTE (2025-08+)      : n={len(oos):>6} | ROI={roi_oos:>+6.2f}% {status_oos}")
        print("-" * 105 + "\n")

if __name__ == "__main__":
    run_temporal_audit()
