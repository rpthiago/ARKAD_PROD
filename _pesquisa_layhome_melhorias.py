"""
PESQUISA QUANTITATIVA DE MELHORIAS PARA O LAY HOME (BETFAIR ODD LAY REAL)
ARKAD_PROD

Objetivo: Testar sistematicamente filtros pré-jogo purificados (sem vazamento)
para verificar se existe uma janela +EV sustentável para o Lay Home.
"""

import os
import pandas as pd
import numpy as np

def run_optimization():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    betfair_csv = os.path.join(base_dir, "Bases_de_Dados_API_FutPythonTrader_Betfair.csv")

    if not os.path.exists(betfair_csv):
        raise FileNotFoundError("Base da Betfair não encontrada.")

    print("[+] Lendo base real Betfair...")
    df = pd.read_csv(betfair_csv, low_memory=False)

    df['Odd_H_Back'] = pd.to_numeric(df.get('Odd_H_Back'), errors='coerce')
    df['Odd_H_Lay'] = pd.to_numeric(df.get('Odd_H_Lay'), errors='coerce')
    df['Odd_D_Back'] = pd.to_numeric(df.get('Odd_D_Back'), errors='coerce')
    df['Odd_A_Back'] = pd.to_numeric(df.get('Odd_A_Back'), errors='coerce')
    df['Odd_Under25_Back'] = pd.to_numeric(df.get('Odd_Under25_FT_Back'), errors='coerce')

    gols_h = pd.to_numeric(df.get('Goals_H_FT'), errors='coerce')
    gols_a = pd.to_numeric(df.get('Goals_A_FT'), errors='coerce')

    df = df.dropna(subset=['Odd_H_Back', 'Odd_H_Lay', 'Goals_H_FT', 'Goals_A_FT']).copy()
    df = df[(df['Odd_H_Back'] > 1.0) & (df['Odd_H_Lay'] > 1.0)].copy()

    df['LayHome_Green'] = gols_h <= gols_a
    df['Is_Home_Zebra'] = df['Odd_H_Back'] > df['Odd_A_Back']
    df['Spread_Lay_Back'] = df['Odd_H_Lay'] - df['Odd_H_Back']

    liability = 100.0
    df['Lucro_Real'] = np.where(
        df['LayHome_Green'],
        (liability / (df['Odd_H_Lay'] - 1.0)) * 0.95,
        -liability
    )

    print(f"[i] Total de partidas válidas na base Betfair: {len(df):,}\n")

    # Grid de Testes de Filtros Pré-Jogo
    results = []

    # 1. Filtro de Teto de Odd Lay (Controlar o impacto do spread e das zebras super-altas)
    for max_lay_odd in [2.50, 2.80, 3.00, 3.20, 3.50, 4.00]:
        for min_lay_odd in [1.50, 2.00, 2.20]:
            for max_u25_odd in [1.60, 1.70, 1.80, 2.00, 9.00]:
                for max_draw_odd in [3.20, 3.40, 3.60, 9.00]:
                    cond = (
                        df['Is_Home_Zebra'] &
                        (df['Odd_H_Lay'] >= min_lay_odd) &
                        (df['Odd_H_Lay'] <= max_lay_odd) &
                        (df['Odd_Under25_Back'] <= max_u25_odd) &
                        (df['Odd_D_Back'] <= max_draw_odd)
                    )
                    sub = df[cond]
                    n = len(sub)
                    if n >= 200:  # Mínimo de 200 partidas para significância estatística
                        wr = sub['LayHome_Green'].mean() * 100.0
                        lucro = sub['Lucro_Real'].sum()
                        roi = (lucro / (n * liability)) * 100.0
                        profit_factor = (sub[sub['Lucro_Real'] > 0]['Lucro_Real'].sum() / abs(sub[sub['Lucro_Real'] < 0]['Lucro_Real'].sum())) if (sub['Lucro_Real'] < 0).any() else 99.0
                        
                        results.append({
                            'Min_Odd_Lay': min_lay_odd,
                            'Max_Odd_Lay': max_lay_odd,
                            'Max_Odd_U25': max_u25_odd,
                            'Max_Odd_Draw': max_draw_odd,
                            'Jogos': n,
                            'WinRate': wr,
                            'Lucro_RS': lucro,
                            'ROI_Pct': roi,
                            'Profit_Factor': profit_factor
                        })

    res_df = pd.DataFrame(results)
    if not res_df.empty:
        res_df = res_df.sort_values('ROI_Pct', ascending=False).reset_index(drop=True)
        print("=== TOP 10 COMBINAÇÕES DE FILTROS PRÉ-JOGO +EV PARA LAY HOME ===")
        print(res_df.head(10).to_string(index=False))
    else:
        print("[!] Nenhuma combinação atendeu ao limite mínimo de jogos.")

if __name__ == "__main__":
    run_optimization()
