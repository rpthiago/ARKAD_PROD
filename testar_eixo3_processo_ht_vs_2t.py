#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
testar_eixo3_processo_ht_vs_2t.py — Execução do Estudo Eixo 3: Processo do 1º Tempo vs Gols no 2º Tempo
Autoridade: PREREGISTRO_eixo3_processo_ht_vs_2t.md + GEMINI.md
"""

import os, sys, warnings
import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy import stats

warnings.filterwarnings("ignore")
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass

ROOT = os.path.dirname(os.path.abspath(__file__))

def main():
    print("1. Carregando Bases_de_Dados_API_FutPythonTrader_Bet365.csv (248k jogos)...")
    csv_path = os.path.join(ROOT, "Bases_de_Dados_API_FutPythonTrader_Bet365.csv")
    df = pd.read_csv(csv_path, low_memory=False)
    
    df["Year"] = pd.to_datetime(df["Date"], errors="coerce").dt.year
    df = df[df["Year"] >= 2024].copy()
    print(f"Total de jogos 2024-2026 carregados: {len(df)}")
    
    # 2. FILTRAR JOGOS COM ESTATÍSTICA REAL DE HT (LEI 6 ANTI-ZEROS)
    for c in ["Total_Shots_H_HT", "Total_Shots_A_HT", "xG_H_HT", "xG_A_HT",
              "Shots_On_Target_H_HT", "Shots_On_Target_A_HT",
              "Goals_H_HT", "Goals_A_HT", "Goals_H_FT", "Goals_A_FT",
              "Odd_H_FT", "Odd_D_FT", "Odd_A_FT", "Odd_Over25_FT", "Odd_Under25_FT"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        
    tem_shots = (df["Total_Shots_H_HT"] > 0) | (df["Total_Shots_A_HT"] > 0)
    tem_xg = tem_shots & ((df["xG_H_HT"] > 0) | (df["xG_A_HT"] > 0))
    
    df_clean = df[tem_xg].copy()
    print(f"Jogos com xG HT real coletado (sem zeros fabricados): {len(df_clean)} ({len(df_clean)/len(df)*100:.1f}%)")
    
    # 3. UNIVERSO DE INTERESSE: JOGOS 0-0 NO INTERVALO (HT)
    zero_zero_ht = (df_clean["Goals_H_HT"] == 0) & (df_clean["Goals_A_HT"] == 0)
    df_ht = df_clean[zero_zero_ht].copy()
    print(f"Jogos 0-0 no intervalo com xG HT real: {len(df_ht)} ({len(df_ht)/len(df_clean)*100:.1f}%)")
    
    # Gols no 2º Tempo
    df_ht["gols_2t_h"] = df_ht["Goals_H_FT"] - df_ht["Goals_H_HT"]
    df_ht["gols_2t_a"] = df_ht["Goals_A_FT"] - df_ht["Goals_A_HT"]
    df_ht["gols_2t"] = df_ht["gols_2t_h"] + df_ht["gols_2t_a"]
    
    df_ht["xg_tot_ht"] = df_ht["xG_H_HT"] + df_ht["xG_A_HT"]
    df_ht["sot_tot_ht"] = df_ht["Shots_On_Target_H_HT"] + df_ht["Shots_On_Target_A_HT"]
    df_ht["shots_tot_ht"] = df_ht["Total_Shots_H_HT"] + df_ht["Total_Shots_A_HT"]
    
    print("\n" + "=" * 80)
    print("TABELA 1: GOLS NO 2º TEMPO POR FAIXA DE xG NO 1º TEMPO (0-0 HT)")
    print("=" * 80)
    
    buckets = [
        (0.0, 0.40, "0.0 - 0.40 (Muito Morto)"),
        (0.40, 0.80, "0.40 - 0.80 (Baixo)"),
        (0.80, 1.20, "0.80 - 1.20 (Médio)"),
        (1.20, 1.80, "1.20 - 1.80 (Alto)"),
        (1.80, 99.0, "1.80+ (Massacre Sem Gol)")
    ]
    
    b_rows = []
    for lo, hi, label in buckets:
        sub = df_ht[(df_ht["xg_tot_ht"] >= lo) & (df_ht["xg_tot_ht"] < hi)]
        n = len(sub)
        if n > 0:
            b_rows.append({
                "Faixa_xG_1T": label,
                "N": n,
                "Gols_2T_Med": f"{sub['gols_2t'].mean():.2f}",
                "% 0 Gol 2T": f"{(sub['gols_2t'] == 0).mean()*100:.1f}%",
                "% 1+ Gol 2T": f"{(sub['gols_2t'] >= 1).mean()*100:.1f}%",
                "% 2+ Gols 2T": f"{(sub['gols_2t'] >= 2).mean()*100:.1f}%",
                "% 3+ Gols 2T": f"{(sub['gols_2t'] >= 3).mean()*100:.1f}%"
            })
    print(pd.DataFrame(b_rows).to_string(index=False))
    
    # Correlação
    r, p_corr = stats.pearsonr(df_ht["xg_tot_ht"], df_ht["gols_2t"])
    print(f"\n-> Correlação Linear Global (xG_1T x Gols_2T): r = {r:.4f} (p = {p_corr:.4e})")
    
    # 4. REGRESSÕES LOGÍSTICAS CONTRA O PREÇO PRÉ-JOGO DA BETFAIR / B365
    print("\n" + "=" * 80)
    print("TABELA 2: REGRESSÃO LOGÍSTICA (Efeito do xG_1T Além da Odd de Over 2.5)")
    print("=" * 80)
    
    df_reg = df_ht[df_ht["Odd_Over25_FT"] > 1.05].dropna(subset=["Odd_Over25_FT", "xg_tot_ht", "sot_tot_ht"]).copy()
    df_reg["log_odd_ov25"] = np.log(df_reg["Odd_Over25_FT"])
    df_reg["tem_1gol_2t"] = (df_reg["gols_2t"] >= 1).astype(int)
    df_reg["tem_2gols_2t"] = (df_reg["gols_2t"] >= 2).astype(int)
    
    print(f"Amostra elegível para regressão (com Odd Over 2.5): N = {len(df_reg)}")
    
    # Modelo 1: 1+ Gol no 2T
    print("\n--- MODELO 1: P(Gols 2T >= 1) ~ log(Odd_Over25) + xG_1T + SoT_1T ---")
    X1 = sm.add_constant(df_reg[["log_odd_ov25", "xg_tot_ht", "sot_tot_ht"]])
    m1 = sm.Logit(df_reg["tem_1gol_2t"], X1).fit(disp=False)
    print(m1.summary())
    
    # Modelo 2: 2+ Gols no 2T
    print("\n--- MODELO 2: P(Gols 2T >= 2) ~ log(Odd_Over25) + xG_1T + SoT_1T ---")
    X2 = sm.add_constant(df_reg[["log_odd_ov25", "xg_tot_ht", "sot_tot_ht"]])
    m2 = sm.Logit(df_reg["tem_2gols_2t"], X2).fit(disp=False)
    print(m2.summary())
    
    # 5. ESTABILIDADE TEMPORAL (POR ANO)
    print("\n" + "=" * 80)
    print("TABELA 3: ESTABILIDADE TEMPORAL DO EFEITO POR ANO (2024, 2025, 2026)")
    print("=" * 80)
    for y in [2024, 2025, 2026]:
        sub_y = df_reg[df_reg["Year"] == y]
        m_y = sm.Logit(sub_y["tem_1gol_2t"], sm.add_constant(sub_y[["log_odd_ov25", "xg_tot_ht"]])).fit(disp=False)
        c_xg = m_y.params["xg_tot_ht"]
        p_xg = m_y.pvalues["xg_tot_ht"]
        print(f"Ano {y} (N={len(sub_y)}): Coef xG_1T = {c_xg:+.4f} (p = {p_xg:.4f})")
        
    # 6. SUB-HIPÓTESE 3-B: FAVORITO PRESSIONANDO EM 0-0 NO HT
    print("\n" + "=" * 80)
    print("TABELA 4: SUB-HIPÓTESE 3-B (Favorito Mandante Odd <= 1.50 empatando 0-0 HT)")
    print("=" * 80)
    fav_df = df_ht[df_ht["Odd_H_FT"] <= 1.50].copy()
    print(f"Total de jogos de Super Fav Mandante (<=1.50) 0-0 no HT: {len(fav_df)}")
    
    fav_df["fav_win_ft"] = (fav_df["Goals_H_FT"] > fav_df["Goals_A_FT"]).astype(int)
    
    fav_alta_pressao = fav_df[fav_df["xG_H_HT"] >= 1.00]
    fav_baixa_pressao = fav_df[fav_df["xG_H_HT"] < 0.60]
    
    print(f"  -> Fav com ALTA PRESSÃO 1T (xG_H_HT >= 1.00): N={len(fav_alta_pressao)} | Vitória FT: {fav_alta_pressao['fav_win_ft'].mean()*100:.1f}%")
    print(f"  -> Fav com BAIXA PRESSÃO 1T (xG_H_HT < 0.60): N={len(fav_baixa_pressao)} | Vitória FT: {fav_baixa_pressao['fav_win_ft'].mean()*100:.1f}%")

if __name__ == "__main__":
    main()
