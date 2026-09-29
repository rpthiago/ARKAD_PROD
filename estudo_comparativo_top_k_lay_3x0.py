# -*- coding: utf-8 -*-
"""
estudo_comparativo_top_k_lay_3x0.py
Estudo Empírico e Forense: Top 1 vs Top 2 vs Top 3 vs Top 4 vs Top 5 vs Todos no Lay 3x0 Correct Score.
Avaliação rigorosa sob as 5 Leis do GEMINI.md:
- Odd executável de Lay real (Odd_CS_3x0_Lay em [14, 35])
- Filtros base aprovados: Under 2.5 <= 1.75 | Odd_H >= 1.80
- Comissão Betfair 5.0%
- Análise temporal: 2024, 2025, 2026 Completo, 2026 OOS Limpo (Mai-Set), Forward Real (Ago-Set)
- Bootstrap 10.000x IC95%
- Simulação B7 Composta (Stop -10% / +10%)
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')
import warnings
warnings.filterwarnings('ignore')

import glob
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent

# 1. Carregar base consolidada oficial
print("[-] Carregando base consolidada oficial...")
dfs_bf = []
for fn in ["Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH3.csv", "metodos_aprovados/.cache_base_betfair.csv"]:
    p = ROOT / fn
    if p.exists():
        dfs_bf.append(pd.read_csv(p, low_memory=False))

feed_files = sorted(glob.glob(str(ROOT / "scratch" / "feed_arquivo" / "*.parquet")))
if feed_files:
    dfs_bf.append(pd.concat([pd.read_parquet(fp) for fp in feed_files], ignore_index=True))

df = pd.concat(dfs_bf, ignore_index=True)
df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
df["Home"] = df["Home"].astype(str).str.strip()
df["Away"] = df["Away"].astype(str).str.strip()
df["Goals_H_FT"] = pd.to_numeric(df["Goals_H_FT"], errors="coerce")
df["Goals_A_FT"] = pd.to_numeric(df["Goals_A_FT"], errors="coerce")

df = df.dropna(subset=["Date", "Home", "Away", "Goals_H_FT", "Goals_A_FT"]).copy()
df = df.drop_duplicates(subset=["Date", "Home", "Away"], keep="last").sort_values(["Date", "Time"]).reset_index(drop=True)

# Merge odds se necessário
for col in ["Odd_Under25_FT_Back", "Odd_Under25_FT", "Odd_Under25"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

for col in ["Odd_H_Back", "Odd_H_FT_Back", "Odd_H_FT", "Odd_H"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

df["Odd_CS_3x0_Lay"] = pd.to_numeric(df.get("Odd_CS_3x0_Lay"), errors="coerce")

u25 = df["Odd_Under25_FT_Back"].fillna(df.get("Odd_Under25_FT", np.nan)).fillna(df.get("Odd_Under25", np.nan))
oh = df["Odd_H_Back"].fillna(df.get("Odd_H_FT_Back", np.nan)).fillna(df.get("Odd_H_FT", np.nan)).fillna(df.get("Odd_H", np.nan))
o3x0 = df["Odd_CS_3x0_Lay"]

gh = df["Goals_H_FT"]
ga = df["Goals_A_FT"]
is_green = ~((gh == 3) & (ga == 0))

# Filtro Base Estrutural do Lay 3x0
mask_base = (u25 <= 1.75) & (oh >= 1.80) & (o3x0 >= 14.0) & (o3x0 <= 35.0) & o3x0.notna()
df_base = df[mask_base].copy()
print(f"[+] Total de jogos no filtro estrutural amplo (sem limite de ranking): {len(df_base)}")

df_base["Year"] = df_base["Date"].str.slice(0, 4)
df_base["Hora"] = df_base["Time"].astype(str).str.slice(0, 5)
df_base["is_green"] = is_green.loc[df_base.index]

COMM = 0.05

def calc_metrics(sub_df):
    if len(sub_df) == 0:
        return {
            "N": 0, "Greens": 0, "Reds": 0, "WR%": 0.0, "BE_WR%": 0.0, "Gap_pp": 0.0,
            "Odds_Med": 0.0, "PL_Stake": 0.0, "ROI_Stake%": 0.0, "PL_Liab": 0.0, "ROI_Liab%": 0.0
        }
    n = len(sub_df)
    greens = int(sub_df["is_green"].sum())
    reds = n - greens
    wr = (greens / n) * 100.0
    
    odds = sub_df["Odd_CS_3x0_Lay"].values
    be_wr = np.mean((odds - 1.0) / (odds - COMM)) * 100.0
    gap = wr - be_wr
    
    # P&L nominal por aposta de 1u stake: green = +0.95, red = -(odd - 1)
    pl_stake = np.where(sub_df["is_green"], 1.0 - COMM, -(odds - 1.0)).sum()
    roi_stake = (pl_stake / n) * 100.0
    
    # P&L sobre Capital em Risco (Liability fixa de 1u):
    # Se liability = 1u, stake = 1 / (odd - 1).
    # Green = (1 / (odd - 1)) * 0.95; Red = -1.0
    pl_liab_array = np.where(sub_df["is_green"], (1.0 - COMM) / (odds - 1.0), -1.0)
    pl_liab = pl_liab_array.sum()
    roi_liab = (pl_liab / n) * 100.0
    
    return {
        "N": n, "Greens": greens, "Reds": reds, "WR%": wr, "BE_WR%": be_wr, "Gap_pp": gap,
        "Odds_Med": float(np.mean(odds)), "PL_Stake": pl_stake, "ROI_Stake%": roi_stake,
        "PL_Liab": pl_liab, "ROI_Liab%": roi_liab
    }

def bootstrap_ci(sub_df, n_iter=10000):
    if len(sub_df) < 5:
        return 0.0, 0.0, 1.0
    odds = sub_df["Odd_CS_3x0_Lay"].values
    pl_liab = np.where(sub_df["is_green"], (1.0 - COMM) / (odds - 1.0), -1.0)
    n = len(pl_liab)
    
    rng = np.random.default_rng(42)
    boot_rois = []
    for _ in range(n_iter):
        sample = rng.choice(pl_liab, size=n, replace=True)
        boot_rois.append((sample.sum() / n) * 100.0)
    
    ci_low = np.percentile(boot_rois, 2.5)
    ci_high = np.percentile(boot_rois, 97.5)
    p_neg = np.mean(np.array(boot_rois) <= 0.0)
    return ci_low, ci_high, p_neg

# Função para selecionar Top-K com ranking diário e desempate por horário distinto
def selecionar_top_k(df_subset, k=None, desempate_horario=True):
    if k is None:
        return df_subset.copy()
    
    selecionados_idx = []
    for dt, grupo in df_subset.groupby("Date"):
        if len(grupo) <= k:
            selecionados_idx.extend(grupo.index.tolist())
            continue
        
        # Ordenar por menor odd
        grupo_sorted = grupo.sort_values("Odd_CS_3x0_Lay", ascending=True)
        
        if not desempate_horario:
            # Top-K direto por menor odd
            selecionados_idx.extend(grupo_sorted.iloc[:k].index.tolist())
        else:
            # Trava com desempate por horário distinto
            dia_sel = []
            horarios_usados = set()
            odds_unicas = sorted(grupo_sorted["Odd_CS_3x0_Lay"].unique())
            
            for o in odds_unicas:
                sub_odd = grupo_sorted[grupo_sorted["Odd_CS_3x0_Lay"] == o]
                for idx, row in sub_odd.iterrows():
                    if len(dia_sel) == k:
                        break
                    h = row["Hora"]
                    if h not in horarios_usados:
                        dia_sel.append(idx)
                        horarios_usados.add(h)
                if len(dia_sel) == k:
                    break
                
                # Se ainda precisa completar vagas na mesma odd
                for idx, row in sub_odd.iterrows():
                    if len(dia_sel) == k:
                        break
                    if idx not in dia_sel:
                        dia_sel.append(idx)
                if len(dia_sel) == k:
                    break
            
            selecionados_idx.extend(dia_sel)
            
    return df_subset.loc[selecionados_idx].sort_values(["Date", "Time"]).copy()

configs = [
    ("Top 1 (Menor Odd)", 1, True),
    ("Top 2 (Menor Odd + Horário)", 2, True),
    ("Top 3 (Atual Produção)", 3, True),
    ("Top 4 (Menor Odd + Horário)", 4, True),
    ("Top 5 (Menor Odd + Horário)", 5, True),
    ("Top 2 (Direto por Odd)", 2, False),
    ("Top 3 (Direto por Odd)", 3, False),
    ("Sem Limite (Todos Qualificados)", None, False),
]

resultados = []

print("\n" + "="*95)
print(f"{'Configuração':<32} | {'N':>5} | {'WR%':>6} | {'BE%':>6} | {'Gap':>5} | {'Odd Med':>7} | {'ROI Liab%':>9} | {'IC95% ROI':>16} | {'P(<=0)':>6}")
print("="*95)

for nome, k, desemp in configs:
    df_k = selecionar_top_k(df_base, k=k, desempate_horario=desemp)
    m = calc_metrics(df_k)
    ci_low, ci_high, p_neg = bootstrap_ci(df_k)
    
    # Análise temporal
    m_24 = calc_metrics(df_k[df_k["Year"] == "2024"])
    m_25 = calc_metrics(df_k[df_k["Year"] == "2025"])
    m_26 = calc_metrics(df_k[df_k["Year"] == "2026"])
    m_oos_clean = calc_metrics(df_k[(df_k["Date"] >= "2026-05-01")])
    m_fwd = calc_metrics(df_k[(df_k["Date"] >= "2026-08-01")])
    
    resultados.append({
        "Config": nome, "k": k, "Desempate": desemp,
        "Total": m, "CI_Low": ci_low, "CI_High": ci_high, "P_Neg": p_neg,
        "2024": m_24, "2025": m_25, "2026": m_26,
        "OOS_Clean_26": m_oos_clean, "Forward_AgoSet": m_fwd,
        "df_k": df_k
    })
    
    print(f"{nome:<32} | {m['N']:>5} | {m['WR%']:>5.2f}% | {m['BE_WR%']:>5.2f}% | {m['Gap_pp']:>+5.2f} | {m['Odds_Med']:>7.2f} | {m['ROI_Liab%']:>+8.2f}% | [{ci_low:>+6.2f}%, {ci_high:>+6.2f}%] | {p_neg:>6.4f}")

print("="*95)

# Tabela Temporal Detalhada
print("\n" + "="*110)
print("📊 DESEMPENHO TEMPORAL (ROI SOBRE LIABILITY % / WR% / REDS)")
print(f"{'Configuração':<28} | {'2024 (N / ROI% / R)':<22} | {'2025 (N / ROI% / R)':<22} | {'2026 OOS Mai-Set':<20} | {'Forward Ago-Set':<20}")
print("="*110)

for r in resultados:
    nome = r["Config"]
    t24 = f"{r['2024']['N']}j | {r['2024']['ROI_Liab%']:+.2f}% ({r['2024']['Reds']}R)"
    t25 = f"{r['2025']['N']}j | {r['2025']['ROI_Liab%']:+.2f}% ({r['2025']['Reds']}R)"
    t_clean = f"{r['OOS_Clean_26']['N']}j | {r['OOS_Clean_26']['ROI_Liab%']:+.2f}% ({r['OOS_Clean_26']['Reds']}R)"
    t_fwd = f"{r['Forward_AgoSet']['N']}j | {r['Forward_AgoSet']['ROI_Liab%']:+.2f}% ({r['Forward_AgoSet']['Reds']}R)"
    print(f"{nome:<28} | {t24:<22} | {t25:<22} | {t_clean:<20} | {t_fwd:<20}")

print("="*110)

# Simulação da Gestão de Banca B7 com Stop Loss Diário (-10% / +10%)
print("\n" + "="*90)
print("💰 SIMULAÇÃO CENÁRIO B7 COMPOSTO (Banca Inicial R$ 2.000 | 10% Liability | Stop -10% / +10%)")
print(f"{'Configuração':<28} | {'Sinais':>7} | {'Banca Final':>14} | {'Lucro Líquido':>14} | {'Max Drawdown':>14}")
print("="*90)

for r in resultados:
    df_sim = r["df_k"].copy().sort_values(["Date", "Time"]).reset_index(drop=True)
    banca = 2000.0
    banca_peak = 2000.0
    max_dd = 0.0
    
    # Agrupar por dia
    for dt, grupo_dia in df_sim.groupby("Date"):
        banca_inicio_dia = banca
        pnl_dia = 0.0
        
        for _, row in grupo_dia.iterrows():
            # Checar stops
            if pnl_dia <= -0.10 * banca_inicio_dia:
                break # Stop Loss atingido
            if pnl_dia >= 0.10 * banca_inicio_dia:
                break # Stop Win atingido
                
            liab = banca * 0.10
            odd = row["Odd_CS_3x0_Lay"]
            stake = liab / (odd - 1.0)
            
            if row["is_green"]:
                lucro = stake * (1.0 - COMM)
                pnl_dia += lucro
                banca += lucro
            else:
                perda = liab
                pnl_dia -= perda
                banca -= perda
                
            if banca > banca_peak:
                banca_peak = banca
            dd = (banca - banca_peak) / banca_peak
            if dd < max_dd:
                max_dd = dd
                
            if banca <= 10.0:
                break
        if banca <= 10.0:
            break
            
    lucro_total = banca - 2000.0
    ret_pct = (lucro_total / 2000.0) * 100.0
    print(f"{r['Config']:<28} | {len(df_sim):>7} | R$ {banca:>11,.2f} | R$ {lucro_total:>+11,.2f} ({ret_pct:>+6.1f}%) | {max_dd*100:>13.2f}%")

print("="*90)
