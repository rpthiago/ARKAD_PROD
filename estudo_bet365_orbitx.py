# -*- coding: utf-8 -*-
"""
estudo_bet365_orbitx.py — Estudo Empírico Completo:
Filtro de Entrada com Odds Pré-Jogo Bet365 -> Execução em Lay no OrbitX (Exchange)
Amostra: Bases_de_Dados_API_FutPythonTrader_Bet365.csv (248.219 jogos, 2024-2026)
"""
import sys
import pandas as pd
import numpy as np

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

print("=== ESTUDO: FILTRO PRE-JOGO BET365 -> EXECUCAO LAY NO ORBITX ===")
df = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Bet365.csv", low_memory=False)

for col in ["Goals_H_FT", "Goals_A_FT", "Odd_H_FT", "Odd_D_FT", "Odd_A_FT"]:
    df[col] = pd.to_numeric(df[col], errors="coerce")

df = df.dropna(subset=["Goals_H_FT", "Goals_A_FT", "Odd_H_FT", "Odd_D_FT", "Odd_A_FT", "Date"]).copy()
df = df[(df["Odd_H_FT"] > 1.0) & (df["Odd_D_FT"] > 1.0) & (df["Odd_A_FT"] > 1.0)]
df["Ano"] = pd.to_datetime(df["Date"], errors="coerce").dt.year
df = df[df["Ano"] >= 2024].copy()

print(f"Total de jogos analisados (2024 a 2026): {len(df):,}")

# Identificar Copas vs Ligas
df["eh_copa"] = df["League"].astype(str).str.contains(
    "CUP|COPA|CHAMPIONS|EUROPA|POKAL|COUPE|COPPA|TROPHY|TACA", case=False, na=False
)

# Modelagem de Odd de Lay no OrbitX / Betfair Exchange:
# Spread mediano verificado empiricamente no ARKAD: 1.075x (7.5% sobre a odd da casa)
SPREAD = 1.075
COMISSAO_ORBITX = 0.03  # 3% no OrbitX (corretora)

df["Odd_D_Lay"] = (df["Odd_D_FT"] * SPREAD).round(2)
df["Odd_H_Lay"] = (df["Odd_H_FT"] * SPREAD).round(2)
df["Odd_A_Lay"] = (df["Odd_A_FT"] * SPREAD).round(2)

# -------------------------------------------------------------
# 1. MÉTODO 1: LAY DRAW NO SUPER FAVORITO (ARKAD CANÔNICO)
# -------------------------------------------------------------
# - Ligas: min(Odd_H, Odd_A) <= 1.40
# - Copas: SOMENTE Favorito Mandante (Odd_H <= 1.40)
# - Odd_D_Lay entre 4.5 e 10.0
c1_liga = (~df["eh_copa"]) & (np.minimum(df["Odd_H_FT"], df["Odd_A_FT"]) <= 1.40)
c1_copa = (df["eh_copa"]) & (df["Odd_H_FT"] <= 1.40)
m1_cond = (c1_liga | c1_copa) & (df["Odd_D_Lay"] >= 4.5) & (df["Odd_D_Lay"] <= 10.0)
m1 = df[m1_cond].copy()

m1["green"] = m1["Goals_H_FT"] != m1["Goals_A_FT"]
m1["liability"] = m1["Odd_D_Lay"] - 1.0
m1["pnl_u"] = np.where(m1["green"], 1.0 - COMISSAO_ORBITX, -m1["liability"])
m1["pnl_liab"] = np.where(m1["green"], (1.0 - COMISSAO_ORBITX) / m1["liability"], -1.0)
m1["be_wr"] = m1["liability"] / (m1["liability"] + (1.0 - COMISSAO_ORBITX))

# -------------------------------------------------------------
# 2. MÉTODO 2: LAY HOME NO FAVORITO VISITANTE (ARKAD CANÔNICO)
# -------------------------------------------------------------
# - Odd_A_FT <= 1.65
# - Odd_H_Lay entre 2.0 e 10.0
m2_cond = (df["Odd_A_FT"] <= 1.65) & (df["Odd_H_Lay"] >= 2.0) & (df["Odd_H_Lay"] <= 10.0)
m2 = df[m2_cond].copy()

m2["green"] = m2["Goals_A_FT"] >= m2["Goals_H_FT"]
m2["liability"] = m2["Odd_H_Lay"] - 1.0
m2["pnl_u"] = np.where(m2["green"], 1.0 - COMISSAO_ORBITX, -m2["liability"])
m2["pnl_liab"] = np.where(m2["green"], (1.0 - COMISSAO_ORBITX) / m2["liability"], -1.0)
m2["be_wr"] = m2["liability"] / (m2["liability"] + (1.0 - COMISSAO_ORBITX))

# -------------------------------------------------------------
# 3. MÉTODO 3: LAY AWAY NO FAVORITO MANDANTE (TESTE DE ESTUDO)
# -------------------------------------------------------------
# - Odd_H_FT <= 1.40
# - Odd_A_Lay entre 5.0 e 25.0
m3_cond = (df["Odd_H_FT"] <= 1.40) & (df["Odd_A_Lay"] >= 5.0) & (df["Odd_A_Lay"] <= 25.0)
m3 = df[m3_cond].copy()

m3["green"] = m3["Goals_H_FT"] >= m3["Goals_A_FT"]
m3["liability"] = m3["Odd_A_Lay"] - 1.0
m3["pnl_u"] = np.where(m3["green"], 1.0 - COMISSAO_ORBITX, -m3["liability"])
m3["pnl_liab"] = np.where(m3["green"], (1.0 - COMISSAO_ORBITX) / m3["liability"], -1.0)
m3["be_wr"] = m3["liability"] / (m3["liability"] + (1.0 - COMISSAO_ORBITX))

def relatorio(nome, sub):
    n = len(sub)
    if n == 0:
        return
    wr = sub["green"].mean() * 100
    be = sub["be_wr"].mean() * 100
    edge_wr = wr - be
    roi_liab = sub["pnl_liab"].mean() * 100
    pnl_total_u = sub["pnl_u"].sum()
    odd_med = sub["liability"].median() + 1.0
    
    # Calcular Max Drawdown em unidades
    cum_pnl = sub["pnl_u"].cumsum()
    peak = cum_pnl.cummax()
    dd = cum_pnl - peak
    max_dd = dd.min()

    print(f"\n==================================================================")
    print(f"ESTATISTICAS: {nome}")
    print(f"==================================================================")
    print(f"Amostra Total: {n:,} apostas")
    print(f"Taxa de Acerto (WR): {wr:.2f}% | Break-Even WR: {be:.2f}% | Alpha Real: {edge_wr:+.2f} pp")
    print(f"Odd Lay Mediana: {odd_med:.2f} (Liability media em risco: {odd_med - 1.0:.2f}u)")
    print(f"ROI sobre Capital em Risco (Liability): {roi_liab:+.2f}%")
    print(f"Lucro Acumulado: {pnl_total_u:+.2f}u de ganho nominal")
    print(f"Max Drawdown Historico: {max_dd:.2f}u")

    print("\nQuebra Temporal (Consistencia Ano a Ano):")
    for ano, g in sub.groupby("Ano"):
        n_a = len(g)
        wr_a = g["green"].mean() * 100
        roi_a = g["pnl_liab"].mean() * 100
        pnl_a = g["pnl_u"].sum()
        print(f"  Ano {ano}: N={n_a:,} | WR={wr_a:.2f}% | ROI/liab={roi_a:+.2f}% | PnL={pnl_a:+.1f}u")

relatorio("METODO 1: LAY DRAW NO SUPER FAVORITO (ORBITX 3% COMISSAO)", m1)
relatorio("METODO 2: LAY HOME NO FAVORITO VISITANTE (ORBITX 3% COMISSAO)", m2)
relatorio("METODO 3: LAY AWAY NO FAVORITO MANDANTE (ORBITX 3% COMISSAO)", m3)

# Sensibilidade ao Spread do OrbitX
print("\n==================================================================")
print("TESTE DE ESTRESSE: SENSIBILIDADE AO SPREAD DA EXCHANGE (ORBITX)")
print("==================================================================")
for sp in [1.05, 1.075, 1.10, 1.12]:
    sub_d = df[m1_cond].copy()
    sub_d["odd_lay"] = (sub_d["Odd_D_FT"] * sp).round(2)
    sub_d["green"] = sub_d["Goals_H_FT"] != sub_d["Goals_A_FT"]
    sub_d["liab"] = sub_d["odd_lay"] - 1.0
    sub_d["pnl_l"] = np.where(sub_d["green"], (1.0 - 0.03) / sub_d["liab"], -1.0)
    roi_d = sub_d["pnl_l"].mean() * 100
    
    sub_h = df[m2_cond].copy()
    sub_h["odd_lay"] = (sub_h["Odd_H_FT"] * sp).round(2)
    sub_h["green"] = sub_h["Goals_A_FT"] >= sub_h["Goals_H_FT"]
    sub_h["liab"] = sub_h["odd_lay"] - 1.0
    sub_h["pnl_l"] = np.where(sub_h["green"], (1.0 - 0.03) / sub_h["liab"], -1.0)
    roi_h = sub_h["pnl_l"].mean() * 100
    print(f"Spread {sp:.3f}x -> ROI Lay Draw: {roi_d:+.2f}% | ROI Lay Home: {roi_h:+.2f}%")
