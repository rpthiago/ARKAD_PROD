# -*- coding: utf-8 -*-
"""
gerar_backtest_saldo_menor_eh2.py — backtest DEFINITIVO do "Saldo Menor EH+2".
Bet: BACK no FAVORITO +2 (European Handicap). RED se a ZEBRA vence por 2+ (favorito perde por 2+).
Usa a ODD REAL do EH+2 da base (EH_H_pos_2 / EH_A_pos_2 do favorito, Bet365).
Gera Backtest_Saldo_Menor_EH2_ODDREAL_2026.xlsx.
"""
import sys, numpy as np, pandas as pd
from pathlib import Path
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass
ROOT = Path(__file__).resolve().parent
BASE = ROOT / "Bases_de_Dados_API_FutPythonTrader_Bet365.csv"
OUT = ROOT / "Backtest_Saldo_Menor_EH2_TOP2_ODDREAL_2026.xlsx"
COMM = 0.045

cols = ["League", "Date", "Time", "Home", "Away", "Goals_H_FT", "Goals_A_FT",
        "Odd_H_FT", "Odd_D_FT", "Odd_A_FT", "Odd_Under25_FT", "EH_H_pos_2", "EH_A_neg_2"]
d = pd.read_csv(BASE, usecols=lambda c: c in cols, low_memory=False)
d["Date"] = pd.to_datetime(d["Date"], errors="coerce")
d = d[d["Date"] >= "2026-01-01"].copy()
for c in ["Goals_H_FT", "Goals_A_FT", "Odd_H_FT", "Odd_D_FT", "Odd_A_FT", "Odd_Under25_FT", "EH_H_pos_2", "EH_A_neg_2"]:
    d[c] = pd.to_numeric(d[c], errors="coerce")
d = d.dropna(subset=["Odd_H_FT", "Odd_A_FT", "Odd_D_FT", "Odd_Under25_FT", "Goals_H_FT", "Goals_A_FT"])

d["is_home_zebra"] = d["Odd_H_FT"] > d["Odd_A_FT"]
d["zebra_odd"] = np.where(d["is_home_zebra"], d["Odd_H_FT"], d["Odd_A_FT"])
d["fav_odd"] = np.where(d["is_home_zebra"], d["Odd_A_FT"], d["Odd_H_FT"])
# odd executavel = EH +2 do FAVORITO (home favorito -> EH_H_pos_2 ; away favorito -> EH_A_neg_2)
d["odd_exec"] = np.where(d["is_home_zebra"], d["EH_A_neg_2"], d["EH_H_pos_2"])
d["Total_xG"] = (1.35 + (d["Odd_Under25_FT"] - 1.50) * 1.75).clip(0.80, 5.50).round(2)

f = d[(d["fav_odd"] >= 2.00) & (d["fav_odd"] <= 5.00) & (d["Odd_D_FT"] <= 3.42)
      & (d["zebra_odd"] >= 2.22) & (d["Total_xG"] <= 2.00)
      & d["odd_exec"].notna() & (d["odd_exec"] > 1.0)].copy()
# TOP 2 do dia: menor Total_xG, desempate menor Odd_Under25_FT
f["dia"] = f["Date"].dt.date
t = (f.sort_values(["dia", "Total_xG", "Odd_Under25_FT"]).groupby("dia").head(2)
     .sort_values(["Date", "Time"]).copy())

# liquidacao: RED se a ZEBRA vence por 2+ (= favorito perde por 2+); GREEN no resto
gh, ga = t["Goals_H_FT"], t["Goals_A_FT"]
green = np.where(t["is_home_zebra"], (gh - ga) <= 1, (ga - gh) <= 1)
t["Resultado"] = np.where(green, "GREEN", "RED")
t["P&L (u)"] = np.where(green, (t["odd_exec"] - 1) * (1 - COMM), -1.0).round(4)

t["Zebra Apostada"] = np.where(t["is_home_zebra"], t["Home"], t["Away"])
t["Favorito (apostado +2)"] = np.where(t["is_home_zebra"], t["Away"], t["Home"])
t["Placar FT"] = gh.astype(int).astype(str) + "-" + ga.astype(int).astype(str)
t["P&L (R$)"] = (t["P&L (u)"] * 100).round(2)
t["Lucro Acum (u)"] = t["P&L (u)"].cumsum().round(3)
t["Lucro Acum (R$)"] = (t["Lucro Acum (u)"] * 100).round(2)
t["_g"] = (t["Resultado"] == "GREEN").astype(int)
t["WR Acum %"] = (t["_g"].cumsum() / (np.arange(len(t)) + 1) * 100).round(1)
aba1 = pd.DataFrame({
    "Nº": np.arange(1, len(t) + 1), "Data": t["Date"].dt.strftime("%Y-%m-%d"), "Horário": t["Time"],
    "Liga": t["League"], "Mandante": t["Home"], "Visitante": t["Away"], "Zebra": t["Zebra Apostada"],
    "Favorito (+2)": t["Favorito (apostado +2)"], "Placar FT": t["Placar FT"], "xG Total": t["Total_xG"],
    "Odd Empate": t["Odd_D_FT"].round(2), "Linha": "Favorito EH +2", "Odd Execução (real)": t["odd_exec"].round(2),
    "Resultado": t["Resultado"], "P&L (u)": t["P&L (u)"], "P&L (R$)": t["P&L (R$)"],
    "Lucro Acum (u)": t["Lucro Acum (u)"], "Lucro Acum (R$)": t["Lucro Acum (R$)"], "WR Acum %": t["WR Acum %"]})

t["Mes"] = t["Date"].dt.strftime("%Y-%m")
rows = []
for mes, g in t.groupby("Mes"):
    n = len(g); gr = int((g["Resultado"] == "GREEN").sum()); lu = g["P&L (u)"].sum()
    rows.append({"Mês": mes, "Apostas": n, "Greens": gr, "Reds": n - gr, "Win Rate %": round(gr / n * 100, 1),
                 "Odd média": round(g["odd_exec"].mean(), 3), "Lucro (u)": round(lu, 2),
                 "Lucro (R$)": round(lu * 100, 2), "ROI %": round(lu / n * 100, 1)})
aba2 = pd.DataFrame(rows)

with pd.ExcelWriter(OUT, engine="openpyxl") as w:
    aba1.to_excel(w, index=False, sheet_name="Jogos_2026")
    aba2.to_excel(w, index=False, sheet_name="Resumo_Mensal")

N = len(t); G = int(t["_g"].sum()); WR = G / N * 100; LU = t["P&L (u)"].sum(); ROI = LU / N * 100
odm = t["odd_exec"].mean(); be = 1 / (1 + (odm - 1) * (1 - COMM)) * 100
print("=== BACKTEST Saldo Menor (Favorito EH+2) — ODD REAL da base ===")
print("Apostas=%d | Greens=%d Reds=%d | WR=%.1f%% | odd real media=%.3f | break-even=%.1f%% (margem %+.1f%%)"
      % (N, G, N - G, WR, odm, be, WR - be))
print("Lucro=%+.2fu (R$ %+.0f) | ROI=%+.2f%%" % (LU, LU * 100, ROI))
print("meses positivos: %d/%d" % ((aba2["Lucro (u)"] > 0).sum(), len(aba2)))
for _, r in aba2.iterrows():
    print("  %s: N=%d WR=%.1f%% odd_med=%.3f ROI=%+.1f%%" % (r["Mês"], r["Apostas"], r["Win Rate %"], r["Odd média"], r["ROI %"]))
print("Excel:", OUT)
