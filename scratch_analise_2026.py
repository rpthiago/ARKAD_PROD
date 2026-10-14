# -*- coding: utf-8 -*-
import sys
import pandas as pd
import numpy as np

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

bf = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Betfair.csv", low_memory=False)
bf = bf.dropna(subset=["Date", "Odd_D_Lay", "Odd_D_Back", "Odd_H_Back", "Odd_A_Back", "Goals_H_FT", "Goals_A_FT"]).copy()
bf["Date"] = pd.to_datetime(bf["Date"], errors="coerce")
bf = bf.dropna(subset=["Date"])
bf["ano"] = bf["Date"].dt.year

# Spread D_Lay / D_Back
bf["spread_D"] = bf["Odd_D_Lay"] / bf["Odd_D_Back"]

print("Spread mediano Draw (Lay / Back) na base Betfair por ano:")
for ano, g in bf.groupby("ano"):
    med = g["spread_D"].median()
    p25 = g["spread_D"].quantile(0.25)
    p75 = g["spread_D"].quantile(0.75)
    print(f"Ano {ano}: N={len(g)} | Spread Mediano={med:.4f} | p25={p25:.4f} | p75={p75:.4f}")

# Super Fav <= 1.40
sf = bf[bf[["Odd_H_Back", "Odd_A_Back"]].min(axis=1) <= 1.40].copy()
sf["draw"] = sf["Goals_H_FT"] == sf["Goals_A_FT"]
print("\nSuper Fav <= 1.40 por ano (Lay Draw):")
for ano, g in sf.groupby("ano"):
    n = len(g)
    draw_pct = g["draw"].mean() * 100
    wr = 100 - draw_pct
    odd_lay_med = g["Odd_D_Lay"].median()
    be_3 = ((g["Odd_D_Lay"] - 1) / (g["Odd_D_Lay"] - 0.03)).mean() * 100
    pnl = np.where(~g["draw"], 0.97 / (g["Odd_D_Lay"] - 1), -1.0)
    print(f"Ano {ano}: N={n:4d} | Empates={draw_pct:5.2f}% | WR={wr:5.2f}% | BE={be_3:5.2f}% | OddLayMed={odd_lay_med:5.2f} | ROI(3%)={pnl.mean()*100:+6.2f}%")

print("\nSuper Fav <= 1.40 em 2026 Mês a Mês (Lay Draw):")
sf_2026 = sf[sf["ano"] == 2026].copy()
sf_2026["mes"] = sf_2026["Date"].dt.strftime("%Y-%m")
for mes, g in sf_2026.groupby("mes"):
    n = len(g)
    draw_pct = g["draw"].mean() * 100
    wr = 100 - draw_pct
    odd_lay_med = g["Odd_D_Lay"].median()
    be_3 = ((g["Odd_D_Lay"] - 1) / (g["Odd_D_Lay"] - 0.03)).mean() * 100
    pnl = np.where(~g["draw"], 0.97 / (g["Odd_D_Lay"] - 1), -1.0)
    print(f"Mes {mes}: N={n:3d} | Empates={draw_pct:5.2f}% | WR={wr:5.2f}% | BE={be_3:5.2f}% | OddLayMed={odd_lay_med:5.2f} | ROI(3%)={pnl.mean()*100:+6.2f}%")
