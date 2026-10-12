# -*- coding: utf-8 -*-
import sys, os, re
sys.stdout.reconfigure(encoding="utf-8")
import pandas as pd, numpy as np
exec(open(os.path.join(sys.argv[1],"hip456.py"), encoding="utf-8").read().split('CORTE = "2026-05-15"')[0])
D["largo"] = D["spread"] > 1.15
print("O spread largo e so um apelido para 'liga obscura'?\n")
print("=== ligas com mais jogos de spread largo ===")
t = D.groupby("League").agg(n=("red","size"), largos=("largo","sum"))
t["%largo"] = (100*t["largos"]/t["n"]).round(0)
print(t[t["n"]>=15].sort_values("%largo", ascending=False).head(10).to_string())
print()
print("=== dentro das ligas GRANDES, o filtro ainda funciona? ===")
grandes = t[t["n"]>=25].index
G = D[D["League"].isin(grandes)]
for rot, g in (("liga grande, spread <= 1.15", G[~G["largo"]]), ("liga grande, spread > 1.15", G[G["largo"]])):
    if len(g) < 15: continue
    print("  %-34s n=%-4d WR %6.2f%% | ROI %+6.2f%%" % (rot, len(g), 100*(1-g["red"].mean()), 100*g["pnl"].mean()))
print()
print("=== o spread se correlaciona com a odd de lay? (poderia ser o teto disfarcado) ===")
print("  correlacao spread x odd_lay: %.3f" % D[["spread","Odd_D_Lay"]].corr().iloc[0,1])
for rot, m in (("odd lay <= 7.0", D["Odd_D_Lay"]<=7.0), ("odd lay > 7.0", D["Odd_D_Lay"]>7.0)):
    g = D[m]
    a = g[~g["largo"]]; b = g[g["largo"]]
    print("  %-16s | spread ok: n=%-4d ROI %+6.2f%%  | spread largo: n=%-4d ROI %+6.2f%%"
          % (rot, len(a), 100*a["pnl"].mean(), len(b), 100*b["pnl"].mean() if len(b) else 0))
