# -*- coding: utf-8 -*-
"""auditar_liquidacao_77.py — auditoria forense da liquidacao dos 77 jogos (03/09 a 09/09)."""
import pandas as pd, numpy as np

CSV = "metodos_aprovados/Sinais_Metodos_Aprovados_Odds_Reais_Betfair.csv"
d = pd.read_csv(CSV)
d.columns = [c.strip().replace("﻿", "") for c in d.columns]
d["Data"] = pd.to_datetime(d["Data"], errors="coerce", dayfirst=True)
for c in ("Odd_Entrada", "PnL_u"):
    d[c] = pd.to_numeric(d[c], errors="coerce")

print("=== BASE MESTRE ===")
print("  linhas: %d | de %s a %s" % (len(d), d.Data.min().date(), d.Data.max().date()))
print("  status: %s" % d["Status"].value_counts().to_dict())
print("  PnL total declarado: %+.2f u | soma real da coluna PnL_u: %+.2f u"
      % (30.07, d.PnL_u.sum()))

jan = d[(d.Data >= "2026-09-03") & (d.Data <= "2026-09-09")]
print("\n=== A JANELA AUDITADA (03/09 a 09/09) ===")
print("  N=%d (declarado: 77)" % len(jan))
print("  resultado: %s" % jan["Resultado"].value_counts().to_dict())
print("  PnL da janela: %+.2f u (declarado: +17,80)" % jan.PnL_u.sum())

print("\n=== 1. INTEGRIDADE DA FORMULA DE LAY ===")
print("   esperado: GREEN = +(1 - comissao) | RED = -(odd - 1)")
lay = d[d["Lado"].astype(str).str.upper().str.contains("LAY", na=False)]
print("   linhas de LAY: %d de %d" % (len(lay), len(d)))
prob = []
for _, r in lay.iterrows():
    if pd.isna(r.PnL_u) or pd.isna(r.Odd_Entrada):
        continue
    res = str(r["Resultado"]).upper()
    if "GREEN" in res:
        esp = [0.95, 0.955]           # comissao 5% ou 4,5%
        ok = any(abs(r.PnL_u - e) < 0.011 for e in esp)
    elif "RED" in res:
        ok = abs(r.PnL_u - (-(r.Odd_Entrada - 1))) < 0.011
    else:
        continue
    if not ok:
        prob.append((r["Data"], r["Jogo"], r["Método"], res, r.Odd_Entrada, r.PnL_u))
print("   linhas com PnL FORA da formula: %d" % len(prob))
for x in prob[:10]:
    print("     %s | %-30s | %-16s | %s odd=%.2f pnl=%+.3f"
          % (str(x[0])[:10], str(x[1])[:30], str(x[2])[:16], x[3], x[4], x[5]))

print("\n=== 2. RISCO DE FALSE GREEN (mercados sensiveis a gol tardio) ===")
sens = d[d["Mercado"].astype(str).str.contains("CS|Correct|Under|0x0|0x1|2x2|0x3", case=False, na=False)]
print("   linhas em mercado sensivel: %d" % len(sens))
if len(sens):
    print("   por mercado: %s" % sens["Mercado"].value_counts().head(8).to_dict())
    js = sens[(sens.Data >= "2026-09-03") & (sens.Data <= "2026-09-09")]
    print("   dentro da janela dos 77: %d | greens: %d | reds: %d"
          % (len(js), js["Resultado"].astype(str).str.upper().str.contains("GREEN").sum(),
             js["Resultado"].astype(str).str.upper().str.contains("RED").sum()))

print("\n=== 3. O PLACAR ESTA PREENCHIDO EM TODOS OS LIQUIDADOS? ===")
liq = d[d["Status"].astype(str).str.contains("LIQUID|Finaliz", case=False, na=False)]
sem_placar = liq[liq["Placar"].isna() | (liq["Placar"].astype(str).str.strip().isin(["", "nan", "-"]))]
print("   liquidados: %d | SEM placar registrado: %d" % (len(liq), len(sem_placar)))
for _, r in sem_placar.head(6).iterrows():
    print("     %s | %s | %s" % (str(r["Data"])[:10], str(r["Jogo"])[:34], r["Método"]))

print("\n=== 4. WR x BREAK-EVEN (a armadilha do Idea1: media engana) ===")
for met, g in d.groupby("Método"):
    gg = g[g["Resultado"].astype(str).str.upper().str.contains("GREEN|RED", na=False)]
    if len(gg) < 8:
        continue
    wr = 100 * gg["Resultado"].astype(str).str.upper().str.contains("GREEN").mean()
    odd = gg.Odd_Entrada.dropna()
    be = 100 * np.mean((odd - 1) / (odd - 0.05)) if len(odd) else np.nan
    print("   %-28s N=%3d WR=%6.2f%% BE=%6.2f%% gap=%+6.2f pp | PnL=%+7.2f u"
          % (str(met)[:28], len(gg), wr, be, wr - be, gg.PnL_u.sum()))
