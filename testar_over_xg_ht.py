# -*- coding: utf-8 -*-
"""
testar_over_xg_ht.py — testa a HIPOTESE 2 do Gemini na forma que o dado permite.

Tese: jogo 0-0 no intervalo MAS com muito xG criado (chances desperdicadas, goleiro inspirado,
trave) tem hazard de gol maior do que a odd sugere — a odd de Over inflou por decaimento de tempo.

Como o jogo esta 0-0 no HT, o total FT E o numero de gols do 2o tempo. Entao da para testar
direto: xG_total do 1o tempo prediz gols no 2o tempo, ALEM do preco pre-jogo do Over 2.5?

Zero requisicao — roda sobre hist_ht_stats.csv, ja coletado.
"""
import numpy as np, pandas as pd, math
from math import erfc

d = pd.read_csv("hist_ht_stats.csv")
d = d[d.fm_tem_stats == 1].copy()
for c in ("xg_h", "xg_a", "sot_h", "sot_a", "shots_h", "shots_a", "bigch_h", "bigch_a",
          "odd_over25", "gols_h_ft", "gols_a_ft"):
    d[c] = pd.to_numeric(d[c], errors="coerce")
d["gols_2t"] = d.gols_h_ft + d.gols_a_ft          # 0-0 no HT => total FT = gols do 2o tempo
d["xg_tot"] = d.xg_h + d.xg_a
d["sot_tot"] = d.sot_h + d.sot_a
d["bigch_tot"] = d.bigch_h + d.bigch_a

x = d.dropna(subset=["xg_tot", "gols_2t"]).copy()
print("N com xG do 1o tempo e resultado: %d" % len(x))
print("media de gols no 2o tempo: %.2f\n" % x.gols_2t.mean())

print("=== gols do 2o TEMPO por faixa de xG criado no 1o tempo (0-0 no intervalo) ===")
print("  %-14s %5s %8s %9s %9s %9s" % ("xG total 1T", "N", "gols med", "P(0 gol)", "P(2+)", "P(3+)"))
for lo, hi in [(0, .4), (.4, .8), (.8, 1.2), (1.2, 1.8), (1.8, 9)]:
    s = x[(x.xg_tot >= lo) & (x.xg_tot < hi)]
    if len(s) >= 20:
        print("  %-14s %5d %8.2f %8.0f%% %8.0f%% %8.0f%%"
              % ("%.1f-%.1f" % (lo, hi), len(s), s.gols_2t.mean(),
                 100 * (s.gols_2t == 0).mean(), 100 * (s.gols_2t >= 2).mean(),
                 100 * (s.gols_2t >= 3).mean()))

corr = np.corrcoef(x.xg_tot, x.gols_2t)[0, 1]
n = len(x)
t = corr * math.sqrt((n - 2) / max(1e-9, 1 - corr ** 2))
print("\n  correlacao xG_1T x gols_2T: r=%.3f  (p=%.3f)" % (corr, erfc(abs(t) / math.sqrt(2))))


def logit(X, y, nomes):
    X = np.column_stack([np.ones(len(X))] + [X[:, i] for i in range(X.shape[1])])
    beta = np.zeros(X.shape[1])
    for _ in range(80):
        eta = np.clip(X @ beta, -30, 30)
        mu = 1 / (1 + np.exp(-eta))
        W = np.clip(mu * (1 - mu), 1e-9, None)
        z_ = eta + (y - mu) / W
        XtW = X.T * W
        try:
            nb = np.linalg.solve(XtW @ X, XtW @ z_)
        except np.linalg.LinAlgError:
            break
        if np.max(np.abs(nb - beta)) < 1e-9:
            beta = nb; break
        beta = nb
    eta = np.clip(X @ beta, -30, 30)
    mu = 1 / (1 + np.exp(-eta))
    W = np.clip(mu * (1 - mu), 1e-9, None)
    se = np.sqrt(np.diag(np.linalg.pinv((X.T * W) @ X)))
    print("    %-26s %9s %8s %8s %8s" % ("termo", "coef", "erro", "z", "p"))
    for i, nm in enumerate(["(intercepto)"] + nomes):
        zz = beta[i] / se[i] if se[i] else 0
        pp = erfc(abs(zz) / math.sqrt(2))
        print("    %-26s %9.4f %8.4f %8.2f %8.3f%s"
              % (nm, beta[i], se[i], zz, pp, "  <<< significativo" if pp < 0.05 and i > 0 else ""))


print("\n=== O xG do 1o tempo adiciona informacao ALEM do preco de Over? ===")
p = x.dropna(subset=["odd_over25"])
p = p[p.odd_over25 > 1.01]
print("  (N=%d com odd de Over 2.5 na base)" % len(p))

print("\n  [1] alvo: SAIU PELO MENOS 1 GOL no 2o tempo")
logit(np.column_stack([np.log(p.odd_over25.values), p.xg_tot.values]),
      (p.gols_2t >= 1).astype(int).values, ["log(odd_Over2.5 pre)", "xG total do 1T"])

print("\n  [2] alvo: SAIRAM 2+ GOLS no 2o tempo")
logit(np.column_stack([np.log(p.odd_over25.values), p.xg_tot.values]),
      (p.gols_2t >= 2).astype(int).values, ["log(odd_Over2.5 pre)", "xG total do 1T"])

print("\n  [3] alvo: 1+ gol — usando chutes no alvo e grandes chances no lugar do xG")
q = p.dropna(subset=["sot_tot", "bigch_tot"])
logit(np.column_stack([np.log(q.odd_over25.values), q.sot_tot.values, q.bigch_tot.values]),
      (q.gols_2t >= 1).astype(int).values,
      ["log(odd_Over2.5 pre)", "chutes no alvo 1T", "grandes chances 1T"])
