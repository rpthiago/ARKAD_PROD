# -*- coding: utf-8 -*-
"""
analisar_hist_xg.py — o teste que decide: as stats do 1o tempo carregam informacao
ALEM do preco? Roda sobre hist_ht_stats.csv (0-0 no intervalo, fav<=1.60, 180 dias).

  1) bootstrap IC95 do gap do filtro de pressao (dado LIMPO do FotMob)
  2) teste z de duas proporcoes: com pressao x sem pressao
  3) regressao logistica  venceu ~ log(odd_h) + X_HT   (IRLS na mao, sem statsmodels)
     -> o coeficiente de X_HT e significativo DADO o preco?

Controle = odd PRE-JOGO (a base nao tem preco in-play do intervalo). O teste com preco
in-play depende do xg_ht_logger acumular; este aqui e o primeiro corte.
"""
import numpy as np, pandas as pd, random, math

d = pd.read_csv("hist_ht_stats.csv")
lim = d[d.fm_tem_stats == 1].copy()
for c in ("xg_h", "xg_a", "sot_h", "sot_a", "corners_h", "corners_a", "bigch_h",
          "touches_box_h", "shots_h", "odd_h"):
    lim[c] = pd.to_numeric(lim[c], errors="coerce")
lim["press"] = ((lim.sot_h >= 3) | (lim.corners_h >= 4)).astype(int)
y = lim.venceu_mandante.values

print("N com stats limpas: %d  (dos quais com xG: %d)" % (len(lim), lim.xg_h.notna().sum()))
print()

# ---------------------------------------------------------------- 1) bootstrap do gap
random.seed(11)
a = lim[lim.press == 1].venceu_mandante.values
b = lim[lim.press == 0].venceu_mandante.values
boot = []
for _ in range(10000):
    sa = np.random.choice(a, len(a), replace=True)
    sb = np.random.choice(b, len(b), replace=True)
    boot.append(100 * (sa.mean() - sb.mean()))
boot.sort()
print("=== 1) FILTRO DE PRESSAO (SoT>=3 ou escanteios>=4), dado LIMPO ===")
print("  com pressao : N=%3d  WR=%.2f%%" % (len(a), 100 * a.mean()))
print("  sem pressao : N=%3d  WR=%.2f%%" % (len(b), 100 * b.mean()))
print("  diferenca   : %+.2f pp   IC95 bootstrap [%+.2f, %+.2f]"
      % (100 * (a.mean() - b.mean()), boot[250], boot[9750]))

# ---------------------------------------------------------------- 2) teste z
p = (a.sum() + b.sum()) / (len(a) + len(b))
se = math.sqrt(p * (1 - p) * (1 / len(a) + 1 / len(b)))
z = (a.mean() - b.mean()) / se if se else 0
from math import erfc
pv = erfc(abs(z) / math.sqrt(2))
print("  teste z     : z=%.3f  p=%.3f  -> %s" % (z, pv, "significativo" if pv < 0.05 else "NAO significativo"))
print()


# ---------------------------------------------------------------- 3) logistica (IRLS)
def logit(X, y, nomes, iters=60):
    X = np.column_stack([np.ones(len(X))] + [X[:, i] for i in range(X.shape[1])])
    beta = np.zeros(X.shape[1])
    for _ in range(iters):
        eta = X @ beta
        mu = 1 / (1 + np.exp(-np.clip(eta, -30, 30)))
        W = np.clip(mu * (1 - mu), 1e-9, None)
        z_ = eta + (y - mu) / W
        XtW = X.T * W
        try:
            beta_novo = np.linalg.solve(XtW @ X, XtW @ z_)
        except np.linalg.LinAlgError:
            break
        if np.max(np.abs(beta_novo - beta)) < 1e-9:
            beta = beta_novo; break
        beta = beta_novo
    eta = X @ beta
    mu = 1 / (1 + np.exp(-np.clip(eta, -30, 30)))
    W = np.clip(mu * (1 - mu), 1e-9, None)
    cov = np.linalg.pinv((X.T * W) @ X)
    se_ = np.sqrt(np.diag(cov))
    print("    %-22s %9s %8s %8s %8s" % ("termo", "coef", "erro", "z", "p"))
    for i, nm in enumerate(["(intercepto)"] + nomes):
        zz = beta[i] / se_[i] if se_[i] else 0
        pp = erfc(abs(zz) / math.sqrt(2))
        marca = "  <<< significativo" if (pp < 0.05 and i > 0) else ""
        print("    %-22s %9.4f %8.4f %8.2f %8.3f%s" % (nm, beta[i], se_[i], zz, pp, marca))
    return beta


print("=== 3) REGRESSAO LOGISTICA — X_HT adiciona informacao ALEM do preco? ===")
base = lim[(lim.odd_h.notna()) & (lim.odd_h > 1.01)].copy()   # odd 0/vazia na base quebra o log()

print("\n  [a] so o preco (referencia)")
logit(np.column_stack([np.log(base.odd_h.values)]),
      base.venceu_mandante.values, ["log(odd_pre)"])
print("    (N=%d)" % len(base))

print("\n  [b] preco + chutes no alvo e escanteios do 1o tempo")
sub = base.dropna(subset=["odd_h", "sot_h", "corners_h"])
logit(np.column_stack([np.log(sub.odd_h.values), sub.sot_h.values, sub.corners_h.values]),
      sub.venceu_mandante.values, ["log(odd_pre)", "SoT mandante 1T", "escanteios 1T"])

print("\n  [c] preco + xG do 1o tempo (so jogos que tem xG)")
sx = base.dropna(subset=["odd_h", "xg_h", "xg_a"])
if len(sx) > 60:
    logit(np.column_stack([np.log(sx.odd_h.values), sx.xg_h.values, sx.xg_a.values]),
          sx.venceu_mandante.values, ["log(odd_pre)", "xG mandante 1T", "xG visitante 1T"])
    print("    (N=%d)" % len(sx))

print("\n  [d] preco + diferenca de xG (mandante - visitante)")
if len(sx) > 60:
    logit(np.column_stack([np.log(sx.odd_h.values), (sx.xg_h - sx.xg_a).values]),
          sx.venceu_mandante.values, ["log(odd_pre)", "xG_H - xG_A no 1T"])
