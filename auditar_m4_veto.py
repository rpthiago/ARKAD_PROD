# -*- coding: utf-8 -*-
"""auditar_m4_veto.py — reproducao independente do Modelo 4 (veto por xG_open no Lay 0x0)."""
import numpy as np, pandas as pd

JAN, MIN_HIST, COMM = 8, 5, 0.05

d = pd.read_csv("hist_time_stats.csv", dtype={"eventid": str})
d["eventid"] = d.eventid.fillna("").str.replace(r"\.0$", "", regex=True)
d = d[(d.tem_stats == 1) & (d.eventid != "")].copy()
for c in d.columns:
    if c not in ("data", "liga", "home", "away", "eventid"):
        d[c] = pd.to_numeric(d[c], errors="coerce")
d["data"] = pd.to_datetime(d.data)

# rolling de xg_open por time, shift(1) — mesma construcao descrita
linhas = []
for _, r in d.iterrows():
    for lado in ("h", "a"):
        linhas.append(dict(data=r.data, eventid=r.eventid,
                           time=r.home if lado == "h" else r.away,
                           casa=1 if lado == "h" else 0,
                           xg_open=r["xg_open_%s" % lado]))
t = pd.DataFrame(linhas).sort_values(["time", "data"]).reset_index(drop=True)
g = t.groupby("time")
t["r_xg_open"] = g.xg_open.transform(lambda s: s.shift(1).rolling(JAN, min_periods=MIN_HIST).mean())
t["n_hist"] = g.cumcount()
H = t[t.casa == 1][["eventid", "r_xg_open", "n_hist"]].rename(
    columns={"r_xg_open": "rH", "n_hist": "nH"})
A = t[t.casa == 0][["eventid", "r_xg_open", "n_hist"]].rename(
    columns={"r_xg_open": "rA", "n_hist": "nA"})
m = d.merge(H, on="eventid").merge(A, on="eventid")
m = m[(m.nH >= MIN_HIST) & (m.nA >= MIN_HIST)].copy()
m["soma"] = m.rH + m.rA
m["zero"] = ((m.gols_h == 0) & (m.gols_a == 0)).astype(int)

faixa = m[(m.odd_cs_0x0 >= 10) & (m.odd_cs_0x0 <= 20)].dropna(subset=["soma"]).sort_values("data")
print("=== reproducao da faixa do Lay 0x0 (odd_cs_0x0 entre 10 e 20) ===")
print("  N=%d | reds(0-0)=%d (%.2f%%) | odd media=%.2f"
      % (len(faixa), faixa.zero.sum(), 100 * faixa.zero.mean(), faixa.odd_cs_0x0.mean()))


def pnl(sub):
    """LAY: green = +0,95 por stake; red = -(odd-1). ROI sobre liability media."""
    liab = (sub.odd_cs_0x0 - 1)
    p = np.where(sub.zero == 1, -(sub.odd_cs_0x0 - 1), (1 - COMM))
    return p.sum() / liab.sum() * 100, p.sum()


roi, tot = pnl(faixa)
print("  ROI sobre liability = %+.2f%% | PnL = %+.1f unidades" % (roi, tot))

print("\n=== o corte 1.80 aplicado a AMOSTRA INTEIRA ===")
ap, ve = faixa[faixa.soma >= 1.80], faixa[faixa.soma < 1.80]
for nome, s in (("aprovados (>=1.80)", ap), ("vetados (<1.80)", ve)):
    if len(s):
        r, _ = pnl(s)
        print("  %-20s N=%4d | reds=%3d (%.2f%%) | ROI=%+.2f%% | odd media=%.2f"
              % (nome, len(s), s.zero.sum(), 100 * s.zero.mean(), r, s.odd_cs_0x0.mean()))

print("\n=== O CORTE 1.80 E ESPECIAL? varredura de limiares na amostra inteira ===")
print("  %-8s %6s %8s %10s" % ("corte", "N_apr", "reds%", "ROI_apr"))
for corte in (1.40, 1.50, 1.60, 1.70, 1.80, 1.90, 2.00, 2.10, 2.20):
    s = faixa[faixa.soma >= corte]
    if len(s) > 100:
        r, _ = pnl(s)
        print("  %-8.2f %6d %7.2f%% %+9.2f%%" % (corte, len(s), 100 * s.zero.mean(), r))

print("\n=== split temporal 50/50 (validacao OOS) ===")
corte_data = faixa.data.quantile(0.5)
tr, va = faixa[faixa.data < corte_data], faixa[faixa.data >= corte_data]
print("  treino ate %s (N=%d) | validacao (N=%d)" % (corte_data.date(), len(tr), len(va)))
rbase, _ = pnl(va)
apv, vev = va[va.soma >= 1.80], va[va.soma < 1.80]
rap, _ = pnl(apv); rve, _ = pnl(vev)
print("  base sem filtro (OOS): N=%d reds=%d (%.2f%%) ROI=%+.2f%%"
      % (len(va), va.zero.sum(), 100 * va.zero.mean(), rbase))
print("  aprovados            : N=%d reds=%d (%.2f%%) ROI=%+.2f%% | odd media=%.2f"
      % (len(apv), apv.zero.sum(), 100 * apv.zero.mean(), rap, apv.odd_cs_0x0.mean()))
print("  vetados              : N=%d reds=%d (%.2f%%) ROI=%+.2f%% | odd media=%.2f"
      % (len(vev), vev.zero.sum(), 100 * vev.zero.mean(), rve, vev.odd_cs_0x0.mean()))
print("  delta de ROI = %+.2f pp" % (rap - rbase))

rng = np.random.default_rng(3)
b = []
for _ in range(10000):
    i = rng.integers(0, len(va), len(va))
    s = va.iloc[i]
    a2 = s[s.soma >= 1.80]
    if len(a2) > 30:
        b.append(pnl(a2)[0] - pnl(s)[0])
b = np.sort(b)
print("  bootstrap IC95 do delta: [%+.2f, %+.2f] pp | P(<=0) = %.4f"
      % (b[250], b[9750], (np.array(b) <= 0).mean()))

print("\n=== o corte escolhido no TREINO, aplicado as cegas na VALIDACAO ===")
melhor, mroi = None, None
for c2 in np.arange(1.0, 2.6, 0.05):
    s = tr[tr.soma >= c2]
    if len(s) > 150:
        r, _ = pnl(s)
        if mroi is None or r > mroi:
            melhor, mroi = c2, r
print("  melhor corte NO TREINO = %.2f (ROI treino %+.2f%%)" % (melhor, mroi))
s = va[va.soma >= melhor]
r, _ = pnl(s)
print("  esse corte na VALIDACAO: N=%d reds=%.2f%% ROI=%+.2f%% (base %+.2f%%) delta=%+.2f pp"
      % (len(s), 100 * s.zero.mean(), r, rbase, r - rbase))

print("\n=== confundidor: o veto seleciona ODD diferente? ===")
print("  odd media aprovados=%.2f | vetados=%.2f | diferenca=%.2f"
      % (ap.odd_cs_0x0.mean(), ve.odd_cs_0x0.mean(), ap.odd_cs_0x0.mean() - ve.odd_cs_0x0.mean()))
print("  taxa de 0-0 esperada pela odd (1/odd): aprovados=%.2f%% | vetados=%.2f%%"
      % (100 * (1 / ap.odd_cs_0x0).mean(), 100 * (1 / ve.odd_cs_0x0).mean()))

print("\n" + "=" * 74)
print("CHECAGEM 1 — LEI Nº1 DO GEMINI.md: a odd usada e de BACK ou de LAY?")
print("=" * 74)
print("  `Odd_CS_0x0` da base Bet365 e a odd de BACK. O Lay 0x0 executa na LAY da Betfair.")
print("  Memoria do projeto: gap back->lay em Correct Score = +40%% a +70%%; coletor mediu 1,56x.")
for fator, rot in ((1.0, "como no relatorio (odd de BACK)"), (1.40, "lay = 1,40x back"),
                   (1.56, "lay = 1,56x back (medido no coletor)"), (1.70, "lay = 1,70x back")):
    f2 = faixa.copy()
    f2["odd_cs_0x0"] = f2.odd_cs_0x0 * fator
    r_all, _ = pnl(f2)
    a2 = f2[f2.soma >= 1.80]
    r_ap, _ = pnl(a2)
    print("  %-34s ROI base=%+7.2f%% | ROI aprovados=%+7.2f%% | delta=%+.2f pp"
          % (rot, r_all, r_ap, r_ap - r_all))

print("\n" + "=" * 74)
print("CHECAGEM 2 — o xG_open prediz 0-0 ALEM do preco? (o unico teste que importa)")
print("=" * 74)
import math
from math import erfc
def logit(X, y):
    X = np.column_stack([np.ones(len(X)), X]); b = np.zeros(X.shape[1])
    for _ in range(80):
        eta = np.clip(X @ b, -30, 30); mu = 1/(1+np.exp(-eta))
        W = np.clip(mu*(1-mu), 1e-9, None); z = eta + (y-mu)/W
        try: nb = np.linalg.solve((X.T*W)@X, (X.T*W)@z)
        except np.linalg.LinAlgError: break
        if np.max(np.abs(nb-b)) < 1e-10: b = nb; break
        b = nb
    eta = np.clip(X @ b, -30, 30); mu = 1/(1+np.exp(-eta))
    W = np.clip(mu*(1-mu), 1e-9, None)
    se = np.sqrt(np.diag(np.linalg.pinv((X.T*W)@X)))
    return b, se
for nome, s in (("amostra inteira", faixa), ("so a VALIDACAO (OOS)", va)):
    imp = np.clip(1/s.odd_cs_0x0.values, 1e-4, 1-1e-4)
    X = np.column_stack([np.log(imp/(1-imp)), s.soma.values])
    b, se = logit(X, s.zero.values)
    z = b[2]/se[2]; p = erfc(abs(z)/math.sqrt(2))
    print("  %-22s N=%4d | coef(xg_open)=%+.4f  erro=%.4f  z=%+.2f  p=%.4f  %s"
          % (nome, len(s), b[2], se[2], z, p, "SIGNIFICATIVO" if p < 0.05 else "nao significativo"))
    print("  %-22s coef(preco)=%+.4f (p=%.4f)" % ("", b[1], erfc(abs(b[1]/se[1])/math.sqrt(2))))
