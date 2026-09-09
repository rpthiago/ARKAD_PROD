# -*- coding: utf-8 -*-
"""auditar_janela_xg12.py — auditoria do experimento de janelas de xG (K=12) no Lay 0x0."""
import numpy as np, pandas as pd, unicodedata, re, math
from math import erfc

COMM = 0.05


def _canon(s):
    if pd.isna(s) or not s:
        return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)


def montar(K=12, minp=7):
    d = pd.read_csv("hist_time_stats_expandido.csv", low_memory=False)
    d["data_dt"] = pd.to_datetime(d["data"], errors="coerce")
    d["data_str"] = d.data_dt.dt.strftime("%Y-%m-%d")
    d["c_home"] = d.home.map(_canon); d["c_away"] = d.away.map(_canon)
    rows = []
    for idx, r in d.iterrows():
        rows.append({"id": idx, "data": r.data_dt, "team": r.c_home, "v": "h", "xg": r.xg_h})
        rows.append({"id": idx, "data": r.data_dt, "team": r.c_away, "v": "a", "xg": r.xg_a})
    l = pd.DataFrame(rows).sort_values(["team", "data"]).reset_index(drop=True)
    l["r"] = l.groupby("team").xg.transform(lambda s: s.shift(1).rolling(K, min_periods=minp).mean())
    h = l[l.v == "h"].set_index("id").r
    a = l[l.v == "a"].set_index("id").r
    d["sum_xg"] = h + a
    b = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH3.csv", low_memory=False)
    b["data_str"] = pd.to_datetime(b["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    b["c_home"] = b.Home.map(_canon); b["c_away"] = b.Away.map(_canon)
    b["lay"] = pd.to_numeric(b["Odd_CS_0x0_Lay"], errors="coerce")
    m = pd.merge(d.dropna(subset=["sum_xg"]),
                 b[["data_str", "c_home", "c_away", "lay"]].dropna(),
                 on=["data_str", "c_home", "c_away"])
    m["is0"] = ((pd.to_numeric(m.gols_h) + pd.to_numeric(m.gols_a)) == 0).astype(int)
    return m[(m.lay >= 10.0) & (m.lay <= 25.0)].sort_values("data_dt").reset_index(drop=True)


def roi_liab(d):
    if not len(d):
        return 0.0
    pnl = np.where(d.is0 == 0, (1 - COMM), -(d.lay - 1.0))
    return 100 * pnl.sum() / (d.lay - 1.0).sum()


def be(d):
    return 100 * np.mean((d.lay - 1) / (d.lay - COMM))


m = montar()
print("=== 1. REPRODUCAO (odd de LAY REAL da Betfair, faixa 10-25) ===")
print("  %-16s %6s %8s %8s %9s %10s" % ("corte", "N", "WR", "BE", "odd lay", "ROI liab"))
for th in (0, 2.80, 3.00, 3.20):
    d = m[m.sum_xg >= th] if th else m
    print("  sum_xg12>=%-6s %6d %7.2f%% %7.2f%% %9.2f %+9.2f%%"
          % (th if th else "(todos)", len(d), 100 * (1 - d.is0.mean()), be(d), d.lay.mean(), roi_liab(d)))

print("\n=== 2. A CURVA INTEIRA (nao so os 4 pontos escolhidos) ===")
print("  %-10s %6s %8s %10s" % ("corte", "N", "WR", "ROI liab"))
for th in np.arange(2.2, 3.9, 0.10):
    d = m[m.sum_xg >= th]
    if len(d) > 200:
        print("  %-10.2f %6d %7.2f%% %+9.2f%%" % (th, len(d), 100 * (1 - d.is0.mean()), roi_liab(d)))

print("\n=== 3. SPLIT TEMPORAL na ODD REAL (eles so fizeram na odd b365) ===")
corte = m.data_dt.quantile(0.5)
tr, va = m[m.data_dt < corte], m[m.data_dt >= corte]
print("  treino ate %s (N=%d) | validacao (N=%d)" % (corte.date(), len(tr), len(va)))
for th in (2.80, 3.00):
    print("  corte %.2f -> treino %+.2f%% (N=%d) | VALIDACAO %+.2f%% (N=%d) | base OOS %+.2f%%"
          % (th, roi_liab(tr[tr.sum_xg >= th]), len(tr[tr.sum_xg >= th]),
             roi_liab(va[va.sum_xg >= th]), len(va[va.sum_xg >= th]), roi_liab(va)))
melhor = max(np.arange(2.2, 3.9, 0.05),
             key=lambda t: roi_liab(tr[tr.sum_xg >= t]) if len(tr[tr.sum_xg >= t]) > 150 else -99)
print("  melhor corte NO TREINO = %.2f -> na VALIDACAO da %+.2f%% (base %+.2f%%)"
      % (melhor, roi_liab(va[va.sum_xg >= melhor]), roi_liab(va)))

print("\n=== 4. BOOTSTRAP do ROI (o efeito e distinguivel de zero?) ===")
rng = np.random.default_rng(11)
for th, lab in ((0, "base (todos)"), (2.80, "sum_xg12>=2,80"), (3.00, "sum_xg12>=3,00")):
    d = m[m.sum_xg >= th] if th else m
    b = np.sort([roi_liab(d.iloc[rng.integers(0, len(d), len(d))]) for _ in range(4000)])
    print("  %-16s N=%4d ROI=%+.2f%% | IC95 [%+.2f%%, %+.2f%%]"
          % (lab, len(d), roi_liab(d), b[100], b[3900]))

print("\n=== 5. O sum_xg12 prediz 0-0 ALEM da odd de LAY REAL? ===")
def logit(X, y):
    X = np.column_stack([np.ones(len(X)), X]); bb = np.zeros(X.shape[1])
    for _ in range(80):
        eta = np.clip(X @ bb, -30, 30); mu = 1 / (1 + np.exp(-eta))
        W = np.clip(mu * (1 - mu), 1e-9, None); z = eta + (y - mu) / W
        try: nb = np.linalg.solve((X.T * W) @ X, (X.T * W) @ z)
        except np.linalg.LinAlgError: break
        if np.max(np.abs(nb - bb)) < 1e-10: bb = nb; break
        bb = nb
    eta = np.clip(X @ bb, -30, 30); mu = 1 / (1 + np.exp(-eta))
    W = np.clip(mu * (1 - mu), 1e-9, None)
    return bb, np.sqrt(np.diag(np.linalg.pinv((X.T * W) @ X)))
for nome, s in (("amostra inteira", m), ("so a VALIDACAO", va)):
    X = np.column_stack([np.log(s.lay.values), s.sum_xg.values])
    bb, se = logit(X, s.is0.values)
    z = bb[2] / se[2]; p = erfc(abs(z) / math.sqrt(2))
    print("  %-16s N=%4d coef(sum_xg12)=%+.4f p=%.4f %s"
          % (nome, len(s), bb[2], p, "SIGNIFICATIVO" if p < 0.05 else "nao significativo"))
