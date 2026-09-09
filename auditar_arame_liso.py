# -*- coding: utf-8 -*-
"""auditar_arame_liso.py — auditoria independente do Estudo 3 (Arame Liso / veto no Lay 0x0)."""
import numpy as np, pandas as pd, math
from math import erfc

CSV = r"c:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD\hist_time_stats_expandido.csv"
SPLIT = pd.Timestamp("2026-04-01")


def montar(K=5, minp=3):
    df = pd.read_csv(CSV, low_memory=False)
    df = df[(df.tem_stats == 1) & df.poss_h.notna() & df.poss_a.notna() &
            df.tbox_h.notna() & df.tbox_a.notna() & df.odd_h.notna() &
            df.odd_a.notna() & df.gols_h.notna() & df.gols_a.notna()].copy()
    df["data"] = pd.to_datetime(df.data, errors="coerce")
    df = df.sort_values("data").reset_index(drop=True)
    recs = []
    for idx, r in df.iterrows():
        for lado in ("h", "a"):
            recs.append(dict(match_idx=idx, data=r.data, team=r["home" if lado == "h" else "away"],
                             poss=r["poss_%s" % lado], tbox=r["tbox_%s" % lado],
                             bigch=r["bigch_%s" % lado] if pd.notna(r["bigch_%s" % lado]) else 0.0,
                             lado=lado))
    t = pd.DataFrame(recs).sort_values(["team", "data", "match_idx"]).reset_index(drop=True)
    g = t.groupby("team")
    for c in ("poss", "tbox", "bigch"):
        t["r_" + c] = g[c].transform(lambda s: s.shift(1).rolling(K, min_periods=minp).mean())
    t["arame"] = t.r_poss / (t.r_tbox + 1.0)
    h = t[t.lado == "h"].set_index("match_idx")
    a = t[t.lado == "a"].set_index("match_idx")
    df["h_arame"] = h.arame
    df["h_tbox"] = h.r_tbox
    df["h_bigch"] = h.r_bigch
    df["a_tbox"] = a.r_tbox
    df["is_0x0"] = ((df.gols_h == 0) & (df.gols_a == 0)).astype(int)
    return df


def pnl(sub, spread, base="stake"):
    """P&L de LAY. base='stake' = convencao do Estudo 3; base='liability' = capital em risco."""
    odd = sub.odd_cs_0x0 * spread
    p = np.where(sub.is_0x0 == 0, 0.95, -(odd - 1.0))
    if base == "stake":
        return 100 * p.mean()
    return 100 * p.sum() / (odd - 1.0).sum()


def universo(df):
    f = df[df.odd_h.between(1.10, 1.65) & df.h_arame.notna()].copy()
    return f[f.odd_cs_0x0.notna() & (f.odd_cs_0x0 > 1.0)].copy()


df = montar()
cs = universo(df)
print("=== REPRODUCAO (K=5, percentil 75, spread 1,50x) ===")
print("  favoritos mandantes com historico e odd de CS: N=%d" % len(cs))
tr, te = cs[cs.data < SPLIT], cs[cs.data >= SPLIT]
corte = tr.h_arame.quantile(0.75)
ap, ve = te[te.h_arame < corte], te[te.h_arame >= corte]
print("  corte no treino (N=%d): %.3f" % (len(tr), corte))
for nome, s in (("OOS base", te), ("OOS aprovados", ap), ("OOS vetados", ve)):
    print("  %-16s N=%4d | reds(0-0)=%2d | WR=%.2f%% | ROI/stake=%+7.2f%% | ROI/liability=%+6.2f%%"
          % (nome, len(s), s.is_0x0.sum(), 100 * (1 - s.is_0x0.mean()),
             pnl(s, 1.50), pnl(s, 1.50, "liability")))

print("\n=== ACHADO 1 — qual delta e o operacionalmente relevante? ===")
print("  aprovados - VETADOS (o que o relatorio chama de delta): %+.2f pp" % (pnl(ap, 1.50) - pnl(ve, 1.50)))
print("  aprovados - BASE    (o que voce ganha filtrando)      : %+.2f pp" % (pnl(ap, 1.50) - pnl(te, 1.50)))
print("  o mesmo, em ROI sobre LIABILITY                       : %+.2f pp"
      % (pnl(ap, 1.50, "liability") - pnl(te, 1.50, "liability")))

print("\n=== ACHADO 2 — sensibilidade ao spread de lay (Lei no 1) ===")
print("  %-8s %14s %14s %12s" % ("spread", "ROI base", "ROI aprovados", "delta"))
for sp in (1.30, 1.50, 1.56, 1.65, 1.80):
    rb, ra = pnl(te, sp), pnl(ap, sp)
    print("  %-8.2f %13.2f%% %13.2f%% %11.2f pp%s"
          % (sp, rb, ra, ra - rb, "   <- medido no coletor" if sp == 1.56 else ""))

print("\n=== ACHADO 3 — quantos REDS sustentam o resultado? ===")
print("  o OOS inteiro tem %d reds. Aprovados: %d. Vetados: %d."
      % (te.is_0x0.sum(), ap.is_0x0.sum(), ve.is_0x0.sum()))
print("  cada red custa ~%.1f unidades de stake (odd lay media %.1f)."
      % ((te.odd_cs_0x0.mean() * 1.5 - 1), te.odd_cs_0x0.mean() * 1.5))
print("  => o delta de ROI e a diferenca de %d eventos raros. 1 red a mais nos aprovados move o ROI em %.2f pp."
      % (ve.is_0x0.sum() - ap.is_0x0.sum(), 100 * (te.odd_cs_0x0.mean() * 1.5) / len(ap)))

print("\n=== ACHADO 4 — o corte e robusto? (percentil e janela K) ===")
print("  %-6s %-8s %8s %10s %10s" % ("K", "percentil", "N_apr", "delta_pp", "reds_apr"))
for K in (3, 5, 8):
    d2 = montar(K=K, minp=min(3, K))
    c2 = universo(d2)
    t2, e2 = c2[c2.data < SPLIT], c2[c2.data >= SPLIT]
    for q in (0.70, 0.75, 0.80):
        ct = t2.h_arame.quantile(q)
        a2 = e2[e2.h_arame < ct]
        print("  %-6d %-8.2f %8d %9.2f %10d" % (K, q, len(a2), pnl(a2, 1.50) - pnl(e2, 1.50), a2.is_0x0.sum()))

print("\n=== ACHADO 5 — monotonicidade da taxa de 0-0 por quartil (amostra inteira) ===")
f = universo(df)
f["q"] = pd.qcut(f.h_arame, 4, labels=["Q1", "Q2", "Q3", "Q4"])
for q, s in f.groupby("q", observed=True):
    print("  %-4s N=%4d | 0-0=%.2f%% | odd_h media=%.3f | odd_cs_0x0 media=%.2f | 1/odd_cs=%.2f%%"
          % (q, len(s), 100 * s.is_0x0.mean(), s.odd_h.mean(), s.odd_cs_0x0.mean(),
             100 * (1 / s.odd_cs_0x0).mean()))

print("\n=== ACHADO 6 — o arame_liso prediz 0-0 ALEM do preco? ===")
def logit(X, y):
    X = np.column_stack([np.ones(len(X)), X]); b = np.zeros(X.shape[1])
    for _ in range(80):
        eta = np.clip(X @ b, -30, 30); mu = 1 / (1 + np.exp(-eta))
        W = np.clip(mu * (1 - mu), 1e-9, None); z = eta + (y - mu) / W
        try: nb = np.linalg.solve((X.T * W) @ X, (X.T * W) @ z)
        except np.linalg.LinAlgError: break
        if np.max(np.abs(nb - b)) < 1e-10: b = nb; break
        b = nb
    eta = np.clip(X @ b, -30, 30); mu = 1 / (1 + np.exp(-eta))
    W = np.clip(mu * (1 - mu), 1e-9, None)
    return b, np.sqrt(np.diag(np.linalg.pinv((X.T * W) @ X)))

for nome, s in (("amostra inteira", f), ("so o OOS", te)):
    s2 = s.dropna(subset=["odd_cs_0x0", "h_arame"])
    X = np.column_stack([np.log(s2.odd_cs_0x0.values), s2.h_arame.values])
    b, se = logit(X, s2.is_0x0.values)
    z = b[2] / se[2]; p = erfc(abs(z) / math.sqrt(2))
    print("  %-18s N=%4d | coef(arame)=%+.4f p=%.4f | controlando pela odd do PROPRIO 0-0  %s"
          % (nome, len(s2), b[2], p, "SIGNIFICATIVO" if p < 0.05 else "nao significativo"))
