# -*- coding: utf-8 -*-
"""
varredura_ranking_lay_todos_mercados.py — GRADE CONGELADA (commitada antes de rodar).
Pergunta do Thiago (13/09): "dos 50+ metodos reprovados, o ranking por menor odd de lay resgata algum?"
Em vez de escolher a dedo, varre TODOS os 43 mercados de lay da base Betfair x 4 cortes de ranking.

Regra por celula (mercado M, corte K):
  - qualificado = jogo com Odd_M_Lay entre LO e HI (faixa fixa por familia, abaixo), sem NENHUM outro filtro
  - ranking = posicao por menor odd de lay entre os qualificados do dia; corte Todos / TOP 3 / TOP 2 / TOP 1
  - lay a odd real, liability 1u, comissao 5%; RED = evento do runner aconteceu (placar FT/HT da base)
  - bootstrap bloco-dia 10.000 (>=6 dias), p unilateral (ROI<=0); ROI negativo entra com p=1
  - BH-FDR q=0,05 sobre M = numero de celulas com N>=100 (todas de uma vez)
  - decisao: PASSA (BH + piso IC95>0) / REPROVA (teto IC95<0) / INCONCLUSIVO
Faixas (fixas, por familia — nao ajustadas depois):
  Match Odds e DC: 1.5-10 | O/U e BTTS: 1.3-6 | CS exato: 5-40 | Any Other: 10-80
Sem multi-testagem escondida: a tabela completa e gravada, inclusive as celulas que nao passam.
"""
import os, sys, warnings
from datetime import datetime
warnings.filterwarnings("ignore"); sys.stdout.reconfigure(encoding="utf-8")
ROOT = os.path.dirname(os.path.abspath(__file__))
import pandas as pd, numpy as np

C = 0.05; B = 10000; N_MIN = 100
BASE = os.path.join(ROOT, "metodos_aprovados", ".cache_base_betfair.csv")

def cs(h, a): return lambda gh, ga, hh, ha: (gh == h) & (ga == a)
MERCADOS = {
    "Odd_H_Lay": (1.5, 10, lambda gh, ga, hh, ha: gh > ga), "Odd_A_Lay": (1.5, 10, lambda gh, ga, hh, ha: ga > gh),
    "Odd_D_Lay": (1.5, 10, lambda gh, ga, hh, ha: gh == ga),
    "Odd_1X_Lay": (1.5, 10, lambda gh, ga, hh, ha: gh >= ga), "Odd_X2_Lay": (1.5, 10, lambda gh, ga, hh, ha: ga >= gh),
    "Odd_12_Lay": (1.5, 10, lambda gh, ga, hh, ha: gh != ga),
    "Odd_Over05_FT_Lay": (1.3, 6, lambda gh, ga, hh, ha: gh + ga > 0.5), "Odd_Under05_FT_Lay": (1.3, 6, lambda gh, ga, hh, ha: gh + ga < 0.5),
    "Odd_Over15_FT_Lay": (1.3, 6, lambda gh, ga, hh, ha: gh + ga > 1.5), "Odd_Under15_FT_Lay": (1.3, 6, lambda gh, ga, hh, ha: gh + ga < 1.5),
    "Odd_Over25_FT_Lay": (1.3, 6, lambda gh, ga, hh, ha: gh + ga > 2.5), "Odd_Under25_FT_Lay": (1.3, 6, lambda gh, ga, hh, ha: gh + ga < 2.5),
    "Odd_Over35_FT_Lay": (1.3, 6, lambda gh, ga, hh, ha: gh + ga > 3.5), "Odd_Under35_FT_Lay": (1.3, 6, lambda gh, ga, hh, ha: gh + ga < 3.5),
    "Odd_Over45_FT_Lay": (1.3, 6, lambda gh, ga, hh, ha: gh + ga > 4.5), "Odd_Under45_FT_Lay": (1.3, 6, lambda gh, ga, hh, ha: gh + ga < 4.5),
    "Odd_BTTS_Yes_Lay": (1.3, 6, lambda gh, ga, hh, ha: (gh > 0) & (ga > 0)), "Odd_BTTS_No_Lay": (1.3, 6, lambda gh, ga, hh, ha: (gh == 0) | (ga == 0)),
    "Odd_Over05_HT_Lay": (1.3, 6, lambda gh, ga, hh, ha: hh + ha > 0.5), "Odd_Under05_HT_Lay": (1.3, 6, lambda gh, ga, hh, ha: hh + ha < 0.5),
    "Odd_Over15_HT_Lay": (1.3, 6, lambda gh, ga, hh, ha: hh + ha > 1.5), "Odd_Under15_HT_Lay": (1.3, 6, lambda gh, ga, hh, ha: hh + ha < 1.5),
    "Odd_Over25_HT_Lay": (1.3, 6, lambda gh, ga, hh, ha: hh + ha > 2.5), "Odd_Under25_HT_Lay": (1.3, 6, lambda gh, ga, hh, ha: hh + ha < 2.5),
    "Odd_CS_Goleada_H_Lay": (10, 80, lambda gh, ga, hh, ha: (gh >= 4) & (gh > ga)),
    "Odd_CS_Goleada_A_Lay": (10, 80, lambda gh, ga, hh, ha: (ga >= 4) & (ga > gh)),
    "Odd_CS_Goleada_D_Lay": (10, 80, lambda gh, ga, hh, ha: (gh == ga) & (gh >= 4)),
}
for h in range(4):
    for a in range(4):
        MERCADOS["Odd_CS_%dx%d_Lay" % (h, a)] = (5, 40, cs(h, a))
CORTES = [("Todos", None), ("TOP 3", 3), ("TOP 2", 2), ("TOP 1", 1)]


def boot(v, blocos, seed=5):
    mm = {k: v[blocos == k] for k in pd.unique(blocos)}; ks = list(mm)
    if len(ks) < 6: return np.nan, np.nan, np.nan, len(ks)
    rng = np.random.default_rng(seed); r = np.empty(B)
    for i in range(B): r[i] = np.concatenate([mm[ks[j]] for j in rng.integers(0, len(ks), len(ks))]).mean()
    return np.percentile(r, 2.5), np.percentile(r, 97.5), float((r <= 0).mean()), len(ks)


def bh(p, q=0.05):
    p = np.asarray(p, float); m = len(p); o = np.argsort(p); ok = p[o] <= q * np.arange(1, m + 1) / m
    out = np.zeros(m, bool)
    if ok.any(): out[o[:np.max(np.where(ok)[0]) + 1]] = True
    return out


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--desde", default=None, help="janela: so jogos com Date >= desde (ex.: 2026-01-01)")
    ap.add_argument("--ate", default=None, help="janela: so jogos com Date <= ate")
    ap.add_argument("--tag", default="", help="sufixo do arquivo de saida (ex.: 2026)")
    a = ap.parse_args()
    b = pd.read_csv(BASE, low_memory=False)
    b["Date"] = pd.to_datetime(b["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    if a.desde: b = b[b["Date"] >= a.desde]
    if a.ate: b = b[b["Date"] <= a.ate]
    for c in ["Goals_H_FT", "Goals_A_FT", "Goals_H_HT", "Goals_A_HT"] + list(MERCADOS): b[c] = pd.to_numeric(b[c], errors="coerce")
    b = b.dropna(subset=["Date", "Goals_H_FT", "Goals_A_FT"])
    gh, ga = b.Goals_H_FT.astype(int), b.Goals_A_FT.astype(int); hh, ha = b.Goals_H_HT, b.Goals_A_HT
    print("base: %d jogos | %s -> %s | %d mercados x 4 cortes" % (len(b), b.Date.min(), b.Date.max(), len(MERCADOS)))
    rows = []
    for col, (lo, hi, red_fn) in MERCADOS.items():
        q = b[b[col].between(lo, hi)].copy()
        if "HT" in col: q = q.dropna(subset=["Goals_H_HT", "Goals_A_HT"])
        if len(q) < N_MIN: continue
        q["odd"] = q[col]; q["red"] = red_fn(q.Goals_H_FT.astype(int), q.Goals_A_FT.astype(int), q.Goals_H_HT, q.Goals_A_HT).astype(int)
        q["pnl"] = np.where(q.red == 1, -1.0, (1 - C) / (q.odd - 1)); q["be"] = (q.odd - 1) / (q.odd - C)
        q["rank"] = q.groupby("Date")["odd"].rank(method="first")
        for ctag, k in CORTES:
            s = q if k is None else q[q["rank"] <= k]
            if len(s) < N_MIN: continue
            wr, be, roi = 1 - s.red.mean(), s.be.mean(), s.pnl.mean(); lo_, hi_, p0, nk = boot(s.pnl.values, s.Date.values)
            p = (p0 if roi > 0 else 1.0) if np.isfinite(p0) else 1.0
            rows.append(dict(mercado=col.replace("Odd_", "").replace("_Lay", ""), corte=ctag, N=len(s), reds=int(s.red.sum()), red_pct=100 * s.red.mean(),
                             WR=100 * wr, BE=100 * be, edge_pp=100 * (wr - be), odd_med=s.odd.median(), ROI=100 * roi,
                             IC_lo=100 * lo_ if np.isfinite(lo_) else np.nan, IC_hi=100 * hi_ if np.isfinite(hi_) else np.nan, p=p, dias=nk))
    T = pd.DataFrame(rows); T["BH_passa"] = bh(T.p.values)
    T["decisao"] = np.where(T.BH_passa & (T.IC_lo > 0), "PASSA", np.where(T.IC_hi < 0, "REPROVA", "INCONCLUSIVO"))
    T = T.sort_values("p")
    snap = os.path.join(ROOT, "varredura_over", "varredura_ranking_lay_%s%s.csv" % (datetime.now().strftime("%Y-%m-%d"), ("_" + a.tag) if a.tag else ""))
    T.to_csv(snap, index=False, encoding="utf-8-sig")
    print("\n=== %d celulas com N>=%d | BH q=0,05 sobre M=%d | decisao: %s ===" % (len(T), N_MIN, len(T), T.decisao.value_counts().to_dict()))
    print("%-16s %-6s %7s %6s %6s %7s %7s %7s %6s %8s %-18s %-7s %s" % ("mercado", "corte", "N", "reds", "red%", "WR", "BE", "edge", "odd", "ROI", "IC95", "p", "decisao"))
    for _, r in T.head(30).iterrows():
        ic = "[%+.1f,%+.1f]" % (r.IC_lo, r.IC_hi) if np.isfinite(r.IC_lo) else "indef"
        print("%-16s %-6s %7d %6d %5.1f%% %6.1f%% %6.1f%% %+6.2fpp %6.2f %+7.2f%% %-18s %-7.4f %s" % (r.mercado, r.corte, r.N, r.reds, r.red_pct, r.WR, r.BE, r.edge_pp, r.odd_med, r.ROI, ic, r.p, r.decisao))
    print("\n(30 melhores por p; tabela completa: %s)" % os.path.relpath(snap, ROOT))
    # efeito do ranking por mercado: ROI Todos vs TOP1
    print("\n=== efeito do ranking: ROI Todos -> TOP 3 -> TOP 1 (so mercados com as 3 celulas) ===")
    piv = T.pivot(index="mercado", columns="corte", values="ROI")
    for c in ("Todos", "TOP 3", "TOP 1"):
        if c not in piv.columns: piv[c] = np.nan
    piv = piv.dropna(subset=["Todos", "TOP 1"]); piv["ganho_TOP1"] = piv["TOP 1"] - piv["Todos"]
    for m, r in piv.sort_values("ganho_TOP1", ascending=False).iterrows():
        print("  %-16s Todos %+6.2f%%  TOP3 %+6.2f%%  TOP1 %+6.2f%%  (ranking: %+.2fpp)" % (m, r["Todos"], r["TOP 3"] if np.isfinite(r["TOP 3"]) else np.nan, r["TOP 1"], r["ganho_TOP1"]))


if __name__ == "__main__":
    main()
