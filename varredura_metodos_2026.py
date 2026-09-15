# -*- coding: utf-8 -*-
"""
varredura_metodos_2026.py — grade congelada em PREREGISTRO_varredura_metodos_2026.md.
43 runners x {lay, back} x favoritismo(4) x contexto de gols(4) x lado do favorito(3) x corte(2) = 8.256 celulas.
So celulas com N>=100. Bootstrap bloco-dia (B_SCAN na varredura, B_FULL nas sobreviventes), BH-FDR sobre todas.
Uso: python varredura_metodos_2026.py --desde 2026-01-01 --tag 2026
"""
import os, sys, argparse, warnings
from datetime import datetime
warnings.filterwarnings("ignore"); sys.stdout.reconfigure(encoding="utf-8")
ROOT = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, ROOT)
import pandas as pd, numpy as np
import varredura_ranking_lay_todos_mercados as V      # MERCADOS (faixas + funcao de evento), bh()

C = 0.05; N_MIN = 100; B_SCAN = 2000; B_FULL = 10000; MIN_DIAS = 6
FAV = [("Todos", None), ("fav<=1.40", (0, 1.40)), ("fav1.40-1.80", (1.40, 1.80)), ("fav>1.80", (1.80, 99))]
GOLS = [("Todos", None), ("U25<=1.60", (0, 1.60)), ("U25 1.60-2.20", (1.60, 2.20)), ("U25>2.20", (2.20, 99))]
LADO = [("Todos", None), ("fav casa", "casa"), ("fav fora", "fora")]
CORTES = [("Todos", None), ("TOP 3", 3)]


def boot_dia(pnl, dias, B, seed=5):
    """bootstrap de bloco por dia, vetorizado: reamostra dias, media = soma/contagem."""
    d = pd.DataFrame({"p": pnl, "d": dias}).groupby("d")["p"].agg(["sum", "count"])
    K = len(d)
    if K < MIN_DIAS: return np.nan, np.nan, np.nan, K
    S, Cn = d["sum"].values, d["count"].values.astype(float)
    rng = np.random.default_rng(seed); idx = rng.integers(0, K, (B, K))
    r = S[idx].sum(1) / Cn[idx].sum(1)
    return np.percentile(r, 2.5), np.percentile(r, 97.5), float((r <= 0).mean()), K


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--desde", default="2026-01-01"); ap.add_argument("--ate", default=None); ap.add_argument("--tag", default="2026")
    a = ap.parse_args()
    b = pd.read_csv(V.BASE, low_memory=False)
    b["Date"] = pd.to_datetime(b["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    cols = list(V.MERCADOS) + [c.replace("_Lay", "_Back") for c in V.MERCADOS] + ["Odd_H_Back", "Odd_A_Back", "Odd_Under25_FT_Back"]
    for c in ["Goals_H_FT", "Goals_A_FT", "Goals_H_HT", "Goals_A_HT"] + sorted(set(cols)): b[c] = pd.to_numeric(b[c], errors="coerce")
    b = b.dropna(subset=["Date", "Goals_H_FT", "Goals_A_FT"])
    b = b[b.Date >= a.desde]
    if a.ate: b = b[b.Date <= a.ate]
    b["fav"] = b[["Odd_H_Back", "Odd_A_Back"]].min(axis=1)
    b["lado_fav"] = np.where(b.Odd_H_Back <= b.Odd_A_Back, "casa", "fora")
    b["u25"] = b["Odd_Under25_FT_Back"]
    b["per"] = np.where(b.Date < "2026-05-01", "jan-abr", np.where(b.Date < "2026-08-01", "mai-jul", "ago-set"))
    print("base: %d jogos | %s -> %s" % (len(b), b.Date.min(), b.Date.max()))
    rows = []; n_cells = 0
    for col_lay, (lo, hi, red_fn) in V.MERCADOS.items():
        runner = col_lay.replace("Odd_", "").replace("_Lay", "")
        ev = red_fn(b.Goals_H_FT.astype(int), b.Goals_A_FT.astype(int), b.Goals_H_HT, b.Goals_A_HT)   # evento aconteceu?
        for lado_ap in ("lay", "back"):
            col = col_lay if lado_ap == "lay" else col_lay.replace("_Lay", "_Back")
            q = b[b[col].between(lo, hi)].copy()
            if "HT" in col: q = q.dropna(subset=["Goals_H_HT", "Goals_A_HT"])
            if len(q) < N_MIN: continue
            e = ev.loc[q.index].astype(bool)
            q["odd"] = q[col]
            if lado_ap == "lay":
                q["perde"] = e.astype(int); q["pnl"] = np.where(e, -1.0, (1 - C) / (q.odd - 1)); q["be"] = (q.odd - 1) / (q.odd - C)
            else:
                q["perde"] = (~e).astype(int); q["pnl"] = np.where(e, (1 - C) * (q.odd - 1), -1.0); q["be"] = 1 / (1 + (1 - C) * (q.odd - 1))
            for ftag, fr in FAV:
                qf = q if fr is None else q[(q.fav > fr[0]) & (q.fav <= fr[1])]
                for gtag, gr in GOLS:
                    qg = qf if gr is None else qf[(qf.u25 > gr[0]) & (qf.u25 <= gr[1])]
                    for ltag, lv in LADO:
                        ql = qg if lv is None else qg[qg.lado_fav == lv]
                        if len(ql) < N_MIN: continue
                        ql = ql.copy(); ql["rank"] = ql.groupby("Date")["odd"].rank(method="first")
                        for ctag, k in CORTES:
                            s = ql if k is None else ql[ql["rank"] <= k]
                            n_cells += 1
                            if len(s) < N_MIN: continue
                            wr, be, roi = 1 - s.perde.mean(), s.be.mean(), s.pnl.mean()
                            lo_, hi_, p0, nk = boot_dia(s.pnl.values, s.Date.values, B_SCAN)
                            p = (p0 if roi > 0 else 1.0) if np.isfinite(p0) else 1.0
                            per = {pp: (int((s.per == pp).sum()), 100 * s.pnl[s.per == pp].mean() if (s.per == pp).any() else np.nan) for pp in ("jan-abr", "mai-jul", "ago-set")}
                            rows.append(dict(runner=runner, lado=lado_ap, fav=ftag, gols=gtag, lado_fav=ltag, corte=ctag, N=len(s), perdas=int(s.perde.sum()),
                                             WR=100 * wr, BE=100 * be, edge_pp=100 * (wr - be), odd_med=s.odd.median(), ROI=100 * roi,
                                             IC_lo=100 * lo_ if np.isfinite(lo_) else np.nan, IC_hi=100 * hi_ if np.isfinite(hi_) else np.nan, p=p, dias=nk,
                                             N_janabr=per["jan-abr"][0], ROI_janabr=per["jan-abr"][1], N_maijul=per["mai-jul"][0], ROI_maijul=per["mai-jul"][1],
                                             N_agoset=per["ago-set"][0], ROI_agoset=per["ago-set"][1], _pnl=s.pnl.values, _dias=s.Date.values))
    T = pd.DataFrame(rows)
    print("celulas avaliadas: %d | com N>=%d: %d" % (n_cells, N_MIN, len(T)))
    T["BH_passa"] = V.bh(T.p.values)
    # re-roda 10.000 nas que passam no BH (o IC final e este)
    for i in T.index[T.BH_passa]:
        lo_, hi_, p0, nk = boot_dia(T.at[i, "_pnl"], T.at[i, "_dias"], B_FULL)
        T.at[i, "IC_lo"], T.at[i, "IC_hi"], T.at[i, "p"] = 100 * lo_, 100 * hi_, p0
    def dec(r):
        if r.BH_passa and np.isfinite(r.IC_lo) and r.IC_lo > 0: return "PASSA" if r.perdas >= 5 else "INCONCLUSIVO (perdas<5)"
        if np.isfinite(r.IC_hi) and r.IC_hi < 0: return "REPROVA"
        return "INCONCLUSIVO"
    T["decisao"] = T.apply(dec, axis=1); T = T.drop(columns=["_pnl", "_dias"]).sort_values("p")
    snap = os.path.join(ROOT, "varredura_over", "varredura_metodos_%s_%s.csv" % (datetime.now().strftime("%Y-%m-%d"), a.tag))
    T.to_csv(snap, index=False, encoding="utf-8-sig")
    print("\n=== BH q=0,05 sobre M=%d | %s ===" % (len(T), T.decisao.value_counts().to_dict()))
    print("%-10s %-4s %-13s %-14s %-9s %-6s %5s %5s %6s %6s %7s %6s %7s %-16s %-7s %-22s %s" % ("runner", "lado", "fav", "gols", "lado_fav", "corte", "N", "perd", "WR", "BE", "edge", "odd", "ROI", "IC95", "p", "jan-abr/mai-jul/ago-set", "decisao"))
    for _, r in T.head(40).iterrows():
        ic = "[%+.1f,%+.1f]" % (r.IC_lo, r.IC_hi) if np.isfinite(r.IC_lo) else "indef"
        f = lambda v: ("%+.1f" % v) if np.isfinite(v) else "--"
        print("%-10s %-4s %-13s %-14s %-9s %-6s %5d %5d %5.1f%% %5.1f%% %+6.2fpp %6.2f %+6.2f%% %-16s %-7.4f %-22s %s" % (
            r.runner, r.lado, r.fav, r.gols, r.lado_fav, r.corte, r.N, r.perdas, r.WR, r.BE, r.edge_pp, r.odd_med, r.ROI, ic, r.p,
            "%s/%s/%s" % (f(r.ROI_janabr), f(r.ROI_maijul), f(r.ROI_agoset)), r.decisao))
    print("\n(40 melhores por p; tabela completa: %s)" % os.path.relpath(snap, ROOT))
    P = T[T.decisao == "PASSA"]
    if len(P):
        print("\n=== PASSA por familia ===\n%s" % P.groupby(["lado", P.runner.str.startswith("CS").map({True: "CS", False: "nao-CS"})]).size().to_string())
        nao_cs = P[~P.runner.str.startswith("CS")]
        if len(nao_cs):
            print("\nPASSA fora do Correct Score:")
            for _, r in nao_cs.iterrows(): print("  %s %s | %s | %s | %s | %s | N=%d ROI %+.2f%% IC [%+.1f, %+.1f] | %+.1f/%+.1f/%+.1f" % (r.lado, r.runner, r.fav, r.gols, r.lado_fav, r.corte, r.N, r.ROI, r.IC_lo, r.IC_hi, r.ROI_janabr, r.ROI_maijul, r.ROI_agoset))


if __name__ == "__main__":
    main()
