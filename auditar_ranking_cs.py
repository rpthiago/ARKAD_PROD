# -*- coding: utf-8 -*-
"""
auditar_ranking_cs.py — 7 mercados remanescentes de Correct Score x ranking diario por menor odd de lay
(Todos / TOP 3 / TOP 2 / TOP 1). Regras exatamente como especificadas na auditoria de 13/09.
Base: Betfair apicomunidade (odd de LAY real + Goals_*_FT). P&L liability=1u, comissao 5%.
Bootstrap bloco-dia 10.000x. Saida: varredura_over/auditoria_ranking_cs_<data>.csv (+ _apostas.csv)
"""
import os, sys, warnings
from datetime import datetime
warnings.filterwarnings("ignore"); sys.stdout.reconfigure(encoding="utf-8")
ROOT = os.path.dirname(os.path.abspath(__file__))
import pandas as pd, numpy as np

C = 0.05; B = 10000
BASE = os.path.join(ROOT, "metodos_aprovados", ".cache_base_betfair.csv")

# (nome, coluna de lay, faixa, filtro(df)->bool Series, red(gh,ga)->bool)
METODOS = [
    ("Lay Goleada Visit (AO Away)", "Odd_CS_Goleada_A_Lay", 15, 60, lambda d: d.u25 <= 2.10, lambda gh, ga: (ga >= 4) & (ga > gh)),
    ("Lay Goleada Mand (AO Home)",  "Odd_CS_Goleada_H_Lay", 15, 60, lambda d: d.u25 <= 2.10, lambda gh, ga: (gh >= 4) & (gh > ga)),
    ("Lay 0x1 SuperFav Mand",       "Odd_CS_0x1_Lay", 5, 15, lambda d: d.oh <= 1.90, lambda gh, ga: (gh == 0) & (ga == 1)),
    ("Lay 1x0 SuperFav Visit",      "Odd_CS_1x0_Lay", 5, 15, lambda d: d.oa <= 1.90, lambda gh, ga: (gh == 1) & (ga == 0)),
    ("Lay 0x0 Quantitativo",        "Odd_CS_0x0_Lay", 10, 20, lambda d: (d.oh <= 1.50) | (d.oa <= 1.40), lambda gh, ga: (gh == 0) & (ga == 0)),
    ("Lay 0x2 Zebra (Mand<=1.45)",  "Odd_CS_0x2_Lay", 5, 25, lambda d: d.oh <= 1.45, lambda gh, ga: (gh == 0) & (ga == 2)),
    ("Lay 2x0 Zebra (Visit<=1.45)", "Odd_CS_2x0_Lay", 5, 25, lambda d: d.oa <= 1.45, lambda gh, ga: (gh == 2) & (ga == 0)),
]
CORTES = [("Todos", None), ("TOP 3", 3), ("TOP 2", 2), ("TOP 1", 1)]


def boot(v, blocos, seed=5):
    mm = {k: v[blocos == k] for k in pd.unique(blocos)}; ks = list(mm)
    if len(ks) < 6: return np.nan, np.nan, np.nan, len(ks)
    rng = np.random.default_rng(seed); r = np.empty(B)
    for i in range(B): r[i] = np.concatenate([mm[ks[j]] for j in rng.integers(0, len(ks), len(ks))]).mean()
    return np.percentile(r, 2.5), np.percentile(r, 97.5), float((r <= 0).mean()), len(ks)


def main():
    b = pd.read_csv(BASE, low_memory=False)
    b["Date"] = pd.to_datetime(b["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    for c in ["Odd_H_Back", "Odd_A_Back", "Odd_Under25_FT_Back", "Goals_H_FT", "Goals_A_FT"] + [m[1] for m in METODOS]:
        b[c] = pd.to_numeric(b[c], errors="coerce")
    b = b.dropna(subset=["Date", "Goals_H_FT", "Goals_A_FT"]).rename(columns={"Odd_H_Back": "oh", "Odd_A_Back": "oa", "Odd_Under25_FT_Back": "u25", "Goals_H_FT": "gh", "Goals_A_FT": "ga"})
    b["gh"] = b.gh.astype(int); b["ga"] = b.ga.astype(int)
    print("base: %d jogos com placar | %s -> %s | 2026: %d" % (len(b), b.Date.min(), b.Date.max(), (b.Date >= "2026-01-01").sum()))
    out, apostas = [], []
    for nome, col, lo, hi, filtro, red in METODOS:
        q = b[filtro(b).fillna(False) & b[col].between(lo, hi)].copy()
        q["odd"] = q[col]; q["is_red"] = red(q.gh, q.ga).astype(int); q["green"] = 1 - q.is_red
        q["pnl"] = np.where(q.is_red == 1, -1.0, (1 - C) / (q.odd - 1)); q["be"] = (q.odd - 1) / (q.odd - C)
        q["rank"] = q.groupby("Date")["odd"].rank(method="first", ascending=True)
        q["metodo"] = nome; apostas.append(q[["Date", "metodo", "Home", "Away", "League", "odd", "oh", "oa", "u25", "gh", "ga", "rank", "is_red", "pnl", "be"]])
        print("\n" + "=" * 118)
        print("%s  |  %s em %d-%d  |  qualificados: %d (%.1f/dia em %d dias)" % (nome, col, lo, hi, len(q), q.groupby("Date").size().mean(), q.Date.nunique()))
        print("=" * 118)
        print("  %-9s %-6s %6s %5s %4s %6s %6s %6s %7s %6s %7s %-18s %6s" % ("periodo", "corte", "N", "G", "R", "red%", "WR", "BE", "edge", "odd", "ROI", "IC95 ROI", "P<=0"))
        for per, sub in [("completo", q), ("2026", q[q.Date >= "2026-01-01"])]:
            for ctag, k in CORTES:
                s = sub if k is None else sub[sub["rank"] <= k]
                if len(s) == 0:
                    out.append(dict(metodo=nome, periodo=per, corte=ctag, N=0)); continue
                wr, be, roi = s.green.mean(), s.be.mean(), s.pnl.mean(); lo_, hi_, p0, nk = boot(s.pnl.values, s.Date.values)
                rec = dict(metodo=nome, periodo=per, corte=ctag, N=len(s), G=int(s.green.sum()), R=int(s.is_red.sum()), red_pct=100 * s.is_red.mean(),
                           WR=100 * wr, BE=100 * be, edge_pp=100 * (wr - be), odd_med=s.odd.mean(), ROI=100 * roi, PnL_u=s.pnl.sum(),
                           IC_lo=100 * lo_ if np.isfinite(lo_) else np.nan, IC_hi=100 * hi_ if np.isfinite(hi_) else np.nan, p_le0=p0, dias=nk,
                           oh_med=s.oh.mean(), oa_med=s.oa.mean())
                out.append(rec)
                ic = ("[%+.1f,%+.1f]" % (rec["IC_lo"], rec["IC_hi"])) if np.isfinite(lo_) else "IC indef(%dd)" % nk
                print("  %-9s %-6s %6d %5d %4d %5.2f%% %5.1f%% %5.1f%% %+6.2fpp %6.2f %+6.2f%% %-18s %6s" %
                      (per, ctag, rec["N"], rec["G"], rec["R"], rec["red_pct"], rec["WR"], rec["BE"], rec["edge_pp"], rec["odd_med"], rec["ROI"], ic, ("%.3f" % p0) if np.isfinite(p0) else "-"))
        # mecanismo: o que muda por posicao (odd do fav, taxa de red, BE)
        print("  mecanismo por posicao (completo): rank | N | odd lay | odd mand | odd visit | P(placar) | BE | edge")
        for r_, g in q.groupby(q["rank"].clip(upper=5)):
            print("    %s%s | %5d | %6.2f | %5.2f | %5.2f | %5.2f%% | %5.1f%% | %+5.2fpp" % (int(r_), "+" if r_ == 5 else " ", len(g), g.odd.mean(), g.oh.mean(), g.oa.mean(), 100 * g.is_red.mean(), 100 * g.be.mean(), 100 * (g.green.mean() - g.be.mean())))
    T = pd.DataFrame(out); os.makedirs(os.path.join(ROOT, "varredura_over"), exist_ok=True)
    snap = os.path.join(ROOT, "varredura_over", "auditoria_ranking_cs_%s.csv" % datetime.now().strftime("%Y-%m-%d"))
    T.to_csv(snap, index=False, encoding="utf-8-sig"); pd.concat(apostas).to_csv(snap.replace(".csv", "_apostas.csv"), index=False, encoding="utf-8-sig")
    print("\ntabelas: %s" % os.path.relpath(snap, ROOT))


if __name__ == "__main__":
    main()
