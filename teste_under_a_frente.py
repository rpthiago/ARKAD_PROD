# -*- coding: utf-8 -*-
"""
teste_under_a_frente.py — Back Under com FOLGA de gols, in-play, na odd real do coletor.
Pergunta do Thiago (15/09): "under a frente e tomar dois gols" — Back Under (gols atuais + 2).5.
Folga 2 = Under (g+2).5 (perde so se sairem 2+ gols); folga 1 = Under (g+1).5 (perde se sair 1+), para comparar.
Estado (gols ja saidos) pelas linhas O/U batidas (gols_por_ou), NUNCA pelo CS. Odd = back do Under na 1a captura
elegivel da janela. Placar FT: oficial primeiro, bases depois. Stake 1u, comissao 5%. Bootstrap bloco-dia, BH sobre
todas as celulas (folga x janela x favoritismo). Declarado como OLHAR (coletor 16/08 -> 13/09), nao julgamento.
"""
import os, sys, argparse, warnings
warnings.filterwarnings("ignore"); sys.stdout.reconfigure(encoding="utf-8")
ROOT = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, ROOT)
import pandas as pd, numpy as np
import relatorio_forward_5metodos as RF
import varredura_over_inplay as V

C = 0.05; B = 5000; N_MIN = 50
JANELAS = [(10, 25), (25, 40), (46, 60), (60, 75), (75, 85)]
ODD_LO, ODD_HI = 1.10, 6.0
FOLGAS = [1, 2]


def boot(v, blocos, seed=5):
    d = pd.DataFrame({"p": v, "d": blocos}).groupby("d")["p"].agg(["sum", "count"]); K = len(d)
    if K < 6: return np.nan, np.nan, np.nan, K
    S, Cn = d["sum"].values, d["count"].values.astype(float); rng = np.random.default_rng(seed); idx = rng.integers(0, K, (B, K))
    r = S[idx].sum(1) / Cn[idx].sum(1)
    return np.percentile(r, 2.5), np.percentile(r, 97.5), float((r <= 0).mean()), K


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--pasta", default=os.path.join(os.environ.get("TEMP", "."), "var")); a = ap.parse_args()
    T = a.pasta
    ou_all = pd.read_csv(os.path.join(T, "ou.csv"), header=None, names=["ts", "ko", "home", "away", "mtk", "mtype", "runner", "back"], dtype=str)
    ou_all["mtk"] = pd.to_numeric(ou_all.mtk, errors="coerce"); ou_all["minuto"] = -ou_all.mtk - 15
    ou_all["back"] = pd.to_numeric(ou_all.back, errors="coerce"); ou_all = ou_all.dropna(subset=["back", "minuto"])
    ou_all["L"] = ou_all.mtype.map(V.LINHAS)
    over = ou_all[ou_all.runner.str.startswith("Over", na=False)]
    under = ou_all[ou_all.runner.str.startswith("Under", na=False)].copy()
    G = V.gols_por_ou(over)
    _, _, fav = V.carregar(T)
    of = pd.read_csv(os.path.join(T, "placares_ft.csv"), dtype=str).fillna(""); of = of[(of.status == "LIQUIDADO") & (of.gh != "")]
    ofi = {(r.ko, r.home, r.away): (int(float(r.gh)) + int(float(r.ga)), "oficial") for r in of.itertuples()}
    import io, contextlib
    with contextlib.redirect_stdout(io.StringIO()): P = RF.placares()
    idx = RF._por_dia(P)
    rows = []
    for (ko, h, a), g in under[under.minuto > 0].groupby(["ko", "home", "away"]):
        f = fav.get((ko, h, a))
        if f is None or not np.isfinite(f): continue
        tot = ofi.get((ko, h, a))
        if tot is None:
            s = RF.achar_placar(P, idx, ko[:10], h, a); tot = (s[0] + s[1], s[2].split(":")[0]) if s else None
        if tot is None: continue
        ft, fonte = tot
        g = g.sort_values("minuto")
        for folga in FOLGAS:
            for lo, hi in JANELAS:
                w = g[(g.minuto >= lo) & (g.minuto < hi)]
                feito = False
                for ts, cap in w.groupby("ts", sort=False):
                    gols = G.get((ts, ko, h, a))
                    if gols is None: continue
                    L = gols + folga + 0.5
                    r = cap[cap.L == L]
                    if r.empty: continue
                    odd = float(r.back.iloc[0])
                    if not (ODD_LO <= odd <= ODD_HI): break          # 1a captura elegivel fora da faixa: nao entra nesta janela
                    win = ft < L
                    rows.append(dict(ko=ko, dia=ko[:10], home=h, away=a, folga=folga, janela="%d-%d" % (lo, hi), fav=V.fav_cat(f),
                                     gols=gols, linha=L, minuto=float(cap.minuto.iloc[0]), odd=odd, win=int(win), ft=ft, fonte=fonte,
                                     pnl=(1 - C) * (odd - 1) if win else -1.0, be=1 / (1 + (1 - C) * (odd - 1))))
                    feito = True; break
    df = pd.DataFrame(rows)
    if df.empty: print("sem apostas"); return
    print("apostas: %d | jogos: %d | dias: %d | placar: %s" % (len(df), df[["ko", "home", "away"]].drop_duplicates().shape[0], df.dia.nunique(), df.drop_duplicates(["ko", "home", "away"]).fonte.value_counts().to_dict()))
    cells = []
    for (fo, J, F), g in df.groupby(["folga", "janela", "fav"]):
        if len(g) < N_MIN: continue
        wr, be, roi = g.win.mean(), g.be.mean(), g.pnl.mean(); lo_, hi_, p0, nk = boot(g.pnl.values, g.dia.values)
        cells.append(dict(folga=fo, janela=J, fav=F, N=len(g), perdas=int((1 - g.win).sum()), WR=100 * wr, BE=100 * be, gap_pp=100 * (wr - be),
                          odd_med=g.odd.median(), ROI=100 * roi, IC_lo=100 * lo_ if np.isfinite(lo_) else np.nan, IC_hi=100 * hi_ if np.isfinite(hi_) else np.nan,
                          p=(p0 if roi > 0 else 1.0) if np.isfinite(p0) else 1.0, dias=nk))
    Tt = pd.DataFrame(cells); Tt["BH_passa"] = V.bh(Tt.p.values)
    Tt["decisao"] = np.where(Tt.BH_passa & (Tt.IC_lo > 0) & (Tt.perdas >= 5), "PASSA", np.where(Tt.IC_hi < 0, "REPROVA", "INCONCLUSIVO"))
    Tt = Tt.sort_values(["folga", "janela", "fav"])
    print("\n=== %d celulas (N>=%d) | BH sobre M=%d | %s ===" % (len(Tt), N_MIN, len(Tt), Tt.decisao.value_counts().to_dict()))
    print("%-5s %-6s %-9s %5s %5s %6s %6s %7s %5s %7s %-16s %s" % ("folga", "janela", "fav", "N", "perd", "WR", "BE", "gap", "odd", "ROI", "IC95", "decisao"))
    for _, r in Tt.iterrows():
        ic = "[%+.1f,%+.1f]" % (r.IC_lo, r.IC_hi) if np.isfinite(r.IC_lo) else "indef"
        print("%-5d %-6s %-9s %5d %5d %5.1f%% %5.1f%% %+6.2fpp %5.2f %+6.2f%% %-16s %s" % (r.folga, r.janela, r.fav, r.N, r.perdas, r.WR, r.BE, r.gap_pp, r.odd_med, r.ROI, ic, r.decisao))
    print("\n=== agregados ===")
    for fo, g in df.groupby("folga"):
        lo_, hi_, _, _ = boot(g.pnl.values, g.dia.values)
        print("  folga %d (Under g+%d.5): N=%d WR=%.1f%% BE=%.1f%% gap=%+.2fpp ROI=%+.2f%% IC95=[%+.1f,%+.1f] odd_med=%.2f" % (fo, fo, len(g), 100 * g.win.mean(), 100 * g.be.mean(), 100 * (g.win.mean() - g.be.mean()), 100 * g.pnl.mean(), 100 * lo_, 100 * hi_, g.odd.median()))
    for (fo, J), g in df.groupby(["folga", "janela"]):
        print("    folga %d janela %-6s N=%4d WR=%.1f%% BE=%.1f%% gap=%+.2fpp ROI=%+.2f%%" % (fo, J, len(g), 100 * g.win.mean(), 100 * g.be.mean(), 100 * (g.win.mean() - g.be.mean()), 100 * g.pnl.mean()))
    out = os.path.join(ROOT, "varredura_over", "under_a_frente_olhar_2026-09-15.csv"); Tt.to_csv(out, index=False, encoding="utf-8-sig")
    df.drop(columns=[]).to_csv(out.replace(".csv", "_apostas.csv"), index=False, encoding="utf-8-sig"); print("\n(tabela: %s)" % os.path.relpath(out, ROOT))


if __name__ == "__main__":
    main()
