# -*- coding: utf-8 -*-
"""
varredura_lay2x2_apos_gol.py — roda a grade congelada em PREREGISTRO_lay2x2_apos_gol.md.
Entrada (coletor): cs22.csv (ts,ko,home,away,mtk,back,lay,lay_size — runner '2 - 2', in-play e pre-KO),
ou.csv (linhas O/U in-play, p/ o estado), mo_pre.csv (favorito pre-KO), placares_ft.csv (oficial).
Estado = gols pelas linhas O/U (gols_por_ou de varredura_over_inplay) + divisao casa/fora pelo CS so se coincidir.
"""
import os, sys, argparse, warnings
from datetime import datetime
warnings.filterwarnings("ignore"); sys.stdout.reconfigure(encoding="utf-8")
ROOT = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, ROOT)
import pandas as pd, numpy as np
import relatorio_forward_5metodos as RF
import varredura_over_inplay as V

C = 0.05; B = 10000; N_MIN = 50; REDS_MIN = 5
JANELAS = [(10, 25), (25, 40), (46, 60), (60, 75)]
LO, HI = 5.0, 30.0


def boot(v, blocos, seed=5):
    mm = {k: v[blocos == k] for k in pd.unique(blocos)}; ks = list(mm)
    if len(ks) < 6: return np.nan, np.nan, np.nan, len(ks)
    rng = np.random.default_rng(seed); r = np.empty(B)
    for i in range(B): r[i] = np.concatenate([mm[ks[j]] for j in rng.integers(0, len(ks), len(ks))]).mean()
    return np.percentile(r, 2.5), np.percentile(r, 97.5), float((r <= 0).mean()), len(ks)


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--pasta", default=os.path.join(os.environ.get("TEMP", "."), "var")); a = ap.parse_args()
    T = a.pasta
    cs = pd.read_csv(os.path.join(T, "cs22.csv"), header=None, names=["ts", "ko", "home", "away", "mtk", "back", "lay", "lay_size"], dtype=str)
    for c in ("mtk", "back", "lay", "lay_size"): cs[c] = pd.to_numeric(cs[c], errors="coerce")
    cs = cs.dropna(subset=["mtk", "lay"]); cs["minuto"] = -cs.mtk - 15
    ou, cs_all, fav = V.carregar(T)                       # ou: O/U in-play; fav: menor back pre-KO
    G = V.gols_por_ou(ou)                                  # (ts,ko,home,away) -> gols ja saidos (fato)
    csm = pd.read_csv(os.path.join(T, "cs.csv"), header=None, names=["ts", "ko", "home", "away", "mtk", "score"], dtype=str) if os.path.exists(os.path.join(T, "cs.csv")) else None
    split = {}
    if csm is not None:
        csm[["gh", "ga"]] = csm.score.str.extract(r"(\d+)\s*-\s*(\d+)").astype(float)
        for r in csm.dropna(subset=["gh", "ga"]).itertuples(): split[(r.ts, r.ko, r.home, r.away)] = (int(r.gh), int(r.ga))
    # placar oficial primeiro, bases depois
    of = pd.read_csv(os.path.join(T, "placares_ft.csv"), dtype=str).fillna(""); of = of[(of.status == "LIQUIDADO") & (of.gh != "")]
    ofi = {(r.ko, r.home, r.away): (int(float(r.gh)), int(float(r.ga)), "oficial") for r in of.itertuples()}
    P = RF.placares(); idx = RF._por_dia(P)
    pre = cs[(cs.mtk >= -5) & (cs.mtk <= 15)].sort_values("mtk").groupby(["ko", "home", "away"]).lay.first().rename("lay_pre")
    live = cs[cs.minuto > 0].copy()
    rows = []
    for (ko, h, a), g in live.groupby(["ko", "home", "away"]):
        k = (ko, h, a); f = fav.get(k)
        if f is None or not np.isfinite(f): continue
        sc = ofi.get(k) or (lambda s: (s[0], s[1], s[2].split(":")[0]) if s else None)(RF.achar_placar(P, idx, ko[:10], h, a))
        if sc is None: continue
        gh_ft, ga_ft, fonte = sc
        g = g.sort_values("minuto")
        for lo, hi in JANELAS:
            w = g[(g.minuto >= lo) & (g.minuto < hi)]
            for cap in w.itertuples():
                tot = G.get((cap.ts, ko, h, a))
                if tot != 1: continue                                    # estado: exatamente 1 gol (fato pelo O/U)
                s = split.get((cap.ts, ko, h, a)); quem = ("mand" if s[0] == 1 else "visit") if (s and s[0] + s[1] == 1) else "?"
                if not (LO <= cap.lay <= HI): break                       # 1a captura elegivel: se fora da faixa, nao entra nesta janela
                rows.append(dict(ko=ko, dia=ko[:10], home=h, away=a, janela="%d-%d" % (lo, hi), fav=V.fav_cat(f), quem=quem,
                                 minuto=cap.minuto, odd=cap.lay, lay_size=cap.lay_size, lay_pre=pre.get(k, np.nan),
                                 red=int(gh_ft == 2 and ga_ft == 2), fonte=fonte))
                break
    df = pd.DataFrame(rows)
    if df.empty: print("sem apostas"); return
    df["pnl"] = np.where(df.red == 1, -1.0, (1 - C) / (df.odd - 1)); df["be"] = (df.odd - 1) / (df.odd - C)
    print("apostas: %d | jogos: %d | dias: %d | fonte: %s" % (len(df), df[["ko", "home", "away"]].drop_duplicates().shape[0], df.dia.nunique(), df.drop_duplicates(["ko", "home", "away"]).fonte.value_counts().to_dict()))
    q = df.dropna(subset=["lay_pre"])
    print("a odd cai depois do gol? lay 2-2 pre-KO mediana %.1f -> no instante (1 gol) %.1f | queda mediana %.1f pontos" % (q.lay_pre.median(), q.odd.median(), (q.lay_pre - q.odd).median()))
    cells = []
    def add(tag_estado, sub):
        for (J, F), g in sub.groupby(["janela", "fav"]):
            if len(g) < N_MIN: continue
            wr, be, roi = 1 - g.red.mean(), g.be.mean(), g.pnl.mean(); lo_, hi_, p0, nk = boot(g.pnl.values, g.dia.values)
            p = (p0 if roi > 0 else 1.0) if np.isfinite(p0) else 1.0
            cells.append(dict(estado=tag_estado, janela=J, fav=F, N=len(g), reds=int(g.red.sum()), red_pct=100 * g.red.mean(), WR=100 * wr, BE=100 * be,
                              edge_pp=100 * (wr - be), odd_med=g.odd.median(), ROI=100 * roi, IC_lo=100 * lo_ if np.isfinite(lo_) else np.nan,
                              IC_hi=100 * hi_ if np.isfinite(hi_) else np.nan, p=p, dias=nk))
    add("1 gol", df); add("1-0 (mand marcou)", df[df.quem == "mand"]); add("0-1 (visit marcou)", df[df.quem == "visit"])
    Tt = pd.DataFrame(cells)
    if Tt.empty: print("nenhuma celula com N>=%d" % N_MIN); return
    Tt["BH_passa"] = V.bh(Tt.p.values)
    def dec(r):
        if r.BH_passa and np.isfinite(r.IC_lo) and r.IC_lo > 0: return "PASSA" if (r.reds >= REDS_MIN and r.N >= 300) else "INCONCLUSIVO (N<300 ou reds<5)"
        if np.isfinite(r.IC_hi) and r.IC_hi < 0: return "REPROVA"
        return "INCONCLUSIVO"
    Tt["decisao"] = Tt.apply(dec, axis=1); Tt = Tt.sort_values("p")
    snap = os.path.join(ROOT, "varredura_over", "lay2x2_apos_gol_%s.csv" % datetime.now().strftime("%Y-%m-%d"))
    Tt.to_csv(snap, index=False, encoding="utf-8-sig"); df.to_csv(snap.replace(".csv", "_apostas.csv"), index=False, encoding="utf-8-sig")
    print("\n=== %d celulas com N>=%d | BH sobre M=%d | %s ===" % (len(Tt), N_MIN, len(Tt), Tt.decisao.value_counts().to_dict()))
    print("%-19s %-6s %-9s %5s %5s %6s %6s %6s %7s %6s %7s %-18s %-7s %s" % ("estado", "janela", "fav", "N", "reds", "red%", "WR", "BE", "edge", "odd", "ROI", "IC95", "p", "decisao"))
    for _, r in Tt.iterrows():
        ic = "[%+.1f,%+.1f]" % (r.IC_lo, r.IC_hi) if np.isfinite(r.IC_lo) else "indef(%dd)" % r.dias
        print("%-19s %-6s %-9s %5d %5d %5.1f%% %5.1f%% %5.1f%% %+6.2fpp %6.1f %+6.2f%% %-18s %-7.4f %s" % (r.estado, r.janela, r.fav, r.N, r.reds, r.red_pct, r.WR, r.BE, r.edge_pp, r.odd_med, r.ROI, ic, r.p, r.decisao))
    print("\n=== agregados ===")
    for tag, sub in (("TOTAL 1 gol", df), ("mandante marcou", df[df.quem == "mand"]), ("visitante marcou", df[df.quem == "visit"])):
        if len(sub): print("  %-18s N=%5d reds=%3d red%%=%5.2f%% BE-red=%5.2f%% edge=%+5.2fpp ROI=%+6.2f%% odd_med=%5.1f" % (tag, len(sub), sub.red.sum(), 100 * sub.red.mean(), 100 * (1 - sub.be.mean()), 100 * (1 - sub.red.mean() - sub.be.mean()), 100 * sub.pnl.mean(), sub.odd.median()))
    for J, sub in df.groupby("janela"):
        print("  janela %-7s N=%5d reds=%3d red%%=%5.2f%% BE-red=%5.2f%% edge=%+5.2fpp ROI=%+6.2f%% odd_med=%5.1f" % (J, len(sub), sub.red.sum(), 100 * sub.red.mean(), 100 * (1 - sub.be.mean()), 100 * (1 - sub.red.mean() - sub.be.mean()), 100 * sub.pnl.mean(), sub.odd.median()))
    print("\n(tabela: %s)" % os.path.relpath(snap, ROOT))


if __name__ == "__main__":
    main()
