# -*- coding: utf-8 -*-
"""
varredura_cs_coletor.py — roda a grade congelada em PREREGISTRO_cs_coletor.md.
Entrada (extraidas do coletor da VPS): cs_ko_all.csv (ko,home,away,mtk,runner,back,lay,lay_size — capturas de CS em
mtk [-5,15]), mo_ou_ko.csv (ko,home,away,mtk,mtype,runner,back — MATCH_ODDS e OVER_UNDER_25 pre-KO),
placares_ft.csv (liquidacao oficial). Placar externo (bases) so como fallback via relatorio_forward_5metodos.
Saida: tabela por celula + snapshot datado em varredura_over/cs_coletor_<data>.csv (+ _apostas.csv).
"""
import os, sys, re, unicodedata, argparse, warnings
from datetime import datetime
warnings.filterwarnings("ignore"); sys.stdout.reconfigure(encoding="utf-8")
ROOT = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, ROOT)
import pandas as pd, numpy as np
import relatorio_forward_5metodos as RF

C = 0.05; B = 10000; N_MIN = 50; REDS_MIN = 5
EXATOS = ["%d - %d" % (h, a) for h in range(4) for a in range(4)]
FAIXA = {r: (5, 40) for r in EXATOS}
FAIXA.update({"Any Other Home Win": (10, 80), "Any Other Away Win": (10, 80), "Any Other Draw": (10, 80)})
RE_CS = re.compile(r"^\s*(\d+)\s*-\s*(\d+)\s*$")


def canon(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower(); return re.sub(r"[^a-z0-9]", "", s)


def red_runner(runner, gh, ga):
    m = RE_CS.match(runner)
    if m: return (gh == int(m.group(1))) & (ga == int(m.group(2)))
    if runner == "Any Other Home Win": return (gh >= 4) & (gh > ga)
    if runner == "Any Other Away Win": return (ga >= 4) & (ga > gh)
    if runner == "Any Other Draw": return (gh == ga) & (gh >= 4)
    return np.zeros(len(gh), bool)


def fav_cat(o):
    return "<=1.40" if o <= 1.40 else ("1.40-1.80" if o <= 1.80 else ">1.80")


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
    ap = argparse.ArgumentParser(); ap.add_argument("--pasta", default=os.path.join(os.environ.get("TEMP", "."), "var")); a = ap.parse_args()
    cs = pd.read_csv(os.path.join(a.pasta, "cs_ko_all.csv"), header=None, names=["ko", "home", "away", "mtk", "runner", "back", "lay", "lay_size"], dtype=str)
    for c in ("mtk", "back", "lay", "lay_size"): cs[c] = pd.to_numeric(cs[c], errors="coerce")
    cs = cs.dropna(subset=["mtk", "lay"]); cs["absm"] = (cs.mtk - 5).abs()
    cs = cs.sort_values("absm").drop_duplicates(["ko", "home", "away", "runner"])          # captura mais perto do KO por runner
    mo = pd.read_csv(os.path.join(a.pasta, "mo_ou_ko.csv"), header=None, names=["ko", "home", "away", "mtk", "mtype", "runner", "back"], dtype=str)
    mo["mtk"] = pd.to_numeric(mo.mtk, errors="coerce"); mo["back"] = pd.to_numeric(mo.back, errors="coerce"); mo = mo.dropna(subset=["mtk", "back"])
    mo["absm"] = (mo.mtk - 5).abs(); mo = mo.sort_values("absm").drop_duplicates(["ko", "home", "away", "mtype", "runner"])
    m1 = mo[mo.mtype == "MATCH_ODDS"]
    fav = m1[m1.runner != "The Draw"].groupby(["ko", "home", "away"]).back.min().rename("fav")
    u25 = mo[(mo.mtype == "OVER_UNDER_25") & mo.runner.str.startswith("Under")].groupby(["ko", "home", "away"]).back.first().rename("u25")
    # placar: oficial primeiro
    of = pd.read_csv(os.path.join(a.pasta, "placares_ft.csv"), dtype=str).fillna("")
    of = of[(of.status == "LIQUIDADO") & (of.gh != "")]
    ofi = {(r.ko, r.home, r.away): (int(float(r.gh)), int(float(r.ga)), "oficial") for r in of.itertuples()}
    P = RF.placares(); idx = RF._por_dia(P)
    jogos = cs[["ko", "home", "away"]].drop_duplicates()
    placar = {}
    for r in jogos.itertuples():
        k = (r.ko, r.home, r.away)
        if k in ofi: placar[k] = ofi[k]; continue
        sc = RF.achar_placar(P, idx, r.ko[:10], r.home, r.away)
        if sc: placar[k] = (sc[0], sc[1], sc[2].split(":")[0])
    print("jogos com CS no KO: %d | com placar: %d (oficial %d, bases %d)" % (len(jogos), len(placar), sum(1 for v in placar.values() if v[2] == "oficial"), sum(1 for v in placar.values() if v[2] != "oficial")))
    df = cs.merge(fav, on=["ko", "home", "away"], how="left").merge(u25, on=["ko", "home", "away"], how="left")
    df = df[df.runner.isin(FAIXA)].copy()
    df["lo"] = df.runner.map(lambda r: FAIXA[r][0]); df["hi"] = df.runner.map(lambda r: FAIXA[r][1])
    df = df[(df.lay >= df.lo) & (df.lay <= df.hi) & df.fav.notna()].copy()
    df["gh"] = [placar.get((r.ko, r.home, r.away), (np.nan, np.nan, ""))[0] for r in df.itertuples()]
    df["ga"] = [placar.get((r.ko, r.home, r.away), (np.nan, np.nan, ""))[1] for r in df.itertuples()]
    df["fonte"] = [placar.get((r.ko, r.home, r.away), (np.nan, np.nan, ""))[2] for r in df.itertuples()]
    df = df.dropna(subset=["gh", "ga"]); df["gh"] = df.gh.astype(int); df["ga"] = df.ga.astype(int)
    df["red"] = 0
    for rn, g in df.groupby("runner"): df.loc[g.index, "red"] = red_runner(rn, g.gh.values, g.ga.values).astype(int)
    df["pnl"] = np.where(df.red == 1, -1.0, (1 - C) / (df.lay - 1)); df["be"] = (df.lay - 1) / (df.lay - C)
    df["favc"] = df.fav.map(fav_cat); df["dia"] = df.ko.str[:10]
    df["rank"] = df.groupby(["dia", "runner"]).lay.rank(method="first")
    print("apostas avaliadas: %d | jogos: %d | dias: %d | fonte do placar: %s" % (len(df), df[["ko", "home", "away"]].drop_duplicates().shape[0], df.dia.nunique(), df.drop_duplicates(["ko", "home", "away"]).fonte.value_counts().to_dict()))
    rows = []
    for (rn, fc), g0 in df.groupby(["runner", "favc"]):
        for ctag, k in (("Todos", None), ("TOP 3", 3), ("TOP 1", 1)):
            g = g0 if k is None else g0[g0["rank"] <= k]
            if len(g) < N_MIN: continue
            wr, be, roi = 1 - g.red.mean(), g.be.mean(), g.pnl.mean(); lo_, hi_, p0, nk = boot(g.pnl.values, g.dia.values)
            p = (p0 if roi > 0 else 1.0) if np.isfinite(p0) else 1.0
            rows.append(dict(runner=rn, fav=fc, corte=ctag, N=len(g), reds=int(g.red.sum()), red_pct=100 * g.red.mean(), WR=100 * wr, BE=100 * be,
                             edge_pp=100 * (wr - be), odd_med=g.lay.median(), lay_size_med=g.lay_size.median(), ROI=100 * roi,
                             IC_lo=100 * lo_ if np.isfinite(lo_) else np.nan, IC_hi=100 * hi_ if np.isfinite(hi_) else np.nan, p=p, dias=nk))
    T = pd.DataFrame(rows)
    if T.empty: print("nenhuma celula com N>=%d" % N_MIN); return
    T["BH_passa"] = bh(T.p.values)
    def dec(r):
        if r.BH_passa and np.isfinite(r.IC_lo) and r.IC_lo > 0: return "PASSA" if r.reds >= REDS_MIN else "INCONCLUSIVO (reds<%d)" % REDS_MIN
        if np.isfinite(r.IC_hi) and r.IC_hi < 0: return "REPROVA"
        return "INCONCLUSIVO"
    T["decisao"] = T.apply(dec, axis=1); T = T.sort_values("p")
    os.makedirs(os.path.join(ROOT, "varredura_over"), exist_ok=True)
    snap = os.path.join(ROOT, "varredura_over", "cs_coletor_%s.csv" % datetime.now().strftime("%Y-%m-%d"))
    T.to_csv(snap, index=False, encoding="utf-8-sig"); df.to_csv(snap.replace(".csv", "_apostas.csv"), index=False, encoding="utf-8-sig")
    print("\n=== GRADE: %d celulas com N>=%d | BH q=0,05 sobre M=%d | decisao: %s ===" % (len(T), N_MIN, len(T), T.decisao.value_counts().to_dict()))
    print("%-20s %-9s %-6s %5s %5s %6s %6s %6s %7s %6s %7s %7s %-18s %-7s %s" % ("runner", "fav", "corte", "N", "reds", "red%", "WR", "BE", "edge", "odd", "size", "ROI", "IC95", "p", "decisao"))
    for _, r in T.head(30).iterrows():
        ic = "[%+.1f,%+.1f]" % (r.IC_lo, r.IC_hi) if np.isfinite(r.IC_lo) else "indef"
        print("%-20s %-9s %-6s %5d %5d %5.1f%% %5.1f%% %5.1f%% %+6.2fpp %6.1f %7.0f %+6.2f%% %-18s %-7.4f %s" % (r.runner, r.fav, r.corte, r.N, r.reds, r.red_pct, r.WR, r.BE, r.edge_pp, r.odd_med, r.lay_size_med, r.ROI, ic, r.p, r.decisao))
    print("\n=== por runner (Todos, sem favoritismo) ===")
    for rn, g in df.groupby("runner"):
        print("  %-20s N=%5d reds=%3d red%%=%5.2f%% BE-red=%5.2f%% edge=%+5.2fpp ROI=%+6.2f%% odd_med=%5.1f size=%4.0f" % (rn, len(g), g.red.sum(), 100 * g.red.mean(), 100 * (1 - g.be.mean()), 100 * (1 - g.red.mean() - g.be.mean()), 100 * g.pnl.mean(), g.lay.median(), g.lay_size.median()))
    print("\n(tabela completa: %s)" % os.path.relpath(snap, ROOT))


if __name__ == "__main__":
    main()
