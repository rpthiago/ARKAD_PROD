# -*- coding: utf-8 -*-
"""
varredura_over_inplay.py — roda a grade congelada em PREREGISTRO_varredura_over_inplay.md.
Entrada: extracao do coletor (ou.csv, cs.csv, mo_pre.csv) + placar externo (relatorio_forward_5metodos.placares).
Saida: tabela por celula + snapshot datado em varredura_over/.
"""
import os, sys, re, unicodedata, argparse, warnings
from datetime import datetime
warnings.filterwarnings("ignore"); sys.stdout.reconfigure(encoding="utf-8")
ROOT = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, ROOT)
import pandas as pd, numpy as np
import relatorio_forward_5metodos as RF

C = 0.05
LINHAS = {"OVER_UNDER_05": 0.5, "OVER_UNDER_15": 1.5, "OVER_UNDER_25": 2.5, "OVER_UNDER_35": 3.5}
JANELAS = [(10, 25), (25, 40), (46, 60), (60, 75), (75, 85)]
N_MIN, MIN_BLOCOS, B = 30, 6, 10000


def estado(gh, ga):
    if gh == 0 and ga == 0: return "0-0"
    if gh + ga == 1: return "1 gol"
    if gh == 1 and ga == 1: return "1-1"
    if gh == ga: return "empate 2-2+"
    if abs(gh - ga) >= 2: return "diff>=2"
    return "outro"          # 2-1, 3-2... (diff 1 com 3+ gols)


def fav_cat(o):
    if o is None or not np.isfinite(o): return None
    return "<=1.40" if o <= 1.40 else ("1.40-1.80" if o <= 1.80 else ">1.80")


def carregar(pasta):
    ou = pd.read_csv(os.path.join(pasta, "ou.csv"), header=None,
                     names=["ts", "ko", "home", "away", "mtk", "mtype", "runner", "back"], dtype=str)
    cs = pd.read_csv(os.path.join(pasta, "cs.csv"), header=None,
                     names=["ts", "ko", "home", "away", "mtk", "score"], dtype=str)
    mo = pd.read_csv(os.path.join(pasta, "mo_pre.csv"), header=None,
                     names=["ko", "home", "away", "runner", "back"], dtype=str)
    for d in (ou, cs):
        d["mtk"] = pd.to_numeric(d["mtk"], errors="coerce"); d["minuto"] = -d["mtk"] - 15
    ou["back"] = pd.to_numeric(ou["back"], errors="coerce")
    ou = ou[ou["runner"].str.startswith("Over", na=False)].dropna(subset=["back", "minuto"])
    cs[["gh", "ga"]] = cs["score"].str.extract(r"(\d+)\s*-\s*(\d+)").astype(float)
    cs = cs.dropna(subset=["gh", "ga", "minuto"])
    mo["back"] = pd.to_numeric(mo["back"], errors="coerce")
    fav = {}
    for (ko, h, a), g in mo.groupby(["ko", "home", "away"]):
        r = g[g["runner"].isin([h, a])]
        if len(r): fav[(ko, h, a)] = float(r["back"].min())
    return ou, cs, fav


def montar_apostas(ou, cs, fav, P, idx):
    """Uma linha por (jogo, janela, linha): odd de back na 1a captura da janela + estado + resultado."""
    cs_k = {k: g.sort_values("minuto") for k, g in cs.groupby(["ts", "ko", "home", "away"])}
    out = []
    sem_placar = set()
    for (ko, h, a, mtype), g in ou.groupby(["ko", "home", "away", "mtype"]):
        L = LINHAS[mtype]
        sc = RF.achar_placar(P, idx, ko[:10], h, a)
        if sc is None:
            sem_placar.add((ko, h, a)); continue
        gh_ft, ga_ft, fonte = sc
        f = fav_cat(fav.get((ko, h, a)))
        if f is None: continue
        g = g.sort_values("minuto")
        for lo, hi in JANELAS:
            w = g[(g["minuto"] >= lo) & (g["minuto"] < hi)]
            if w.empty: continue
            cap = w.iloc[0]
            st = cs_k.get((cap["ts"], ko, h, a))
            if st is None or st.empty: continue
            gh, ga = int(st.iloc[0]["gh"]), int(st.iloc[0]["ga"])
            if gh + ga > L:            # linha ja batida: celula impossivel
                continue
            S = estado(gh, ga)
            if S == "outro": continue
            odd = float(cap["back"])
            if not (1.01 < odd < 50): continue
            green = (gh_ft + ga_ft) > L
            out.append(dict(ko=ko, dia=ko[:10], home=h, away=a, linha=L, janela="%d-%d" % (lo, hi),
                            estado=S, fav=f, minuto=float(cap["minuto"]), odd=odd, gols_mom=gh + ga,
                            gols_ft=gh_ft + ga_ft, green=int(green),
                            pnl=(odd - 1) * (1 - C) if green else -1.0, be=1 / (1 + (odd - 1) * (1 - C)),
                            fonte=fonte.split(":")[0]))
    return pd.DataFrame(out), len(sem_placar)


def boot_dia(df):
    mm = {k: g["pnl"].values for k, g in df.groupby("dia")}; ks = list(mm)
    if len(ks) < MIN_BLOCOS: return np.nan, np.nan, np.nan, len(ks)
    rng = np.random.default_rng(7); r = np.empty(B)
    for i in range(B):
        r[i] = np.concatenate([mm[ks[j]] for j in rng.integers(0, len(ks), len(ks))]).mean()
    lo, hi = np.percentile(r, [2.5, 97.5])
    return lo, hi, float((r <= 0).mean()), len(ks)


def bh(pvals, q=0.05):
    p = np.asarray(pvals, float); m = len(p); ordem = np.argsort(p); passa = np.zeros(m, bool)
    thr = q * (np.arange(1, m + 1) / m); ok = p[ordem] <= thr
    if ok.any():
        k = np.max(np.where(ok)[0]); passa[ordem[:k + 1]] = True
    return passa


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pasta", default=os.path.join(os.environ.get("TEMP", "."), "var"))
    a = ap.parse_args()
    ou, cs, fav = carregar(a.pasta)
    print("coletor: %d capturas O/U | %d capturas CS | %d jogos com favorito pre-jogo" % (len(ou), len(cs), len(fav)))
    P = RF.placares(); idx = RF._por_dia(P)
    df, sem = montar_apostas(ou, cs, fav, P, idx)
    print("jogos sem placar externo (fora): %d | apostas avaliadas: %d | jogos distintos: %d | dias: %d"
          % (sem, len(df), df[["ko", "home", "away"]].drop_duplicates().shape[0], df["dia"].nunique()))
    print("fonte do placar:", df.drop_duplicates(["ko", "home", "away"])["fonte"].value_counts().to_dict())

    rows = []
    for (L, J, S, F), g in df.groupby(["linha", "janela", "estado", "fav"]):
        if len(g) < N_MIN: continue
        wr, be, roi = g["green"].mean(), g["be"].mean(), g["pnl"].mean()
        lo, hi, p0, nk = boot_dia(g)
        p_dir = (p0 if roi > 0 else 1.0) if np.isfinite(p0) else 1.0
        rows.append(dict(linha=L, janela=J, estado=S, fav=F, N=len(g), WR=100 * wr, BE=100 * be, gap_pp=100 * (wr - be),
                         ROI=100 * roi, odd_med=g["odd"].median(), IC_lo=100 * lo if np.isfinite(lo) else np.nan,
                         IC_hi=100 * hi if np.isfinite(hi) else np.nan, p=p_dir, blocos=nk))
    T = pd.DataFrame(rows)
    if T.empty:
        print("nenhuma celula com N>=%d" % N_MIN); return
    T["BH_passa"] = bh(T["p"].values)
    def decisao(r):
        if r["BH_passa"] and np.isfinite(r["IC_lo"]) and r["IC_lo"] > 0: return "PASSA"
        if np.isfinite(r["IC_hi"]) and r["IC_hi"] < 0: return "REPROVA"
        return "INCONCLUSIVO"
    T["decisao"] = T.apply(decisao, axis=1)
    T = T.sort_values("p")
    os.makedirs(os.path.join(ROOT, "varredura_over"), exist_ok=True)
    snap = os.path.join(ROOT, "varredura_over", "varredura_over_%s.csv" % datetime.now().strftime("%Y-%m-%d"))
    T.to_csv(snap, index=False, encoding="utf-8-sig")

    print("\n=== GRADE: %d celulas com N>=%d | BH q=0,05 sobre M=%d ===" % (len(T), N_MIN, len(T)))
    print("decisao:", T["decisao"].value_counts().to_dict())
    print("\n%-5s %-6s %-12s %-10s %5s %6s %6s %7s %7s %6s %-18s %-8s %s" % ("linha", "janela", "estado", "fav", "N", "WR", "BE", "gap", "ROI", "odd", "IC95", "p", "decisao"))
    for _, r in T.head(25).iterrows():
        ic = "[%+.1f,%+.1f]" % (r["IC_lo"], r["IC_hi"]) if np.isfinite(r["IC_lo"]) else "nk=%d" % r["blocos"]
        print("%-5s %-6s %-12s %-10s %5d %5.1f%% %5.1f%% %+6.1fpp %+6.1f%% %6.2f %-18s %-8.4f %s" % (
            r["linha"], r["janela"], r["estado"], r["fav"], r["N"], r["WR"], r["BE"], r["gap_pp"], r["ROI"], r["odd_med"], ic, r["p"], r["decisao"]))
    print("\n(25 melhores por p; tabela completa em %s)" % os.path.relpath(snap, ROOT))
    # visao agregada por dimensao, para leitura
    print("\n=== por linha (todas as celulas somadas) ===")
    for L, g in df.groupby("linha"):
        print("  Over %.1f: N=%5d WR=%5.1f%% BE=%5.1f%% gap=%+5.1fpp ROI=%+5.1f%%" % (L, len(g), 100 * g.green.mean(), 100 * g.be.mean(), 100 * (g.green.mean() - g.be.mean()), 100 * g.pnl.mean()))
    print("=== por janela ===")
    for J, g in df.groupby("janela"):
        print("  min %-6s N=%5d WR=%5.1f%% BE=%5.1f%% gap=%+5.1fpp ROI=%+5.1f%%" % (J, len(g), 100 * g.green.mean(), 100 * g.be.mean(), 100 * (g.green.mean() - g.be.mean()), 100 * g.pnl.mean()))
    print("=== por estado ===")
    for S, g in df.groupby("estado"):
        print("  %-12s N=%5d WR=%5.1f%% BE=%5.1f%% gap=%+5.1fpp ROI=%+5.1f%%" % (S, len(g), 100 * g.green.mean(), 100 * g.be.mean(), 100 * (g.green.mean() - g.be.mean()), 100 * g.pnl.mean()))
    print("=== TOTAL: N=%d WR=%.1f%% BE=%.1f%% gap=%+.1fpp ROI=%+.1f%% ===" % (len(df), 100 * df.green.mean(), 100 * df.be.mean(), 100 * (df.green.mean() - df.be.mean()), 100 * df.pnl.mean()))

    # ---- H-extra (pre-registrada 12/09, ver PREREGISTRO): Over 1.5 · 1 gol · fav>1.80 · min 25-85,
    #      1 aposta/jogo, SO jogos com KO >= 2026-09-13 (os do snapshot 1 nao contam)
    hx = df[(df["linha"] == 1.5) & (df["estado"] == "1 gol") & (df["fav"] == ">1.80") &
            (df["minuto"] >= 25) & (df["minuto"] < 85) & (df["ko"] >= "2026-09-13")]
    hx = hx.sort_values("minuto").drop_duplicates(["ko", "home", "away"], keep="first")
    print("\n=== H-EXTRA (Over 1.5 · 1 gol · fav>1.80 · min 25-85 · KO>=13/09) ===")
    if hx.empty:
        print("  sem jogos elegiveis ainda (dado de julgamento comeca em 13/09)")
    else:
        wr, be, roi = hx.green.mean(), hx.be.mean(), hx.pnl.mean(); lo, hi, p0, nk = boot_dia(hx)
        ic = "[%+.1f%%,%+.1f%%] p=%.4f" % (100 * lo, 100 * hi, p0) if np.isfinite(lo) else "IC INDEFINIDO (nk=%d<%d)" % (nk, MIN_BLOCOS)
        print("  N=%d (de 300) | WR=%.1f%% BE=%.1f%% gap=%+.1fpp | ROI=%+.1f%% | IC95 %s" % (len(hx), 100 * wr, 100 * be, 100 * (wr - be), 100 * roi, ic))
        if np.isfinite(hi) and hi < 0: print("  -> REPROVA")
        elif len(hx) >= 300 and np.isfinite(lo) and lo > 0 and p0 <= 0.05: print("  -> APROVA (snapshot 3 precisa confirmar com N novo)")
        else: print("  -> INCONCLUSIVO")


if __name__ == "__main__":
    main()
