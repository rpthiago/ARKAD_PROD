# -*- coding: utf-8 -*-
"""
trader_inplay_olhar.py — roda o MESMO núcleo (trader_inplay_core) sobre o histórico do coletor.
Serve para (1) medir a odd real no instante de entrada de cada método e (2) o primeiro olhar declarado.
Entrada: trader_hist.csv extraído do coletor (ts,ko,home,away,mtk,mtype,runner,back,back_size,lay,lay_size).
"""
import os, sys, argparse, warnings
warnings.filterwarnings("ignore"); sys.stdout.reconfigure(encoding="utf-8")
ROOT = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, ROOT)
import pandas as pd, numpy as np
import trader_inplay_core as C

B = 5000


def boot(v, dias, seed=5):
    d = pd.DataFrame({"p": v, "d": dias}).groupby("d")["p"].agg(["sum", "count"]); K = len(d)
    if K < 6: return np.nan, np.nan, K
    S, Cn = d["sum"].values, d["count"].values.astype(float); rng = np.random.default_rng(seed); idx = rng.integers(0, K, (B, K))
    r = S[idx].sum(1) / Cn[idx].sum(1); return np.percentile(r, 2.5), np.percentile(r, 97.5), K


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--arq", default=os.path.join(os.environ.get("TEMP", "."), "trader_hist.csv")); ap.add_argument("--desde", default=None); a = ap.parse_args()
    d = pd.read_csv(a.arq, header=None, names=["ts", "ko", "home", "away", "mtk", "mtype", "runner", "back", "bsz", "lay", "lsz"],
                    dtype={"ts": str, "ko": str, "home": str, "away": str, "mtype": str, "runner": str}, low_memory=False)
    for c in ("mtk", "back", "bsz", "lay", "lsz"): d[c] = pd.to_numeric(d[c], errors="coerce")
    d = d.dropna(subset=["mtk", "ko"]); d["te"] = pd.to_datetime(d.ts, errors="coerce").astype("int64") // 10 ** 9
    if a.desde: d = d[d.ko >= a.desde]
    d = d.sort_values(["ko", "home", "away", "te"])
    print("linhas: %d | jogos: %d | %s -> %s" % (len(d), d[["ko", "home", "away"]].drop_duplicates().shape[0], d.ko.min(), d.ko.max()))
    trades = []
    for (ko, h, aw), g in d.groupby(["ko", "home", "away"], sort=False):
        J = C.Jogo(ko, h, aw)
        linhas = list(zip(g.te.values, g.ts.values, g.mtk.values, g.mtype.values, g.runner.values,
                          [None if np.isnan(x) else float(x) for x in g.back.values], g.bsz.fillna(0).values,
                          [None if np.isnan(x) else float(x) for x in g.lay.values], g.lsz.fillna(0).values))
        for ts, mtk, mk in C.agrupar_capturas(linhas):
            J.captura(ts, float(mtk), mk)
        J.encerrar()
        for t in J.fechados:
            t.update(ko=ko, home=h, away=aw, dia=ko[:10], fav=J.fav_pre[0] if J.fav_pre else None); trades.append(t)
    T = pd.DataFrame(trades)
    if T.empty: print("nenhuma entrada"); return
    out = os.path.join(ROOT, "varredura_over", "trader_inplay_olhar_%s.csv" % pd.Timestamp.now().strftime("%Y-%m-%d"))
    T.to_csv(out, index=False, encoding="utf-8-sig")
    for m in C.METODOS:
        t = T[T.id == m["id"]]
        if t.empty: print("\n== %s: 0 entradas ==" % m["nome"]); continue
        print("\n== %s ==  primeira captura elegível: %d jogos | status: %s" % (m["nome"], len(t), t.status.value_counts().to_dict()))
        q = t.odd_in.quantile([.05, .25, .5, .75, .95]).values
        print("   odd no instante (todas as entradas elegíveis): p5 %.2f | p25 %.2f | mediana %.2f | p75 %.2f | p95 %.2f | na faixa %.2f-%.2f: %.1f%%" % (*q, m["odd"][0], m["odd"][1], 100 * t.odd_in.between(*m["odd"]).mean()))
        print("   liquidez no instante: mediana %.0f | >=200: %.1f%%" % (t.liq_in.median(), 100 * (t.liq_in >= 200).mean()))
        f = t[t.status == "FECHADO"]
        if len(f):
            lo, hi, K = boot(f.pnl.values, f.dia.values)
            print("   FECHADOS: N=%d | verdes %.1f%% | pnl médio %+.4f u/u de risco (ROI %+.2f%%) | IC95 [%+.1f%%, %+.1f%%] | dias %d" % (len(f), 100 * (f.pnl > 0).mean(), f.pnl.mean(), 100 * f.pnl.mean(), 100 * lo, 100 * hi, K))
            print("   por motivo de saída:"); print(f.groupby("motivo").agg(N=("pnl", "size"), pnl_medio=("pnl", "mean"), odd_in=("odd_in", "median"), odd_out=("odd_out", "median")).round(4).to_string())
    print("\n(tabela: %s)" % os.path.relpath(out, ROOT))


if __name__ == "__main__":
    main()
