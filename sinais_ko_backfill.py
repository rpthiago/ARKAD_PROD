# -*- coding: utf-8 -*-
"""
sinais_ko_backfill.py — preenche o ledger do KO (forward_ko_ledger.csv) desde 16/08/2026 com a odd de lay real do
coletor na primeira captura dentro de JANELA_KO, usando o MESMO sinais_ko_core do servico da VPS.
Entrada: ko_hist.csv extraido do coletor (ts,mtype,competicao,home,away,ko,mtk,runner,back,bsz,lay,lsz), mtk em [3,17].
Linhas ganham origem='backfill'. Nao sobrescreve o que o servico ja gravou (chave Data|Metodo|Home|Away).
"""
import os, sys, csv, argparse, warnings
from datetime import datetime, timedelta
warnings.filterwarnings("ignore"); sys.stdout.reconfigure(encoding="utf-8")
ROOT = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, ROOT)
import pandas as pd, numpy as np
import sinais_ko_core as K
from sinais_ko_vps import COLS, GAP_S

LEDGER = os.path.join(ROOT, "metodos_aprovados", "forward_ko_ledger.csv")


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--arq", default=os.path.join(os.environ.get("TEMP", "."), "ko_hist.csv")); a = ap.parse_args()
    d = pd.read_csv(a.arq, header=None, names=["ts", "mtype", "comp", "home", "away", "ko", "mtk", "runner", "back", "bsz", "lay", "lsz"], dtype=str, low_memory=False)
    for c in ("mtk", "back", "bsz", "lay", "lsz"): d[c] = pd.to_numeric(d[c], errors="coerce")
    d = d.dropna(subset=["mtk", "ko"]); d["te"] = pd.to_datetime(d.ts, errors="coerce").astype("int64") // 10 ** 9
    d = d.sort_values(["ko", "home", "away", "te"])
    print("linhas %d | jogos %d | %s -> %s" % (len(d), d[["ko", "home", "away"]].drop_duplicates().shape[0], d.ko.min(), d.ko.max()))
    existentes = set()
    if os.path.exists(LEDGER):
        L = pd.read_csv(LEDGER, dtype=str).fillna("")
        existentes = set(zip(L.Data, L.Metodo, L.Home, L.Away))
    top3 = {}; dia_top3 = None; rows = []
    for (ko, h, aw), g in d.groupby(["ko", "home", "away"], sort=False):
        comp = str(g.comp.iloc[0]) if pd.notna(g.comp.iloc[0]) else ""
        # passes (linhas a < GAP_S s), so os dentro da janela; avalia a PRIMEIRA (a mais longe do KO dentro da janela)
        passes, cur, t0 = [], None, None
        for r in g.itertuples():
            if cur is None or r.te - t0 > GAP_S:
                if cur is not None: passes.append(cur)
                cur = (r.ts, r.mtk, {}); t0 = r.te
            cur[2].setdefault(r.mtype, {})[r.runner] = (None if np.isnan(r.back) else r.back, 0 if np.isnan(r.bsz) else r.bsz,
                                                         None if np.isnan(r.lay) else r.lay, 0 if np.isnan(r.lsz) else r.lsz)
        if cur is not None: passes.append(cur)
        passes = [p for p in passes if K.JANELA_KO[0] <= p[1] <= K.JANELA_KO[1]]
        if not passes: continue
        ts, mtk, mk = passes[0]
        dia = ko[:10]
        if dia_top3 != dia: top3 = {}; dia_top3 = dia
        sinais = K.avaliar(mk, h, aw, comp, top3)
        kodt = datetime.strptime(ko, "%Y-%m-%d %H:%M") - timedelta(hours=3)
        for metodo, odd, liq, fav in sinais:
            key = (kodt.strftime("%Y-%m-%d"), metodo, h, aw)
            if key in existentes: continue
            be = (odd - 1) / (odd - 0.05)
            rows.append([kodt.strftime("%Y-%m-%d"), metodo, comp, h, aw, kodt.strftime("%H:%M"), round(odd, 2), round(fav, 2) if fav else "",
                         round(liq, 0), round(mtk, 1), ts, "backfill", "PENDENTE", "", "", "", "", "", "", round(be, 4), "", ""])
    novo = not os.path.exists(LEDGER)
    with open(LEDGER, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if novo: w.writerow(COLS)
        w.writerows(rows)
    print("backfill: %d sinais adicionados" % len(rows))
    T = pd.DataFrame(rows, columns=COLS)
    if len(T): print(T.groupby("Metodo").size().to_string())


if __name__ == "__main__":
    main()
