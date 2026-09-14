# -*- coding: utf-8 -*-
"""
auditar_top3_cs_operacional.py — itens 4 e 5 da auditoria TOP 3: spread/liquidez reais no coletor e
estabilidade do ranking ao longo do dia. Entrada: cs_pre.csv (capturas pre-KO dos runners 2-2 e 0-3:
ts,ko,home,away,min_to_ko,runner,back,back_size,lay,lay_size) e o ledger do forward.
"""
import os, sys, re, unicodedata, warnings
warnings.filterwarnings("ignore"); sys.stdout.reconfigure(encoding="utf-8")
ROOT = os.path.dirname(os.path.abspath(__file__))
import pandas as pd, numpy as np
T = os.path.join(os.environ.get("TEMP", "."), "var")

d = pd.read_csv(os.path.join(T, "cs_pre.csv"), header=None, names=["ts", "ko", "home", "away", "mtk", "runner", "back", "back_size", "lay", "lay_size"])
for c in ("mtk", "back", "back_size", "lay", "lay_size"): d[c] = pd.to_numeric(d[c], errors="coerce")
d = d.dropna(subset=["mtk", "lay"]); d["dia"] = d.ko.str[:10]
d["spread_pct"] = (d.lay - d.back) / d.back * 100
print("capturas pre-KO: %d | jogos: %d | dias: %d (%s -> %s)" % (len(d), d[["ko", "home", "away"]].drop_duplicates().shape[0], d.dia.nunique(), d.dia.min(), d.dia.max()))

for runner, lo, hi, nome in (("2 - 2", 8, 20, "Lay 2x2"), ("0 - 3", 14, 35, "Lay 0x3")):
    r = d[(d.runner == runner) & (d.lay >= lo) & (d.lay <= hi)]
    print("\n" + "=" * 96); print("%s — runner '%s', lay na faixa %d-%d (capturas pre-KO)" % (nome, runner, lo, hi)); print("=" * 96)
    print("  %-14s %6s %9s %9s %9s %10s %10s %8s" % ("janela", "N", "spread50", "spread75", "spread90", "lay_size50", "lay_size25", "sem back"))
    for tag, a, b in (("KO (0-15min)", -5, 15), ("1h antes", 45, 75), ("3h antes", 150, 210), ("6h antes", 330, 400)):
        w = r[(r.mtk >= a) & (r.mtk <= b)]
        if len(w) == 0: continue
        sp = w.spread_pct.dropna()
        print("  %-14s %6d %8.1f%% %8.1f%% %8.1f%% %10.0f %10.0f %7.1f%%" % (tag, len(w), sp.median(), sp.quantile(.75), sp.quantile(.9), w.lay_size.median(), w.lay_size.quantile(.25), 100 * w.back.isna().mean()))
    # liquidez por faixa de odd, perto do KO
    w = r[(r.mtk >= -5) & (r.mtk <= 15)]
    print("  perto do KO, por faixa de odd de lay:")
    for a, b in ((lo, lo + 4), (lo + 4, lo + 8), (lo + 8, lo + 14), (lo + 14, hi + 1)):
        x = w[(w.lay >= a) & (w.lay < b)]
        if len(x) < 20: continue
        print("    odd %2d-%2d N=%5d spread50=%5.1f%% lay_size50=R$%5.0f p25=R$%4.0f | ate R$100 de stake casa? %.0f%%"
              % (a, b, len(x), x.spread_pct.median(), x.lay_size.median(), x.lay_size.quantile(.25), 100 * (x.lay_size >= 100).mean()))

    # ---- estabilidade do ranking: TOP3 por menor lay entre os jogos na faixa, "de manha" (>=3h antes) vs no KO
    print("  estabilidade do ranking TOP 3 (so pelo lay do runner, entre jogos na faixa):")
    jogos = r.groupby(["dia", "ko", "home", "away"])
    cedo = r[r.mtk >= 180].sort_values("mtk", ascending=False).groupby(["dia", "ko", "home", "away"]).first().reset_index()   # captura mais antiga
    tarde = r[(r.mtk >= -5) & (r.mtk <= 15)].sort_values("mtk").groupby(["dia", "ko", "home", "away"]).first().reset_index()  # mais perto do KO
    m = cedo.merge(tarde, on=["dia", "ko", "home", "away"], suffixes=("_cedo", "_ko"))
    m["dlay"] = m.lay_ko - m.lay_cedo
    print("    jogos com captura >=3h antes E no KO: %d | odd mediana cedo %.1f -> KO %.1f | |mudanca| mediana %.1f | sobe %.0f%% / desce %.0f%%"
          % (len(m), m.lay_cedo.median(), m.lay_ko.median(), m.dlay.abs().median(), 100 * (m.dlay > 0).mean(), 100 * (m.dlay < 0).mean()))
    manteve = []; saiu_faixa = 0
    for dia, g in m.groupby("dia"):
        if len(g) < 4: continue
        t_cedo = set(g.nsmallest(3, "lay_cedo").index); t_ko = set(g.nsmallest(3, "lay_ko").index)
        manteve.append(len(t_cedo & t_ko) / 3.0)
        saiu_faixa += int(((g.loc[list(t_cedo), "lay_ko"] < lo) | (g.loc[list(t_cedo), "lay_ko"] > hi)).sum())
    if manteve:
        print("    dias com >=4 jogos: %d | do TOP3 'da manha', quantos ainda estao no TOP3 no KO: media %.0f%% | dias com TOP3 identico: %.0f%%"
              % (len(manteve), 100 * np.mean(manteve), 100 * np.mean([x == 1.0 for x in manteve])))
        print("    jogos do TOP3 da manha cujo lay SAIU da faixa ate o KO: %d" % saiu_faixa)

# ---- odd do feed (o que o ledger gravou) vs odd real no coletor perto do KO
print("\n" + "=" * 96); print("ODD DO SINAL (feed diario, ledger) vs ODD REAL NO COLETOR (runner, perto do KO)"); print("=" * 96)
def canon(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower(); return re.sub(r"[^a-z0-9]", "", s)
L = pd.read_csv(os.path.join(ROOT, "metodos_aprovados", "forward_5metodos_ledger.csv"), encoding="utf-8-sig")
d["k"] = d.dia + "|" + d.home.map(canon) + "|" + d.away.map(canon)
for nome, runner in (("Lay 2x2 Top 3", "2 - 2"), ("Lay 0x3 Top 3", "0 - 3")):
    g = L[L.Metodo == nome].copy(); g["k"] = g.Data + "|" + g.Home.map(canon) + "|" + g.Away.map(canon)
    ko = d[(d.runner == runner) & (d.mtk >= -5) & (d.mtk <= 15)].sort_values("mtk").groupby("k").first()
    cedo = d[(d.runner == runner) & (d.mtk >= 180)].sort_values("mtk", ascending=False).groupby("k").first()
    j = g.merge(ko[["lay", "lay_size", "spread_pct"]], left_on="k", right_index=True, how="inner").merge(cedo[["lay"]], left_on="k", right_index=True, how="left", suffixes=("_ko", "_cedo"))
    j["odd_feed"] = pd.to_numeric(j.Odd_Lay)
    print("  %-14s sinais casados no coletor: %d de %d" % (nome, len(j), len(g)))
    if len(j):
        print("     odd do feed mediana %.2f | coletor >=3h antes %.2f | coletor no KO %.2f | (KO - feed) mediana %+.2f | KO > feed em %.0f%% dos jogos"
              % (j.odd_feed.median(), j.lay_cedo.median(), j.lay_ko.median(), (j.lay_ko - j.odd_feed).median(), 100 * (j.lay_ko > j.odd_feed).mean()))
        print("     no KO: lay_size mediana R$%.0f | spread mediano %.1f%% | sinais cuja odd no KO saiu da faixa: %d" % (j.lay_size.median(), j.spread_pct.median(), int(((j.lay_ko < (8 if '2x2' in nome else 14)) | (j.lay_ko > (20 if '2x2' in nome else 35))).sum())))
