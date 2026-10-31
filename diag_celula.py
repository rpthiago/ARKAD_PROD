# -*- coding: utf-8 -*-
"""A celula liga x faixa nao tem N. Antes de engrossar, TESTAR se a liga importa.
Se a razao lay/back nao depender da liga, agrupar por faixa so e justificado -- e aí mede-se
o vies fora da amostra da versao engrossada. Se depender, a rota esta fechada."""
import re, sys, unicodedata
import numpy as np
import pandas as pd

FAV_MAX, TETO, PISO = 1.40, 7.00, 200.0
BANDAS = [2.5, 3.5, 4.5, 6.0, 8.0, 12.0, 20.0, 1e9]
COM = 0.05
rng = np.random.default_rng(11)
be = lambda o: (o - 1.0) / (o - COM)

d = pd.read_csv(sys.argv[1])
for c in ["min_to_ko", "back", "back_size", "lay", "lay_size"]:
    d[c] = pd.to_numeric(d[c], errors="coerce")
d["jogo"] = d["competition"] + "|" + d["home"] + "|" + d["away"] + "|" + d["ko"]
d["dist"] = (d["min_to_ko"] - 10.0).abs()
d = d.sort_values("dist").groupby(["jogo", "lado"], as_index=False).first()
p = d.pivot_table(index=["jogo", "competition", "home", "away", "ko"], columns="lado",
                  values=["back", "lay", "lay_size"], aggfunc="first").reset_index()
p.columns = ["_".join([str(x) for x in c if x != ""]) for c in p.columns]
p = p.dropna(subset=["back_H", "back_A", "back_D", "lay_D", "lay_size_D"])
p["fav"] = p[["back_H", "back_A"]].min(axis=1)
p = p[(p["back_D"] > 1.01) & (p["lay_D"] >= p["back_D"]) & (p["lay_D"] < 1000)]
r = p[p["fav"] <= FAV_MAX].copy()
r["razao"] = r["lay_D"] / r["back_D"]
r["data"] = pd.to_datetime(r["ko"], errors="coerce"); r = r.dropna(subset=["data"])
r["dia"] = r["data"].dt.date
rot = ["%g-%g" % (BANDAS[i], BANDAS[i+1]) if BANDAS[i+1] < 1e8 else "%g+" % BANDAS[i]
       for i in range(len(BANDAS)-1)]
r["faixa"] = pd.cut(r["back_D"], bins=BANDAS, labels=rot, right=False)
print("universo da regra: N=%d (%s a %s)" % (len(r), r["data"].min().date(), r["data"].max().date()))

print("\n### 1. razao por FAIXA (sem liga), no universo da regra")
print("  %-10s %6s %9s %9s %9s %8s" % ("faixa", "N", "razao med", "p25", "p75", "P(liq)"))
for f in rot:
    s = r[r["faixa"] == f]
    if len(s) < 20: continue
    print("  %-10s %6d %9.4f %9.4f %9.4f %7.0f%%" % (
        f, len(s), s["razao"].median(), s["razao"].quantile(.25), s["razao"].quantile(.75),
        100*(s["lay_size_D"] >= PISO).mean()))

print("\n### 2. a LIGA importa? razao por liga dentro da faixa dominante")
dom = r["faixa"].value_counts().idxmax()
sd = r[r["faixa"] == dom]
print("  faixa dominante: %s (N=%d)" % (dom, len(sd)))
g = sd.groupby("competition")["razao"].agg(["size", "median"])
g = g[g["size"] >= 15].sort_values("median")
if len(g) >= 3:
    print("  %-30s %5s %9s" % ("competicao", "N", "razao med"))
    for comp, row in g.iterrows():
        print("  %-30s %5d %9.4f" % (str(comp)[:30], row["size"], row["median"]))
    # teste de Kruskal-Wallis: a razao difere entre ligas?
    from scipy.stats import kruskal
    grupos = [sd[sd["competition"] == c]["razao"].values for c in g.index]
    H, pv = kruskal(*grupos)
    print("  Kruskal-Wallis entre %d ligas: H=%.2f, P=%.4f  ->  %s" % (
        len(grupos), H, pv,
        "liga NAO importa (agrupar por faixa e justificado)" if pv > 0.05
        else "liga IMPORTA (agrupar por faixa mistura regimes)"))
else:
    print("  ligas com N>=15 insuficientes para testar (%d)" % len(g))

print("\n### 3. vies FORA DA AMOSTRA da versao engrossada (faixa + piso de liquidez)")
corte = r["data"].quantile(.5)
tr, te = r[r["data"] <= corte], r[r["data"] > corte].copy()
mapa = tr.groupby("faixa", observed=True)["razao"].median()
pliq = tr.groupby("faixa", observed=True)["lay_size_D"].apply(lambda s: float((s >= PISO).mean()))
te["razao_prev"] = te["faixa"].map(mapa).astype(float)
te["p_liq_prev"] = te["faixa"].map(pliq).astype(float)
te = te.dropna(subset=["razao_prev"])
te["lay_prev"] = te["back_D"] * te["razao_prev"]
print("  treino N=%d (ate %s) | teste N=%d" % (len(tr), corte.date(), len(te)))

def vies(sub, rot_):
    if len(sub) < 60:
        print("  %-44s N=%4d  (N<60, nao reportado)" % (rot_, len(sub))); return
    e = ((be(sub["lay_prev"]) - be(sub["lay_D"])) * 100.0).values
    dias = sub["dia"].values; u = np.unique(dias)
    idx = {x: np.where(dias == x)[0] for x in u}
    am = np.array([e[np.concatenate([idx[x] for x in rng.choice(u, len(u), True)])].mean()
                   for _ in range(2000)])
    lo, hi = np.percentile(am, [2.5, 97.5])
    print("  %-44s N=%4d  vies %+.3f pp  IC95 [%+.3f, %+.3f]%s" % (
        rot_, len(sub), e.mean(), lo, hi, "   <-- contem zero" if lo <= 0 <= hi else "   *** EXCLUI zero"))

vies(te, "faixa so, sem screen de liquidez")
vies(te[te["p_liq_prev"] >= 0.70], "faixa com P(liq)>=70% aprendido no treino")
vies(te[te["lay_size_D"] >= PISO], "referencia: lay_size real >= R$200")
vies(te[te["lay_D"] <= TETO], "com o teto operacional lay <= 7.00")
vies(te[(te["lay_D"] <= TETO) & (te["lay_size_D"] >= PISO)], "teto 7.00 + lay_size real >= R$200")

print("\n### 4. FLUXO real da regra (sem exigir celula)")
for rotf, sub in [("todo o universo Fav<=1.40", r),
                  ("+ teto lay <= 7.00", r[r["lay_D"] <= TETO]),
                  ("+ teto e lay_size >= R$200", r[(r["lay_D"] <= TETO) & (r["lay_size_D"] >= PISO)])]:
    dias_tot = (r["data"].max() - r["data"].min()).days or 1
    print("  %-32s N=%5d  -> %5.1f sinais/mes  | N=250 em %4.1f meses" % (
        rotf, len(sub), 30.0*len(sub)/dias_tot, 250.0/max(.1, 30.0*len(sub)/dias_tot)))
