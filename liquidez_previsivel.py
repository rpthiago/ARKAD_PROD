# -*- coding: utf-8 -*-
"""A conversao back->lay so e nao-enviesada com piso de lay_size >= R$200, mas o feed gratis
nao traz liquidez. Unica saida: a liquidez e previsivel por (liga, faixa de odd)?

Teste honesto: aprende no PRIMEIRO periodo quais celulas (liga x faixa) tem liquidez
confiavel, aplica no SEGUNDO, e mede o vies que sobra -- com bootstrap por bloco de dia.
"""
import sys
import numpy as np
import pandas as pd

COMISSAO, PISO = 0.05, 200.0
rng = np.random.default_rng(7)
be = lambda o: (o - 1.0) / (o - COMISSAO)

df = pd.read_csv(sys.argv[1])
for c in ["min_to_ko", "back", "back_size", "lay", "lay_size"]:
    df[c] = pd.to_numeric(df[c], errors="coerce")
df = df.dropna(subset=["back", "lay", "min_to_ko"])
df = df[(df["back"] > 1.01) & (df["lay"] > 1.01) & (df["lay"] < 1000) &
        (df["lay"] >= df["back"]) & (df["back_size"].fillna(0) > 0) & (df["lay_size"].fillna(0) > 0)]
df["jogo"] = df["competition"] + "|" + df["home"] + "|" + df["away"] + "|" + df["ko"]
df["dist"] = (df["min_to_ko"] - 10.0).abs()
df = df.sort_values("dist").groupby(["alvo", "jogo"], as_index=False).first()
df["data"] = pd.to_datetime(df["ko"], errors="coerce")
df = df.dropna(subset=["data"]); df["dia"] = df["data"].dt.date
df["razao"] = df["lay"] / df["back"]

d = df[df["alvo"] == "DRAW"].copy()
d["fx"] = pd.cut(d["back"], bins=[2.5, 3.5, 4.5, 6.0, 8.0, 12.0, 1e9], right=False)
corte = d["data"].quantile(.5)
tr, te = d[d["data"] <= corte].copy(), d[d["data"] > corte].copy()

# o que se aprende no treino: P(lay_size >= PISO) e a razao mediana, por celula
cel = tr.groupby(["competition", "fx"], observed=True).agg(
    n=("lay", "size"), p_liq=("lay_size", lambda s: (s >= PISO).mean()),
    razao=("razao", "median")).reset_index()
cel = cel[cel["n"] >= 20]
print("celulas (liga x faixa) com N>=20 no treino: %d" % len(cel))
print("distribuicao de P(lay_size>=R$200): p25 %.2f | mediana %.2f | p75 %.2f" % (
    cel["p_liq"].quantile(.25), cel["p_liq"].median(), cel["p_liq"].quantile(.75)))

def vies(sub, rotulo):
    if len(sub) < 100:
        print("  %-42s N=%d  (insuficiente)" % (rotulo, len(sub))); return
    e = ((be(sub["lay_prev"]) - be(sub["lay"])) * 100.0).values
    dias = sub["dia"].values; uniq = np.unique(dias)
    idx = {u: np.where(dias == u)[0] for u in uniq}
    am = np.array([e[np.concatenate([idx[u] for u in rng.choice(uniq, len(uniq), True)])].mean()
                   for _ in range(2000)])
    lo, hi = np.percentile(am, [2.5, 97.5])
    marca = "   <-- IC contem zero" if lo <= 0 <= hi else ""
    print("  %-42s N=%5d  vies %+.3f pp  IC95 [%+.3f, %+.3f]%s" % (
        rotulo, len(sub), e.mean(), lo, hi, marca))

mapa_razao = cel.set_index(["competition", "fx"])["razao"]
mapa_liq = cel.set_index(["competition", "fx"])["p_liq"]
chaves = list(zip(te["competition"], te["fx"]))
te["razao_prev"] = [mapa_razao.get(k, np.nan) for k in chaves]
te["p_liq_prev"] = [mapa_liq.get(k, np.nan) for k in chaves]
te = te.dropna(subset=["razao_prev"])
te["lay_prev"] = te["back"] * te["razao_prev"]

print("\n>>> VIES FORA DA AMOSTRA, usando SO o que o feed gratis te daria (liga + odd):")
vies(te, "todas as celulas conhecidas")
for lim in [0.5, 0.7, 0.8, 0.9]:
    vies(te[te["p_liq_prev"] >= lim], "so celulas com P(liq>=R$200) >= %.0f%%" % (100 * lim))
print("\n>>> referencia: com o lay_size REAL observado (que o feed nao tem):")
vies(te[te["lay_size"] >= PISO], "piso aplicado no lay_size real")

# quanto do universo sobra, e a liquidez prevista acerta?
for lim in [0.7, 0.8, 0.9]:
    s = te[te["p_liq_prev"] >= lim]
    if len(s):
        print("  P(liq)>=%.0f%%: sobra %.1f%% do universo | de fato liquidos: %.1f%%" % (
            100 * lim, 100 * len(s) / len(te), 100 * (s["lay_size"] >= PISO).mean()))
