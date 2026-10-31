# -*- coding: utf-8 -*-
"""Calibra back->lay no acervo Betfair, com teste FORA DA AMOSTRA no tempo.

A pergunta nao e "qual o spread medio" (isso seria ajuste). E: se eu observar so o preco de
BACK da exchange (o que o football-data publica de graca), com quanto erro eu consigo prever o
preco de LAY -- e quantos pontos percentuais de edge esse erro custa na decisao?

Uso: python calibra_back_lay.py <back_lay_pregame.csv>
"""
import sys
import numpy as np
import pandas as pd

COMISSAO = 0.05
ENTRADA = sys.argv[1]

df = pd.read_csv(ENTRADA)
print("linhas extraidas: %d" % len(df))

# ---------------------------------------------------------------- limpeza
for c in ["min_to_ko", "back", "back_size", "lay", "lay_size"]:
    df[c] = pd.to_numeric(df[c], errors="coerce")

antes = len(df)
df = df.dropna(subset=["back", "lay", "min_to_ko"])
print("  com os dois lados cotados: %d  (-%d)" % (len(df), antes - len(df)))

# guarda contra livro degenerado/vazio (back ~1.01 em livro vazio ja contaminou analise antes)
cond = (df["back"] > 1.01) & (df["lay"] > 1.01) & (df["lay"] < 1000) & \
       (df["lay"] >= df["back"]) & (df["back_size"].fillna(0) > 0) & (df["lay_size"].fillna(0) > 0)
print("  descartadas por livro degenerado: %d" % int((~cond).sum()))
df = df[cond].copy()

# ---------------------------------------------------------------- 1 captura por jogo, perto do KO-10
df["jogo"] = df["competition"] + "|" + df["home"] + "|" + df["away"] + "|" + df["ko"]
df["dist"] = (df["min_to_ko"] - 10.0).abs()
df = df.sort_values("dist").groupby(["alvo", "jogo"], as_index=False).first()
print("  1 captura por jogo (mais proxima do KO-10): %d" % len(df))

df["data"] = pd.to_datetime(df["ko"], errors="coerce")
df = df.dropna(subset=["data"])

df["razao"] = df["lay"] / df["back"]
df["sp_prob"] = 1.0 / df["back"] - 1.0 / df["lay"]


def be_wr(odd):
    """WR de equilibrio de um lay: (odd-1)/(odd-c)."""
    return (odd - 1.0) / (odd - COMISSAO)


for alvo, nome in [("DRAW", "MATCH_ODDS / The Draw"), ("CS00", "CORRECT_SCORE / 0 - 0")]:
    d = df[df["alvo"] == alvo].copy()
    if len(d) < 100:
        print("\n### %s: N=%d, insuficiente" % (nome, len(d)))
        continue
    print("\n" + "=" * 78)
    print("### %s   N=%d   (%s a %s)" % (nome, len(d), d["data"].min().date(), d["data"].max().date()))
    print("=" * 78)
    print("  razao lay/back: mediana %.4f | p25 %.4f | p75 %.4f | p90 %.4f" % (
        d["razao"].median(), d["razao"].quantile(.25), d["razao"].quantile(.75), d["razao"].quantile(.90)))

    # ---- spread em funcao da LIQUIDEZ (o filtro que o projeto provou ter mecanismo)
    print("\n  spread por liquidez do lado LAY (lay_size):")
    print("    %-16s %7s %10s %10s %10s" % ("faixa R$", "N", "razao med", "p90", "sp_prob med"))
    bins = [0, 50, 100, 200, 500, 1e12]
    rots = ["< 50", "50-100", "100-200", "200-500", ">= 500"]
    d["fx_liq"] = pd.cut(d["lay_size"], bins=bins, labels=rots, right=False)
    for r in rots:
        s = d[d["fx_liq"] == r]
        if len(s) < 30:
            print("    %-16s %7d   (N<30, nao reportado)" % (r, len(s)))
            continue
        print("    %-16s %7d %10.4f %10.4f %10.5f" % (
            r, len(s), s["razao"].median(), s["razao"].quantile(.90), s["sp_prob"].median()))

    # ---- spread por faixa de odd de BACK
    if alvo == "DRAW":
        obins = [2.5, 3.5, 4.5, 6.0, 8.0, 12.0, 20.0, 1e9]
    else:
        obins = [5.0, 8.0, 11.0, 15.0, 20.0, 30.0, 1e9]
    orots = ["%g-%g" % (obins[i], obins[i + 1]) if obins[i + 1] < 1e8 else "%g+" % obins[i]
             for i in range(len(obins) - 1)]
    d["fx_odd"] = pd.cut(d["back"], bins=obins, labels=orots, right=False)
    print("\n  spread por faixa de odd de BACK:")
    print("    %-12s %7s %10s %10s" % ("faixa", "N", "razao med", "p90"))
    for r in orots:
        s = d[d["fx_odd"] == r]
        if len(s) < 30:
            continue
        print("    %-12s %7d %10.4f %10.4f" % (r, len(s), s["razao"].median(), s["razao"].quantile(.90)))

    # ---- TESTE FORA DA AMOSTRA no tempo
    corte = d["data"].quantile(.5)
    tr = d[d["data"] <= corte]
    te = d[d["data"] > corte].copy()
    print("\n  >>> TESTE FORA DA AMOSTRA  (treino <= %s: N=%d | teste: N=%d)" % (
        corte.date(), len(tr), len(te)))
    if len(te) < 100 or len(tr) < 100:
        print("      amostra insuficiente para split")
        continue

    # calibra: mediana da razao por faixa de odd, aprendida SO no treino
    mapa = tr.groupby("fx_odd", observed=True)["razao"].median()
    global_med = tr["razao"].median()
    te["razao_prev"] = te["fx_odd"].map(mapa).astype(float).fillna(global_med)
    te["lay_prev"] = te["back"] * te["razao_prev"]

    err = te["lay_prev"] - te["lay"]
    err_rel = err / te["lay"]
    print("      erro do lay previsto: mediana %+.3f | MAE %.3f | p90 |erro| %.3f" % (
        err.median(), err.abs().mean(), err.abs().quantile(.90)))
    print("      erro relativo:        mediana %+.2f%% | p90 |erro| %.2f%%" % (
        100 * err_rel.median(), 100 * err_rel.abs().quantile(.90)))

    # ---- o que realmente importa: erro em PONTOS PERCENTUAIS de WR de equilibrio
    d_be = (be_wr(te["lay_prev"]) - be_wr(te["lay"])) * 100.0
    print("      *** custo na DECISAO (erro em pp do WR de equilibrio):")
    print("          mediana %+.2f pp | MAE %.2f pp | p90 |erro| %.2f pp | p99 %.2f pp" % (
        d_be.median(), d_be.abs().mean(), d_be.abs().quantile(.90), d_be.abs().quantile(.99)))

    # com piso de liquidez, que e onde o projeto opera
    te2 = te[te["lay_size"] >= 200]
    if len(te2) >= 100:
        d_be2 = (be_wr(te2["lay_prev"]) - be_wr(te2["lay"])) * 100.0
        print("          com piso lay_size>=R$200 (N=%d): MAE %.2f pp | p90 %.2f pp" % (
            len(te2), d_be2.abs().mean(), d_be2.abs().quantile(.90)))

    # ---- viés: usar BACK como se fosse LAY (o erro ja catalogado no projeto)
    d_be_cru = (be_wr(te["back"]) - be_wr(te["lay"])) * 100.0
    print("      para comparar - usar o BACK direto como se fosse lay:")
    print("          erro mediano %+.2f pp | MAE %.2f pp   <-- o erro que a calibracao evita" % (
        d_be_cru.median(), d_be_cru.abs().mean()))
