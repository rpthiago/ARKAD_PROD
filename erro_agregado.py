# -*- coding: utf-8 -*-
"""Duas perguntas que decidem se a calibracao serve:

(1) O erro por aposta e ~0,6 pp, mas e ENVIESADO? Se a mediana e ~0 e os erros sao
    independentes, ele some na media -- o que importa para medir edge agregado.
    Mede com bootstrap por BLOCO DE DIA (jogos do mesmo dia sao correlacionados),
    que e a metodologia do proprio projeto.
(2) O bfe_d gratis do football-data realmente bate com o preco de BACK do acervo?
    Se nao bater, todo o plano cai.
"""
import sys
import numpy as np
import pandas as pd
import re, unicodedata

COMISSAO = 0.05
ENT, FD = sys.argv[1], sys.argv[2]
rng = np.random.default_rng(42)


def be_wr(o):
    return (o - 1.0) / (o - COMISSAO)


df = pd.read_csv(ENT)
for c in ["min_to_ko", "back", "back_size", "lay", "lay_size"]:
    df[c] = pd.to_numeric(df[c], errors="coerce")
df = df.dropna(subset=["back", "lay", "min_to_ko"])
df = df[(df["back"] > 1.01) & (df["lay"] > 1.01) & (df["lay"] < 1000) &
        (df["lay"] >= df["back"]) & (df["back_size"].fillna(0) > 0) & (df["lay_size"].fillna(0) > 0)]
df["jogo"] = df["competition"] + "|" + df["home"] + "|" + df["away"] + "|" + df["ko"]
df["dist"] = (df["min_to_ko"] - 10.0).abs()
df = df.sort_values("dist").groupby(["alvo", "jogo"], as_index=False).first()
df["data"] = pd.to_datetime(df["ko"], errors="coerce")
df = df.dropna(subset=["data"])
df["dia"] = df["data"].dt.date
df["razao"] = df["lay"] / df["back"]

print("=" * 78)
print("(1) ERRO AGREGADO - bootstrap por bloco de dia, 4000 reamostragens")
print("=" * 78)
for alvo, nome in [("DRAW", "MATCH_ODDS / The Draw"), ("CS00", "CORRECT_SCORE / 0 - 0")]:
    d = df[df["alvo"] == alvo].copy()
    obins = [2.5, 3.5, 4.5, 6.0, 8.0, 12.0, 20.0, 1e9] if alvo == "DRAW" else \
            [5.0, 8.0, 11.0, 15.0, 20.0, 30.0, 1e9]
    d["fx"] = pd.cut(d["back"], bins=obins, right=False)
    corte = d["data"].quantile(.5)
    tr, te = d[d["data"] <= corte], d[d["data"] > corte].copy()
    mapa = tr.groupby("fx", observed=True)["razao"].median()
    te["lay_prev"] = te["back"] * te["fx"].map(mapa).astype(float).fillna(tr["razao"].median())
    for piso, rot in [(0, "sem piso"), (200, "lay_size >= R$200")]:
        t = te[te["lay_size"] >= piso]
        if len(t) < 100:
            continue
        erro = (be_wr(t["lay_prev"]) - be_wr(t["lay"])) * 100.0
        dias = t["dia"].values
        uniq = np.unique(dias)
        idx = {u: np.where(dias == u)[0] for u in uniq}
        ev = erro.values
        amostras = np.empty(4000)
        for b in range(4000):
            esc = rng.choice(uniq, size=len(uniq), replace=True)
            amostras[b] = np.concatenate([idx[u] for u in esc]).size and \
                ev[np.concatenate([idx[u] for u in esc])].mean()
        lo, hi = np.percentile(amostras, [2.5, 97.5])
        print("  %-22s %-18s N=%5d  vies medio %+.3f pp  IC95 [%+.3f, %+.3f]" % (
            nome if piso == 0 else "", rot, len(t), ev.mean(), lo, hi))
print("""
  Leitura: se o IC95 do vies contem zero e e estreito, a conversao NAO desloca a
  estimativa de edge agregada -- so adiciona variancia, que cai com N.""")

print("\n" + "=" * 78)
print("(2) O bfe_d GRATIS bate com o BACK do acervo?")
print("=" * 78)


def norm(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode()
    s = re.sub(r"[^a-z0-9 ]", " ", s.lower())
    for w in ["fc", "cf", "sc", "ac", "afc", "cd", "ca", "club", "de", "do", "da"]:
        s = re.sub(r"\b%s\b" % w, " ", s)
    return re.sub(r"\s+", " ", s).strip()


fd = pd.read_csv(FD, low_memory=False)
fd = fd[["liga", "Date", "Home", "Away", "bfe_h", "bfe_d", "bfe_a"]].copy()
fd["bfe_d"] = pd.to_numeric(fd["bfe_d"], errors="coerce")
fd = fd.dropna(subset=["bfe_d"])
fd["dia"] = pd.to_datetime(fd["Date"], errors="coerce").dt.date
fd["k"] = fd["dia"].astype(str) + "|" + fd["Home"].map(norm) + "|" + fd["Away"].map(norm)

ac = df[df["alvo"] == "DRAW"].copy()
ac["k"] = ac["dia"].astype(str) + "|" + ac["home"].map(norm) + "|" + ac["away"].map(norm)

print("  acervo DRAW: %d jogos | football_data com bfe_d: %d jogos" % (len(ac), len(fd)))
j = ac.merge(fd[["k", "bfe_d", "liga"]].drop_duplicates("k"), on="k", how="inner")
print("  casaram por data+times normalizados: %d (%.1f%% do acervo)" % (
    len(j), 100 * len(j) / max(1, len(ac))))
if len(j) >= 50:
    j["dif"] = j["bfe_d"] - j["back"]
    j["dif_rel"] = j["dif"] / j["back"]
    print("  bfe_d vs back do acervo (KO-10):")
    print("    diferenca mediana %+.3f | MAE %.3f | p90 |dif| %.3f" % (
        j["dif"].median(), j["dif"].abs().mean(), j["dif"].abs().quantile(.90)))
    print("    relativa: mediana %+.2f%% | p90 |dif| %.2f%%" % (
        100 * j["dif_rel"].median(), 100 * j["dif_rel"].abs().quantile(.90)))
    print("    correlacao: %.4f" % j[["bfe_d", "back"]].corr().iloc[0, 1])
    dbe = (be_wr(j["bfe_d"] * 1.05) - be_wr(j["lay"])) * 100.0
    print("    usando bfe_d * razao_mediana como lay -> erro %+.2f pp mediano, MAE %.2f pp" % (
        dbe.median(), dbe.abs().mean()))
    print("  ligas que casaram: %d  ->  %s" % (
        j["liga"].nunique(), ", ".join(sorted(j["liga"].dropna().unique())[:8])))
else:
    print("  *** casamento insuficiente: o bfe_d nao cobre as ligas do acervo ***")
    print("  ligas do acervo (amostra): %s" % ", ".join(sorted(ac["competition"].unique())[:10]))
    print("  ligas do football_data:   %s" % ", ".join(sorted(fd["liga"].dropna().unique())[:10]))
