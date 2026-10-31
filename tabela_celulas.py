# -*- coding: utf-8 -*-
"""Monta a TABELA DE CELULAS congelada do pre-registro do Lay Draw.

Universo da regra: Fav = min(Odd_H, Odd_A) <= 1.40, captura mais proxima do KO-10.
Chave da celula = (liga do football-data) x (faixa de odd de BACK do empate), porque a liga do
football-data e o que sera observavel para frente. Faixas = as MESMAS ja publicadas em
CALIBRACAO_BACK_LAY_claude.md (fixadas antes de olhar este subconjunto).
"""
import re, sys, unicodedata, hashlib
import numpy as np
import pandas as pd

FAV_MAX, TETO_LAY, PISO_LIQ, P_LIQ_MIN, N_MIN = 1.40, 7.00, 200.0, 0.70, 20
BANDAS = [2.5, 3.5, 4.5, 6.0, 8.0, 12.0, 20.0, 1e9]
MO3, FD = sys.argv[1], sys.argv[2]

d = pd.read_csv(MO3)
for c in ["min_to_ko", "back", "back_size", "lay", "lay_size"]:
    d[c] = pd.to_numeric(d[c], errors="coerce")
d["jogo"] = d["competition"] + "|" + d["home"] + "|" + d["away"] + "|" + d["ko"]
d["dist"] = (d["min_to_ko"] - 10.0).abs()
# por jogo x lado, a captura mais proxima do KO-10
d = d.sort_values("dist").groupby(["jogo", "lado"], as_index=False).first()

piv = d.pivot_table(index=["jogo", "competition", "home", "away", "ko"], columns="lado",
                    values=["back", "lay", "lay_size"], aggfunc="first").reset_index()
piv.columns = ["_".join([str(x) for x in c if x != ""]) for c in piv.columns]
need = ["back_H", "back_A", "back_D", "lay_D", "lay_size_D"]
piv = piv.dropna(subset=need)
print("jogos com os 3 runners cotados no KO-10: %d" % len(piv))

piv["fav"] = piv[["back_H", "back_A"]].min(axis=1)
piv = piv[(piv["back_D"] > 1.01) & (piv["lay_D"] >= piv["back_D"]) & (piv["lay_D"] < 1000)]
reg = piv[piv["fav"] <= FAV_MAX].copy()
print("universo da regra (Fav <= %.2f): %d jogos" % (FAV_MAX, len(reg)))
reg["razao"] = reg["lay_D"] / reg["back_D"]
reg["data"] = pd.to_datetime(reg["ko"], errors="coerce")
reg = reg.dropna(subset=["data"])


def norm(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode()
    s = re.sub(r"[^a-z0-9 ]", " ", s.lower())
    for w in ["fc", "cf", "sc", "ac", "afc", "cd", "ca", "club", "de", "do", "da"]:
        s = re.sub(r"\b%s\b" % w, " ", s)
    return re.sub(r"\s+", " ", s).strip()


# ---- crosswalk competicao do acervo -> liga do football-data, pelo casamento real de jogos
fd = pd.read_csv(FD, low_memory=False)
fd["bfe_d"] = pd.to_numeric(fd["bfe_d"], errors="coerce")
fd["dia"] = pd.to_datetime(fd["Date"], errors="coerce").dt.date
fd["k"] = fd["dia"].astype(str) + "|" + fd["Home"].map(norm) + "|" + fd["Away"].map(norm)
piv["dia"] = pd.to_datetime(piv["ko"], errors="coerce").dt.date
piv["k"] = piv["dia"].astype(str) + "|" + piv["home"].map(norm) + "|" + piv["away"].map(norm)
cj = piv.merge(fd[["k", "liga"]].dropna().drop_duplicates("k"), on="k", how="inner")
cw = (cj.groupby("competition")["liga"].agg(lambda s: s.value_counts().idxmax()))
nj = cj.groupby("competition")["liga"].size()
cw = cw[nj >= 3]   # exige >=3 jogos casados para aceitar o mapeamento
print("crosswalk aceito (>=3 jogos casados): %d competicoes -> %d ligas" % (
    len(cw), cw.nunique()))

reg["liga_fd"] = reg["competition"].map(cw)
cob = reg["liga_fd"].notna()
print("jogos da regra em ligas mapeadas: %d de %d (%.1f%%)" % (
    cob.sum(), len(reg), 100 * cob.mean()))
reg = reg[cob].copy()

rot = []
for i in range(len(BANDAS) - 1):
    rot.append("%g-%g" % (BANDAS[i], BANDAS[i + 1]) if BANDAS[i + 1] < 1e8 else "%g+" % BANDAS[i])
reg["faixa"] = pd.cut(reg["back_D"], bins=BANDAS, labels=rot, right=False)

cel = reg.groupby(["liga_fd", "faixa"], observed=True).agg(
    N=("lay_D", "size"),
    p_liq=("lay_size_D", lambda s: float((s >= PISO_LIQ).mean())),
    razao=("razao", "median"),
    lay_med=("lay_D", "median")).reset_index()
apr = cel[(cel["N"] >= N_MIN) & (cel["p_liq"] >= P_LIQ_MIN)].copy()
apr = apr.sort_values(["liga_fd", "faixa"])

print("\ncelulas candidatas: %d | APROVADAS (N>=%d e P(liq)>=%.0f%%): %d" % (
    len(cel), N_MIN, 100 * P_LIQ_MIN, len(apr)))
print("ligas aprovadas: %d" % apr["liga_fd"].nunique())

print("\n==================== TABELA DE CELULAS CONGELADA ====================")
print("%-26s %-8s %6s %7s %8s %8s" % ("liga (football-data)", "faixa", "N", "P(liq)", "razao", "lay med"))
for _, r in apr.iterrows():
    print("%-26s %-8s %6d %6.0f%% %8.4f %8.2f" % (
        str(r["liga_fd"])[:26], r["faixa"], r["N"], 100 * r["p_liq"], r["razao"], r["lay_med"]))

# hash do conteudo congelado
blob = "\n".join("%s|%s|%d|%.4f|%.4f" % (r["liga_fd"], r["faixa"], r["N"], r["p_liq"], r["razao"])
                 for _, r in apr.iterrows())
print("\nSHA256 da tabela: %s" % hashlib.sha256(blob.encode()).hexdigest())
apr.to_csv(sys.argv[3], index=False)

# ---- FLUXO: quantos sinais a regra daria por mes nas celulas aprovadas
ok = set(zip(apr["liga_fd"], apr["faixa"].astype(str)))
reg["cel_ok"] = [ (l, str(f)) in ok for l, f in zip(reg["liga_fd"], reg["faixa"]) ]
sin = reg[reg["cel_ok"]].copy()
sin["lay_est"] = sin["back_D"] * sin["faixa"].map(apr.set_index("faixa")["razao"].groupby(level=0).median()).astype(float)
sin = sin[sin["lay_D"] <= TETO_LAY]   # teto na odd de lay
print("\n==================== FLUXO HISTORICO ====================")
print("jogos da regra em celula aprovada E com lay <= %.2f: %d" % (TETO_LAY, len(sin)))
dias = (reg["data"].max() - reg["data"].min()).days or 1
print("janela do acervo: %s a %s (%d dias)" % (reg["data"].min().date(), reg["data"].max().date(), dias))
print("ritmo: %.1f sinais/mes  ->  N=250 em ~%.1f meses" % (
    30.0 * len(sin) / dias, 250.0 / max(0.1, 30.0 * len(sin) / dias)))
print("\npor mes:")
print(sin.groupby(sin["data"].dt.to_period("M")).size().to_string())
