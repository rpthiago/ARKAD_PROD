# -*- coding: utf-8 -*-
"""
teoria_retornos_matchodds.py — Teoria dos Retornos (Match Odds), versao Arkad portada.

Roda sobre a base Bet365 ATUAL (Bases_de_Dados_API_FutPythonTrader_Bet365.csv), data-driven
(pega as ligas da propria base — sem os 140 'if' hardcoded nem os bugs China/Colombia da
versao antiga). Replica fielmente o algoritmo: curva acumulada de backar H/D/A -> media
movel de Hull ponderada + features tecnicas (distancia, retorno, inclinacao, desvio,
amplitude) -> normalizacao -> k-NN por distancia L1 -> 3 vizinhos -> N1/N2/N3 (H/D/A).

Uso:  python teoria_retornos_matchodds.py
Saida: Teoria_dos_Retornos/<hoje>_Teoria_dos_Retornos_Match_Odds.csv  (League, N1, N2, N3)

NOTA HONESTA: e um metodo de analise tecnica sobre curva de aposta + k-NN, SEM edge validado
no crivo (walk-forward + odd real + FDR). Sao palpites, nao sinal comprovado. Pra ficar do dia
de hoje, atualize a base antes (o pull diario da API).
"""
import pandas as pd, numpy as np, math, os
from datetime import date

HERE = os.path.dirname(os.path.abspath(__file__))
BASE = os.path.join(HERE, "Bases_de_Dados_API_FutPythonTrader_Bet365.csv")
OUT = os.path.join(HERE, "Teoria_dos_Retornos")
MIN_DIAS = 60   # precisa de historico (a versao original usava 101 dias-de-jogo)


def teoria_liga(df):
    """Roda a teoria dos retornos p/ uma liga. Retorna (N1,N2,N3) ou None se dados insuficientes."""
    df = df.sort_values("Date").reset_index(drop=True)
    oH, oD, oA = df["Odd_H_FT"], df["Odd_D_FT"], df["Odd_A_FT"]
    res = np.where(df["Goals_H_FT"] > df["Goals_A_FT"], "H",
          np.where(df["Goals_H_FT"] < df["Goals_A_FT"], "A", "D"))
    df["H"] = np.where(res == "H", 100 * oH - 100, -100)
    df["D"] = np.where(res == "D", 100 * oD - 100, -100)
    df["A"] = np.where(res == "A", 100 * oA - 100, -100)
    g = df.groupby("Date").agg(H=("H", "sum"), D=("D", "sum"), A=("A", "sum")).reset_index()
    if len(g) < MIN_DIAS:
        return None
    d2 = g.tail(101).reset_index(drop=True)
    d2["Id"] = d2.index + 1
    for c in ["H", "D", "A"]:
        d2[c + "acu"] = d2[c].cumsum()
        d2.loc[0, c + "acu"] = np.nan

    def wmean(col):
        def f(s):
            dd = d2.loc[s.index, col]; w = d2.loc[s.index, "Id"]
            return (dd * w).sum() / w.sum()
        return f

    for X in ["H", "D", "A"]:
        d2["wa16"] = d2.rolling(16)[X + "acu"].apply(wmean(X + "acu"), raw=False)
        d2["wa8"] = d2.rolling(8)[X + "acu"].apply(wmean(X + "acu"), raw=False)
        d2[X + "C"] = 2 * d2["wa8"] - d2["wa16"]
        d2[X + "hull"] = d2.rolling(4)[X + "C"].apply(wmean(X + "C"), raw=False)
        d2[X + "dist"] = d2[X + "acu"] / d2[X + "hull"]
        d2[X + "r"] = d2[X + "hull"].rolling(2).apply(
            lambda s: (s.iloc[1] - s.iloc[0]) / abs(s.iloc[0]) if s.iloc[0] != 0 else np.nan, raw=False)

        def inc(s, XX=X):
            x = d2.loc[s.index, "Id"].values; y = d2.loc[s.index, XX + "hull"].values
            if np.isnan(y).any():
                return np.nan
            return np.polyfit(x, y, 1)[0]

        d2[X + "inc"] = d2[X + "hull"].rolling(5).apply(inc, raw=False)
        d2[X + "dp"] = d2[X + "acu"].rolling(10).std()
        d2[X + "amp"] = d2[X + "acu"].rolling(10).max() / d2[X + "acu"].rolling(10).min()

    def normaliz(s):
        val = s.iloc[4]; rng = s.max() - s.min()
        if rng == 0 or math.isnan(rng):
            return 0
        n = (val - s.min()) / rng
        return 0 if math.isnan(n) else n

    cols = [X + suf for suf in ["hull", "dist", "r", "inc", "dp", "amp"] for X in ["H", "D", "A"]]
    d3 = pd.DataFrame(index=d2.index)
    for c in cols:
        d3[c] = d2[c].iloc[23:].rolling(5).apply(normaliz, raw=False)

    R = pd.Series(index=d2.index, dtype=object)   # alvo = resultado lider do proximo dia
    for i in range(len(d2)):
        if i > 26 and i + 1 < len(d2):
            h, dd, a = d2.iloc[i + 1][["H", "D", "A"]]
            R.iloc[i] = "H" if (h > dd and h > a) else ("D" if (dd > h and dd > a) else "A")
    d3["R"] = R

    sel = d3.iloc[-1]
    d3 = d3.dropna(subset=cols)
    if len(d3) < 4:
        return None
    d3["eucli"] = d3.apply(lambda row: sum(abs(row[k] - sel[k]) for k in cols), axis=1)
    d4 = d3[d3["eucli"] > 1e-9].sort_values("eucli")
    d4 = d4[d4["R"].notna()]
    if len(d4) < 3:
        return None
    return d4["R"].iloc[0], d4["R"].iloc[1], d4["R"].iloc[2]


def main():
    print("Carregando base...", flush=True)
    d = pd.read_csv(BASE, low_memory=False)
    d = d.dropna(subset=["Odd_H_FT", "Odd_D_FT", "Odd_A_FT", "Goals_H_FT", "Goals_A_FT", "League", "Date"])
    d["Date"] = pd.to_datetime(d["Date"], errors="coerce").dt.date
    print(f"base: {len(d)} jogos | ate {max(d['Date'])} | ligas: {d['League'].nunique()}", flush=True)

    linhas = []
    for lg, sub in d.groupby("League"):
        try:
            r = teoria_liga(sub)
        except Exception:
            r = None
        if r:
            linhas.append({"League": lg, "N1": r[0], "N2": r[1], "N3": r[2]})
    out = pd.DataFrame(linhas).sort_values("League").reset_index(drop=True)
    out["consenso"] = np.where((out["N1"] == out["N2"]) & (out["N2"] == out["N3"]), out["N1"], "")

    os.makedirs(OUT, exist_ok=True)
    hoje = date.today().strftime("%Y-%m-%d")
    path = os.path.join(OUT, f"{hoje}_Teoria_dos_Retornos_Match_Odds.csv")
    out.to_csv(path, index=False)
    print(f"\n=== PALPITES: {len(out)} ligas | consenso forte (N1=N2=N3): {int((out['consenso'] != '').sum())} ===")
    print(out.to_string(index=False))
    print(f"\nsalvo: {path}")


if __name__ == "__main__":
    main()
