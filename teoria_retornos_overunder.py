# -*- coding: utf-8 -*-
"""
teoria_retornos_overunder.py — Teoria dos Retornos (Over/Under), versao Arkad portada.

Mesmo algoritmo do Match Odds, mas com 2 resultados (Over/Under) e cobrindo as 5 linhas
(0.5, 1.5, 2.5, 3.5, 4.5). Data-driven na base Bet365 atual (sem os 140 'if' nem os bugs
China/Colombia). Curva acumulada de backar Over/Under -> Hull ponderada + features tecnicas
-> normalizacao -> k-NN (L1) -> 3 vizinhos -> N1/N2/N3 (Over/Under).

Uso:  python teoria_retornos_overunder.py
Saida: Teoria_dos_Retornos/<hoje>_Teoria_dos_Retornos_OverUnder.csv (League, Line, N1..N3, consenso)

NOTA HONESTA: analise tecnica sobre curva de aposta + k-NN, SEM edge validado no crivo.
Sao palpites, nao sinal comprovado. Pra ficar do dia, atualize a base antes.
"""
import pandas as pd, numpy as np, math, os
from datetime import date

HERE = os.path.dirname(os.path.abspath(__file__))
BASE = os.path.join(HERE, "Bases_de_Dados_API_FutPythonTrader_Bet365.csv")
OUT = os.path.join(HERE, "Teoria_dos_Retornos")
MIN_DIAS = 60

# linha -> (piso de gols p/ Over, coluna Over, coluna Under)
LINHAS = {
    "0.5": (0, "Odd_Over05_FT", "Odd_Under05_FT"),
    "1.5": (1, "Odd_Over15_FT", "Odd_Under15_FT"),
    "2.5": (2, "Odd_Over25_FT", "Odd_Under25_FT"),
    "3.5": (3, "Odd_Over35_FT", "Odd_Under35_FT"),
    "4.5": (4, "Odd_Over45_FT", "Odd_Under45_FT"),
}


def teoria_ou_liga(df, piso, col_over, col_under):
    df = df.sort_values("Date").reset_index(drop=True)
    tg = df["Goals_H_FT"] + df["Goals_A_FT"]
    over = tg > piso
    df["O"] = np.where(over, 100 * df[col_over] - 100, -100)
    df["U"] = np.where(~over, 100 * df[col_under] - 100, -100)
    g = df.groupby("Date").agg(O=("O", "sum"), U=("U", "sum")).reset_index()
    if len(g) < MIN_DIAS:
        return None
    d2 = g.tail(101).reset_index(drop=True)
    d2["Id"] = d2.index + 1
    for c in ["O", "U"]:
        d2[c + "acu"] = d2[c].cumsum()
        d2.loc[0, c + "acu"] = np.nan

    def wmean(col):
        def f(s):
            dd = d2.loc[s.index, col]; w = d2.loc[s.index, "Id"]
            return (dd * w).sum() / w.sum()
        return f

    for X in ["O", "U"]:
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

    cols = [X + suf for suf in ["hull", "dist", "r", "inc", "dp", "amp"] for X in ["O", "U"]]
    d3 = pd.DataFrame(index=d2.index)
    for c in cols:
        d3[c] = d2[c].iloc[23:].rolling(5).apply(normaliz, raw=False)

    R = pd.Series(index=d2.index, dtype=object)
    for i in range(len(d2)):
        if i > 26 and i + 1 < len(d2):
            o, u = d2.iloc[i + 1][["O", "U"]]
            R.iloc[i] = "Over" if o > u else "Under"
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
    need = ["Goals_H_FT", "Goals_A_FT", "League", "Date"] + [c for _, o, u in LINHAS.values() for c in (o, u)]
    d = d.dropna(subset=["Goals_H_FT", "Goals_A_FT", "League", "Date"])
    d["Date"] = pd.to_datetime(d["Date"], errors="coerce").dt.date
    print(f"base: {len(d)} jogos | ate {max(d['Date'])} | ligas: {d['League'].nunique()}", flush=True)

    linhas = []
    for nome, (piso, co, cu) in LINHAS.items():
        if co not in d.columns or cu not in d.columns:
            print(f"[linha {nome}] colunas ausentes na base — pulando"); continue
        n_ok = 0
        for lg, sub in d.groupby("League"):
            s = sub.dropna(subset=[co, cu])
            try:
                r = teoria_ou_liga(s, piso, co, cu)
            except Exception:
                r = None
            if r:
                linhas.append({"League": lg, "Line": nome, "N1": r[0], "N2": r[1], "N3": r[2]}); n_ok += 1
        print(f"  linha {nome}: {n_ok} ligas")
    out = pd.DataFrame(linhas)
    out["consenso"] = np.where((out["N1"] == out["N2"]) & (out["N2"] == out["N3"]), out["N1"], "")
    out = out.sort_values(["Line", "League"]).reset_index(drop=True)

    os.makedirs(OUT, exist_ok=True)
    hoje = date.today().strftime("%Y-%m-%d")
    path = os.path.join(OUT, f"{hoje}_Teoria_dos_Retornos_OverUnder.csv")
    out.to_csv(path, index=False)
    print(f"\n=== {len(out)} palpites (5 linhas) | consenso forte: {int((out['consenso'] != '').sum())} ===")
    print(f"salvo: {path}")


if __name__ == "__main__":
    main()
