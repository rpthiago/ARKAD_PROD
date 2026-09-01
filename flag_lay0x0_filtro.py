# -*- coding: utf-8 -*-
"""
flag_lay0x0_filtro.py — marca cada Lay 0x0 / Lay Under 0.5 do paper como DENTRO/FORA do filtro.
Filtro REVISADO (estudo 08/2026): favoritismo forte e o que manda; a vantagem de mando importa ->
    DENTRO  <=>  favorito MANDANTE com Odd_H <= 1.50  OU  favorito VISITANTE com Odd_A <= 1.40
    (liga_rate deixou de ser gate — vira redundante com favorito forte; fica so como coluna informativa)
Combinado: ROI +3.33% (2026, IC95 [+2.3,+4.4], 8/8 meses; 25/25 meses positivos em 2 anos).

As odds do favorito (Odd_H_FT/Odd_A_FT) vem da base por (data, times canon). Jogos fora da base
(ex.: ultimos dias, coletor parado) ficam SEM_ODD ate ter a odd. Grava _fav_odd, _fav_lado,
_liga_rate_0x0 e _filtro_0x0 no placares_manuais.xlsx.

Uso:  python flag_lay0x0_filtro.py
"""
import os, sys, re, unicodedata, shutil
import numpy as np, pandas as pd
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass

ROOT = os.path.dirname(os.path.abspath(__file__))
PAPER = os.path.join(ROOT, "placares_manuais.xlsx")
BASE = os.path.join(ROOT, "Bases_de_Dados_API_FutPythonTrader_Bet365.csv")
FAM = {"Lay 0x0", "Lay Under 0.5 FT (Fav)"}      # mesma familia (lay do 0-0)
FAV_CASA_MAX, FAV_FORA_MAX = 1.50, 1.40
COMM = 0.045


def _cn(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)


def base_lookup():
    """(data, home_canon, away_canon) -> (Odd_H_FT, Odd_A_FT, liga_rate) a partir da base."""
    b = pd.read_csv(BASE, usecols=["Date", "League", "Home", "Away", "Odd_H_FT", "Odd_A_FT", "Goals_H_FT", "Goals_A_FT"])
    b["Date"] = pd.to_datetime(b["Date"], errors="coerce")
    # liga_rate 0-0 de toda a base
    g = b.dropna(subset=["Goals_H_FT", "Goals_A_FT"]).copy()
    g["z"] = ((g["Goals_H_FT"] == 0) & (g["Goals_A_FT"] == 0)).astype(int)
    lr = g.groupby("League")["z"].mean().to_dict()
    b = b[b["Date"] >= "2026-01-01"].dropna(subset=["Odd_H_FT", "Odd_A_FT"])
    odds = {}
    for _, r in b.iterrows():
        k = (r["Date"].strftime("%Y-%m-%d"), _cn(r["Home"]), _cn(r["Away"]))
        if k not in odds:
            odds[k] = (float(r["Odd_H_FT"]), float(r["Odd_A_FT"]))
    return odds, lr


def main():
    odds, lr = base_lookup()
    m = pd.read_excel(PAPER)
    fam = m["Metodo"].astype(str).isin(FAM)
    fav_odd = np.full(len(m), np.nan); fav_lado = np.array([""] * len(m), dtype=object)
    filtro = np.array([""] * len(m), dtype=object); lrate = np.full(len(m), np.nan)
    achou = 0
    for i in m.index[fam]:
        d = str(m.loc[i, "Data"])[:10]
        k = (d, _cn(m.loc[i, "Mandante"]), _cn(m.loc[i, "Visitante"]))
        lrate[i] = lr.get(str(m.loc[i, "Liga"]), np.nan)
        if k in odds:
            oh, oa = odds[k]; achou += 1
            fav_home = oh <= oa
            fav_odd[i] = min(oh, oa); fav_lado[i] = "casa" if fav_home else "fora"
            dentro = (oh <= FAV_CASA_MAX) if fav_home else (oa <= FAV_FORA_MAX)
            filtro[i] = "DENTRO" if dentro else "FORA"
        else:
            filtro[i] = "SEM_ODD"
    m["_fav_odd"] = fav_odd; m["_fav_lado"] = fav_lado
    m["_liga_rate_0x0"] = np.where(fam, lrate, np.nan); m["_filtro_0x0"] = np.where(fam, filtro, "")

    try:
        shutil.copy(PAPER, PAPER.replace(".xlsx", "_bak.xlsx"))
        m.to_excel(PAPER, index=False); destino = PAPER
    except PermissionError:
        destino = PAPER.replace(".xlsx", "_flag.xlsx"); m.to_excel(destino, index=False)
        print("[aviso] placares_manuais.xlsx aberto no Excel -> gravei em %s" % os.path.basename(destino))

    nfam = int(fam.sum())
    print("Gravado em %s | familia 0x0/Under0.5: %d linhas | odd casada na base: %d" % (os.path.basename(destino), nfam, achou))
    print("  DENTRO=%d  FORA=%d  SEM_ODD=%d (jogos fora da base / coletor parado)"
          % ((m["_filtro_0x0"] == "DENTRO").sum(), (m["_filtro_0x0"] == "FORA").sum(), (m["_filtro_0x0"] == "SEM_ODD").sum()))

    # resultado acumulado do recorte DENTRO (jogos com placar)
    z = m[fam & m["Gols_M"].notna() & m["Gols_V"].notna()].copy()
    z["gm"] = z["Gols_M"].astype(int); z["gv"] = z["Gols_V"].astype(int)
    z["green"] = ~((z["gm"] == 0) & (z["gv"] == 0))
    z["liab"] = z["Odd"] - 1; z["pnl"] = np.where(z["green"], 1 - COMM, -(z["Odd"] - 1))

    def R(df, t):
        if len(df) == 0: print("  %-16s -> 0" % t); return
        print("  %-16s | N=%-3d WR=%.1f%% | PnL=%+.1fu | ROI/liab=%+.1f%% | reds=%d"
              % (t, len(df), df["green"].mean() * 100, df["pnl"].sum(), df["pnl"].sum() / df["liab"].sum() * 100, (~df["green"]).sum()))
    print("\n=== acumulado (so jogos com placar E odd casada) ===")
    R(z, "TODOS")
    R(z[z["_filtro_0x0"] == "DENTRO"], "DENTRO filtro")
    R(z[z["_filtro_0x0"] == "FORA"], "FORA (descarte)")
    sem = int((z["_filtro_0x0"] == "SEM_ODD").sum())
    if sem: print("  (%d jogos com placar mas SEM odd na base — flag pendente ate o coletor/base ter a odd)" % sem)


if __name__ == "__main__":
    main()
