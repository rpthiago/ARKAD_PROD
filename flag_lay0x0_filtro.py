# -*- coding: utf-8 -*-
"""
flag_lay0x0_filtro.py — marca cada Lay 0x0 do paper como DENTRO/FORA do filtro congelado.
Regra congelada (pre-registrada 2026-08-02, ver memoria lay0x0-xgb-regra-congelada-forward):
    DENTRO  <=>  liga_rate < 0.08  E  mkt_prob < 0.09   (mkt_prob = 1/Odd)
liga_rate = taxa historica de 0-0 da liga (da base b365_base_lean). So faz sentido p/ 'Lay 0x0'.

Grava 3 colunas novas em placares_manuais.xlsx: _liga_rate_0x0, _mkt_prob_0x0, _filtro_0x0.
E imprime o resultado ACUMULADO so do recorte DENTRO (o que interessa acompanhar).

Uso:  python flag_lay0x0_filtro.py   (rode depois de preencher jogos novos)
"""
import os, sys, re, shutil
import numpy as np, pandas as pd
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass

ROOT = os.path.dirname(os.path.abspath(__file__))
PAPER = os.path.join(ROOT, "placares_manuais.xlsx")
BASE = os.path.join(ROOT, "b365_base_lean.csv")
LR_MAX, MP_MAX = 0.08, 0.09
COMM = 0.05
MIN_JOGOS_LIGA = 50


def liga_rates():
    b = pd.read_csv(BASE, usecols=["League", "Goals_H_FT", "Goals_A_FT"]).dropna(subset=["Goals_H_FT", "Goals_A_FT"])
    b["is00"] = ((b["Goals_H_FT"] == 0) & (b["Goals_A_FT"] == 0)).astype(int)
    g = b.groupby("League")["is00"].agg(["mean", "size"])
    return g[g["size"] >= MIN_JOGOS_LIGA]["mean"].to_dict()


def main():
    lr = liga_rates()
    m = pd.read_excel(PAPER)
    is0x0 = m["Metodo"].astype(str).str.strip() == "Lay 0x0"
    m["_liga_rate_0x0"] = np.where(is0x0, m["Liga"].map(lr), np.nan)
    m["_mkt_prob_0x0"] = np.where(is0x0, 1.0 / m["Odd"], np.nan)
    dentro = is0x0 & (m["_liga_rate_0x0"] < LR_MAX) & (m["_mkt_prob_0x0"] < MP_MAX)
    m["_filtro_0x0"] = np.where(~is0x0, "", np.where(dentro, "DENTRO", "FORA"))

    # grava de volta (backup antes; se o arquivo estiver aberto no Excel, salva copia)
    try:
        shutil.copy(PAPER, PAPER.replace(".xlsx", "_bak.xlsx"))
        m.to_excel(PAPER, index=False)
        destino = PAPER
    except PermissionError:
        destino = PAPER.replace(".xlsx", "_flag.xlsx")
        m.to_excel(destino, index=False)
        print("[aviso] placares_manuais.xlsx aberto no Excel -> gravei em %s" % os.path.basename(destino))

    # resultado acumulado do recorte DENTRO (so jogos ja com placar)
    z = m[is0x0 & m["Gols_M"].notna() & m["Gols_V"].notna()].copy()
    z["gm"] = z["Gols_M"].astype(int); z["gv"] = z["Gols_V"].astype(int)
    z["green"] = ~((z["gm"] == 0) & (z["gv"] == 0))
    z["liab"] = z["Odd"] - 1
    z["pnl"] = np.where(z["green"], 1 - COMM, -(z["Odd"] - 1))

    def R(df, t):
        if len(df) == 0:
            print("  %-14s -> 0" % t); return
        print("  %-14s | N=%-3d WR=%.1f%% | PnL=%+.1fu | ROI/liab=%+.1f%% | reds=%d"
              % (t, len(df), df["green"].mean() * 100, df["pnl"].sum(),
                 df["pnl"].sum() / df["liab"].sum() * 100, (~df["green"]).sum()))

    print("Gravado em %s (colunas _liga_rate_0x0, _mkt_prob_0x0, _filtro_0x0)." % os.path.basename(destino))
    n_dentro = int((m["_filtro_0x0"] == "DENTRO").sum()); n_fora = int((m["_filtro_0x0"] == "FORA").sum())
    print("Lay 0x0 no paper: %d DENTRO / %d FORA do filtro.\n" % (n_dentro, n_fora))
    print("=== Lay 0x0 ACUMULADO (so jogos com placar) ===")
    R(z, "TODOS 0x0")
    R(z[z["_filtro_0x0"] == "DENTRO"], "DENTRO filtro")
    R(z[z["_filtro_0x0"] == "FORA"], "FORA (descarte)")


if __name__ == "__main__":
    main()
