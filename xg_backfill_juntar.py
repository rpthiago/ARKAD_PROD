# -*- coding: utf-8 -*-
"""
xg_backfill_juntar.py — traz o xg_ft_backfill.csv da VPS e junta na base, gerando estatisticas_jogos.csv,
a tabela que os estudos passam a usar (chave Data + Home + Away, a mesma do resto do sistema).

Regra de precedencia: o que a base b365 JA tem preenchido manda; o backfill so entra onde falta
(posse=0 e chutes=0, ou xG ausente). Nada e sobrescrito — assim o backfill nunca "conserta" um numero
que ja existia, o que esconderia erro de casamento de nome.

  python xg_backfill_juntar.py [--baixar]     (--baixar faz o scp da VPS antes de juntar)
"""
import os, sys, subprocess, argparse, unicodedata, re, warnings
warnings.filterwarnings("ignore")
ROOT = os.path.dirname(os.path.abspath(__file__))
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass
import pandas as pd, numpy as np

VPS = "ubuntu@163.176.59.215"
CHAVE = os.path.expanduser("~/Downloads/ssh-key-2026-07-31.key")
REMOTO = "/home/ubuntu/betfair-collector/xg_ft_backfill.csv"
LOCAL = os.path.join(ROOT, "xg_ft_backfill.csv")
B365 = os.path.join(os.path.dirname(ROOT), "DASHBOARD_ARKAD-1", "Bases_de_Dados_API_FutPythonTrader_Bet365.csv")
SAIDA = os.path.join(ROOT, "estatisticas_jogos.csv")


def cn(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)


def baixar():
    cmd = ["scp", "-q", "-l", "8000", "-o", "StrictHostKeyChecking=no", "-i", CHAVE, "%s:%s" % (VPS, REMOTO), LOCAL]
    print("baixando da VPS..."); r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode: print("  [erro no scp] %s" % (r.stderr or "")[:200]); sys.exit(1)
    print("  ok (%.0f KB)" % (os.path.getsize(LOCAL) / 1024))


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--baixar", action="store_true"); a = ap.parse_args()
    if a.baixar or not os.path.exists(LOCAL): baixar()
    bf = pd.read_csv(LOCAL, encoding="utf-8-sig")
    print("backfill: %d jogos | %s -> %s | casamento: %s"
          % (len(bf), bf.Data.min(), bf.Data.max(), bf.casou.str.split(".").str[0].value_counts().to_dict()))

    cols = ["Date", "League", "Home", "Away", "Goals_H_FT", "Goals_A_FT", "Possession_H_FT", "Possession_A_FT",
            "Total_Shots_H_FT", "Total_Shots_A_FT", "Shots_On_Target_H_FT", "Shots_On_Target_A_FT",
            "Corners_H_FT", "Corners_A_FT", "xG_H_FT", "xG_A_FT", "xGOT_H_FT", "xGOT_A_FT"]
    head = pd.read_csv(B365, nrows=1, low_memory=False)
    b = pd.read_csv(B365, usecols=[c for c in cols if c in head.columns], low_memory=False)
    b["Date"] = pd.to_datetime(b.Date, errors="coerce")
    b = b[(b.Date >= "2026-01-01") & pd.to_numeric(b.Goals_H_FT, errors="coerce").notna()].copy()
    b["Data"] = b.Date.dt.strftime("%Y-%m-%d")
    b["k"] = b.Data + "|" + b.Home.map(cn) + "|" + b.Away.map(cn)
    bf["k"] = bf.Data + "|" + bf.Home.map(cn) + "|" + bf.Away.map(cn)
    bf = bf.drop_duplicates("k", keep="last").set_index("k")

    # o que ja existe na base
    saida = pd.DataFrame({"Data": b.Data, "League": b.League, "Home": b.Home, "Away": b.Away,
                          "gh": b.Goals_H_FT, "ga": b.Goals_A_FT})
    par = [("poss", "Possession"), ("shots", "Total_Shots"), ("sot", "Shots_On_Target"),
           ("corners", "Corners"), ("xg", "xG"), ("xgot", "xGOT")]
    for nome, base_col in par:
        for lado, suf in (("_h", "_H_FT"), ("_a", "_A_FT")):
            col = base_col + suf
            v = pd.to_numeric(b[col], errors="coerce") if col in b.columns else pd.Series(np.nan, index=b.index)
            saida[nome + lado] = v.replace(0, np.nan)
    saida["fonte"] = "b365"
    # preenche o que falta com o backfill
    falta = saida[["poss_h", "shots_h"]].isna().all(axis=1) | saida.xg_h.isna()
    idx = b.index[falta]; chaves = b.loc[idx, "k"]
    achou = chaves.isin(bf.index)
    print("linhas com buraco: %d | dessas, o backfill cobre: %d" % (len(idx), int(achou.sum())))
    for nome, _ in par:
        for lado in ("_h", "_a"):
            col = nome + lado
            if col in bf.columns:
                novo = pd.to_numeric(chaves[achou].map(bf[col]), errors="coerce")
                saida.loc[novo.index, col] = saida.loc[novo.index, col].fillna(novo)
    saida.loc[chaves[achou].index, "fonte"] = np.where(
        saida.loc[chaves[achou].index, "fonte"].eq("b365") & saida.loc[chaves[achou].index, "poss_h"].notna(),
        "b365+api", "api")
    # colunas extras que so o backfill tem
    for extra in ("bigch_h", "bigch_a", "touch_box_h", "touch_box_a", "saves_h", "saves_a", "xg_open_h", "xg_open_a"):
        if extra in bf.columns:
            saida[extra] = pd.to_numeric(b.k.map(bf[extra]), errors="coerce")
    saida.to_csv(SAIDA, index=False, encoding="utf-8-sig")
    n_xg = saida.xg_h.notna().sum()
    print("\n-> %s (%d jogos de 2026)" % (os.path.basename(SAIDA), len(saida)))
    print("   com xG: %d (%.1f%%)  | antes do backfill era %.1f%%"
          % (n_xg, 100 * n_xg / len(saida), 100 * pd.to_numeric(b.get("xG_H_FT"), errors="coerce").replace(0, np.nan).notna().mean()))
    print("   por fonte: %s" % saida.fonte.value_counts().to_dict())


if __name__ == "__main__":
    main()
