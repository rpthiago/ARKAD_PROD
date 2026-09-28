# -*- coding: utf-8 -*-
"""
xg_backfill_alvos.py — monta a LISTA DE ALVOS do backfill de estatisticas (roda aqui, no PC).

A base b365 tem xG/chutes/posse, mas em 2026 26,9% dos jogos vem sem NENHUMA estatistica
(posse=0 e chutes=0, que num jogo jogado e impossivel) e outros 31,8% vem sem xG. Este script
escolhe quais jogos valem uma chamada de API e grava alvos_xg.csv para o xg_backfill.py (VPS).

Prioriza, nesta ordem:
  1. ligas que aparecem nos SEUS sinais (ledger das 06:00) — sao as que decidem metodo;
  2. jogos mais recentes (o estudo olha o regime atual).

  python xg_backfill_alvos.py [--desde 2026-01-01] [--limite 3000] [--so-sem-xg]
"""
import os, sys, io, csv, argparse, contextlib, warnings
warnings.filterwarnings("ignore")
ROOT = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, ROOT)
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass
import pandas as pd, numpy as np

B365 = os.path.join(os.path.dirname(ROOT), "DASHBOARD_ARKAD-1", "Bases_de_Dados_API_FutPythonTrader_Bet365.csv")
SAIDA = os.path.join(ROOT, "alvos_xg.csv")
COLS = ["Date", "League", "Home", "Away", "Goals_H_FT", "Goals_A_FT",
        "Possession_H_FT", "Total_Shots_H_FT", "xG_H_FT", "xG_A_FT"]


def ligas_do_ledger():
    """as ligas em que os seus metodos realmente deram sinal (peso 1); o resto vem depois."""
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            import relatorio_forward_5metodos as R
        L = pd.read_csv(R.LEDGER, dtype=str, encoding="utf-8-sig")
        return set(L.Liga.dropna().astype(str).str.upper())
    except Exception as e:
        print("  [aviso] nao consegui ler o ledger (%s); sigo sem prioridade de liga" % str(e)[:60])
        return set()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--desde", default="2026-01-01")
    ap.add_argument("--limite", type=int, default=3000, help="quantos alvos gravar (1 chamada de API cada)")
    ap.add_argument("--so-sem-xg", action="store_true", help="incluir tambem jogos que tem chutes mas nao tem xG")
    a = ap.parse_args()

    d = pd.read_csv(B365, usecols=COLS, low_memory=False)
    d["Date"] = pd.to_datetime(d.Date, errors="coerce")
    d = d[(d.Date >= a.desde) & pd.to_numeric(d.Goals_H_FT, errors="coerce").notna()].copy()
    poss = pd.to_numeric(d.Possession_H_FT, errors="coerce").fillna(0)
    sh = pd.to_numeric(d.Total_Shots_H_FT, errors="coerce").fillna(0)
    xg = pd.to_numeric(d.xG_H_FT, errors="coerce").fillna(0)
    d["sem_nada"] = (poss == 0) & (sh == 0)
    d["sem_xg"] = (xg == 0)
    alvo = d.sem_nada | d.sem_xg if a.so_sem_xg else d.sem_nada
    d = d[alvo].copy()
    print("jogos sem estatistica desde %s: %d (sem nada: %d | sem xG: %d)"
          % (a.desde, len(d), int(d.sem_nada.sum()), int(d.sem_xg.sum())))

    lg = ligas_do_ledger()
    d["prio"] = np.where(d.League.astype(str).str.upper().isin(lg), 0, 1)
    d = d.sort_values(["prio", "Date"], ascending=[True, False]).head(a.limite)
    print("gravando %d alvos | dos quais em ligas dos seus sinais: %d | dias distintos: %d"
          % (len(d), int((d.prio == 0).sum()), d.Date.dt.strftime("%Y-%m-%d").nunique()))

    with io.open(SAIDA, "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh); w.writerow(["Data", "Home", "Away", "League", "gh", "ga"])
        for r in d.itertuples():
            w.writerow([r.Date.strftime("%Y-%m-%d"), r.Home, r.Away, r.League, int(r.Goals_H_FT), int(r.Goals_A_FT)])
    print("-> %s" % os.path.basename(SAIDA))
    print("\nagora, na VPS:  python3 xg_backfill.py --max 500")


if __name__ == "__main__":
    main()
