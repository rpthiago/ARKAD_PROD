# -*- coding: utf-8 -*-
"""liquidar_under15_pendentes.py — fecha os pendentes do observador Lay Under 1.5 FT.

POR QUE EXISTE: o `observar_under15_forward.py` liquida lendo a base FRESH, que esta parada em
2026-08-20. Os 8 sinais de 28/08 a 08/09 ficaram 'Pendente' para sempre — nao e bug de codigo,
e base de resultados desatualizada. Aqui uso o feed de resultados `football_data_odds.csv`
(colunas gh/ga), que esta atualizado.

LAY Under 1.5 FT:  GREEN quando o jogo tem >= 2 gols (o Under 1.5 NAO acontece).
                   RED   quando tem 0 ou 1 gol.
P&L por stake: GREEN +(1 - comissao) · RED -(odd_lay - 1).
"""
import csv, io, re, unicodedata, sys
import pandas as pd

COMM = 0.045
LOG = r"c:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD\observacao_under15_forward.csv"
RES = r"c:\Users\thiag\OneDrive\Documentos\GitHub\DASHBOARD_ARKAD-1\football_data_odds.csv"


def cn(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)


def bate(a, b):
    x, y = cn(a), cn(b)
    if not x or not y:
        return False
    return x == y or (len(x) >= 5 and len(y) >= 5 and (x in y or y in x))


res = pd.read_csv(RES, usecols=["Date", "Home", "Away", "gh", "ga"], low_memory=False)
res["Date"] = pd.to_datetime(res.Date, errors="coerce")
res = res.dropna(subset=["gh", "ga"])
print("feed de resultados: %d jogos | de %s a %s"
      % (len(res), res.Date.min().date(), res.Date.max().date()))

rows = list(csv.DictReader(io.open(LOG, encoding="utf-8-sig")))
cols = list(rows[0].keys())
print("\nlog do observador: %d linhas | pendentes: %d\n"
      % (len(rows), sum(1 for r in rows if r["status"] == "Pendente")))

liq = 0
for r in rows:
    if r["status"] != "Pendente":
        continue
    jogo = r["jogo"]
    if " x " not in jogo:
        continue
    h, a = [p.strip() for p in jogo.split(" x ", 1)]
    dia = pd.to_datetime(r["data"], errors="coerce")
    cand = res[(res.Date >= dia - pd.Timedelta(days=1)) & (res.Date <= dia + pd.Timedelta(days=1))]
    achou = None
    for _, x in cand.iterrows():
        if bate(h, x.Home) and bate(a, x.Away):
            achou = x
            break
    if achou is None:
        print("  [nao achei resultado] %s (%s)" % (jogo, r["data"]))
        continue
    tot = int(achou.gh) + int(achou.ga)
    odd = float(r["odd_lay"])
    green = tot >= 2                       # LAY Under 1.5 ganha quando saem 2+ gols
    pnl = (1 - COMM) if green else -(odd - 1.0)
    r["status"] = "Liquidado"
    r["resultado"] = "GREEN" if green else "RED"
    r["pnl_unidades"] = "%.4f" % pnl
    liq += 1
    print("  %-34s %s  %d-%d (%d gols) | lay %.2f -> %-5s  pnl %+.3f"
          % (jogo[:34], str(achou.Date)[:10], achou.gh, achou.ga, tot, odd,
             r["resultado"], pnl))

with io.open(LOG, "w", encoding="utf-8-sig", newline="") as f:
    w = csv.DictWriter(f, fieldnames=cols, restval="")
    w.writeheader(); w.writerows(rows)

fin = [r for r in rows if r["status"] == "Liquidado"]
print("\nliquidados agora: %d | total liquidado no log: %d" % (liq, len(fin)))
if fin:
    p = [float(r["pnl_unidades"]) for r in fin]
    g = sum(1 for r in fin if r["resultado"] == "GREEN")
    odds = [float(r["odd_lay"]) for r in fin]
    be = sum((o - 1) / (o - COMM) for o in odds) / len(odds)
    print("  N=%d | %dG %dR | WR=%.1f%% | break-even medio=%.1f%%"
          % (len(fin), g, len(fin) - g, 100 * g / len(fin), 100 * be))
    print("  PnL=%+.3f u | ROI/stake=%+.2f%% | ROI/liability=%+.2f%%"
          % (sum(p), 100 * sum(p) / len(p), 100 * sum(p) / sum(o - 1 for o in odds)))
