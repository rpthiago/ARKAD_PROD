# -*- coding: utf-8 -*-
"""auditar_lay0x1_fav.py — testa a IDEIA 2 (Lay 0x1 / Lay 1x0 no super favorito) no historico,
com a odd de LAY REAL da Betfair, ano a ano (o teste que mata "pool esconde edge morto").

Regra do observador:
  Lay 0x1: Odd_H_Back <= 1.90 e 5.0 <= Odd_CS_0x1_Lay <= 15.0  -> RED se o FT for exatamente 0-1
  Lay 1x0: Odd_A_Back <= 1.90 e 5.0 <= Odd_CS_1x0_Lay <= 15.0  -> RED se o FT for exatamente 1-0
"""
import numpy as np, pandas as pd

COMM = 0.045
FAV_MAX = 1.90
LAY_LO, LAY_HI = 5.0, 15.0

b = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH3.csv", low_memory=False)
b["Date"] = pd.to_datetime(b.Date, errors="coerce")
for c in ("Odd_H_Back", "Odd_A_Back", "Odd_CS_0x1_Lay", "Odd_CS_1x0_Lay",
          "Goals_H_FT", "Goals_A_FT"):
    b[c] = pd.to_numeric(b[c], errors="coerce")
b = b.dropna(subset=["Goals_H_FT", "Goals_A_FT"])
b["ano"] = b.Date.dt.year


def universo(tipo):
    if tipo == "0x1":
        d = b[(b.Odd_H_Back <= FAV_MAX) & (b.Odd_CS_0x1_Lay >= LAY_LO) & (b.Odd_CS_0x1_Lay <= LAY_HI)].copy()
        d["lay"] = d.Odd_CS_0x1_Lay
        d["red"] = ((d.Goals_H_FT == 0) & (d.Goals_A_FT == 1)).astype(int)
    else:
        d = b[(b.Odd_A_Back <= FAV_MAX) & (b.Odd_CS_1x0_Lay >= LAY_LO) & (b.Odd_CS_1x0_Lay <= LAY_HI)].copy()
        d["lay"] = d.Odd_CS_1x0_Lay
        d["red"] = ((d.Goals_H_FT == 1) & (d.Goals_A_FT == 0)).astype(int)
    return d.dropna(subset=["lay"])


def resumo(d, lab):
    if len(d) < 30:
        print("  %-20s N=%d (insuficiente)" % (lab, len(d))); return None
    odd = d.lay.values
    p = np.where(d.red == 0, (1 - COMM), -(odd - 1.0))
    wr = 100 * (1 - d.red.mean())
    bewr = 100 * np.mean((odd - 1) / (odd - COMM))
    roi = 100 * p.sum() / (odd - 1).sum()
    print("  %-20s N=%6d | WR=%6.2f%% | BE=%6.2f%% | gap=%+5.2f pp | odd=%5.2f | ROI_liab=%+7.2f%% | reds=%d"
          % (lab, len(d), wr, bewr, wr - bewr, odd.mean(), roi, int(d.red.sum())))
    return roi


tot = []
for tipo in ("0x1", "1x0"):
    d = universo(tipo)
    print("=" * 104)
    print("LAY %s — super favorito (Odd back <= %.2f) e odd de lay real entre %.1f e %.1f"
          % (tipo, FAV_MAX, LAY_LO, LAY_HI))
    print("=" * 104)
    resumo(d, "TODOS os anos")
    for ano, g in d.groupby("ano"):
        resumo(g, "  %d" % ano)
    print("  --- por faixa de odd de lay (a armadilha do Idea1: BE muda muito) ---")
    for lo, hi in ((5, 8), (8, 11), (11, 13), (13, 15.01)):
        resumo(d[(d.lay >= lo) & (d.lay < hi)], "  lay %.0f-%.0f" % (lo, hi))
    d["tipo"] = tipo
    tot.append(d[["Date", "ano", "lay", "red", "tipo"]])
    print()

j = pd.concat(tot)
print("=" * 104)
print("OS DOIS JUNTOS (como o observador opera)")
print("=" * 104)
resumo(j, "TODOS")
for ano, g in j.groupby("ano"):
    resumo(g, "  %d" % ano)

odd = j.lay.values
p = np.where(j.red == 0, (1 - COMM), -(odd - 1.0))
rng = np.random.default_rng(17)
bt = []
for _ in range(10000):
    i = rng.integers(0, len(j), len(j))
    bt.append(100 * p[i].sum() / (odd[i] - 1).sum())
bt = np.sort(bt)
print("\n  bootstrap IC95 do ROI sobre liability: [%+.2f%%, %+.2f%%]" % (bt[250], bt[9750]))

pr = j.red.mean()
be_med = np.mean((odd - 1) / (odd - COMM))
print("\n=== QUANTO N SERIA PRECISO PARA DECIDIR? ===")
print("  taxa de RED observada: %.3f%% | break-even exige %.3f%% de green" % (100 * pr, 100 * be_med))
var = np.var(p)
for alvo in (0.01, 0.02):
    n = (1.96 ** 2) * var / (alvo * np.mean(odd - 1)) ** 2
    print("  p/ detectar edge de %.0f%% sobre liability com IC95 excluindo zero: N ~ %,.0f picks"
          .replace(",", "") % (100 * alvo, n))
