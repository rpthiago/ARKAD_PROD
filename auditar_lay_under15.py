# -*- coding: utf-8 -*-
"""auditar_lay_under15.py — auditoria do Lay Under 1.5 FT (XGBoost) pelo protocolo do GEMINI.md.

Testes:
  1) MERCADO, ANO A ANO — laiar Under 1.5 cego na faixa 2,50-4,50. E o teste que matou o
     Under 1.5 HT ("pool esconde edge morto": +35% em 2021, -5% de 2023 em diante).
  2) MODELO, ANO A ANO — a selecao do XGB com EV>=5%, com a odd de LAY REAL.
  3) OOS estrito — o modelo foi treinado em Date<2026-01-01; so 2026 e fora da amostra.
  4) Bootstrap IC95 e break-even por faixa de odd (a armadilha do Idea1).
"""
import numpy as np, pandas as pd, joblib, math
from math import erfc

COMM = 0.045
ODD_COL, ODD_MIN, ODD_MAX = "Odd_Under15_FT_Lay", 2.50, 4.50


def pnl_lay(is_win, odd):
    return np.where(is_win == 1, (1 - COMM), -(odd - 1.0))


def resumo(d, lab, odd_col=ODD_COL):
    if len(d) < 10:
        print("  %-22s N=%d (insuficiente)" % (lab, len(d))); return None
    odd = d[odd_col].values
    p = pnl_lay(d.is_over15.values, odd)
    wr = 100 * d.is_over15.mean()
    be = 100 * np.mean((odd - 1) / (odd - COMM))
    roi_liab = 100 * p.sum() / (odd - 1).sum()
    print("  %-22s N=%5d | WR=%6.2f%% | BE=%6.2f%% | gap=%+5.2f pp | odd=%.2f | ROI_liab=%+7.2f%%"
          % (lab, len(d), wr, be, wr - be, odd.mean(), roi_liab))
    return roi_liab


b = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH3.csv", low_memory=False)
b["Date"] = pd.to_datetime(b.Date, errors="coerce")
for c in (ODD_COL, "Goals_H_FT", "Goals_A_FT"):
    b[c] = pd.to_numeric(b[c], errors="coerce")
b["is_over15"] = ((b.Goals_H_FT + b.Goals_A_FT) > 1.5).astype(int)
uni = b[(b[ODD_COL] >= ODD_MIN) & (b[ODD_COL] <= ODD_MAX)].dropna(subset=["Goals_H_FT"]).copy()
uni["ano"] = uni.Date.dt.year

print("=" * 96)
print("1. O MERCADO — laiar Under 1.5 CEGO na faixa %.2f-%.2f (odd de LAY REAL)" % (ODD_MIN, ODD_MAX))
print("=" * 96)
resumo(uni, "TODOS os anos")
for ano, g in uni.groupby("ano"):
    resumo(g, "  %d" % ano)

print("\n" + "=" * 96)
print("2. O MODELO — selecao do XGB com EV>=5%")
print("=" * 96)
try:
    bundle = joblib.load("models/modelo_lay_under15_xgb.joblib")
    feats = bundle["features"]
    ds = pd.read_parquet("scratch/dataset_leak_free_features.parquet")
    ds["Date"] = pd.to_datetime(ds["Date"], errors="coerce")
    faltando = [f for f in feats if f not in ds.columns]
    if faltando:
        print("  [PULADO] o parquet nao tem %d features do modelo: %s"
              % (len(faltando), faltando[:5]))
        raise SystemExit(0)
    for c in (ODD_COL, "Goals_H_FT", "Goals_A_FT"):
        if c in ds.columns:
            ds[c] = pd.to_numeric(ds[c], errors="coerce")
    d = ds[(ds[ODD_COL] >= ODD_MIN) & (ds[ODD_COL] <= ODD_MAX)].dropna(subset=feats + ["Goals_H_FT"]).copy()
    d["is_over15"] = ((d.Goals_H_FT + d.Goals_A_FT) > 1.5).astype(int)
    d["p"] = bundle["model"].predict_proba(d[feats])[:, 1]
    d["ev"] = d.p * (1 - COMM) - (1 - d.p) * (d[ODD_COL] - 1)
    d["ano"] = d.Date.dt.year
    print("  universo com features completas: N=%d | de %s a %s"
          % (len(d), d.Date.min().date(), d.Date.max().date()))
    sel = d[d.ev >= 0.05]
    print()
    resumo(sel, "SINAIS (EV>=5%) todos")
    for ano, g in sel.groupby("ano"):
        resumo(g, "  %d" % ano)

    print("\n  --- OOS ESTRITO (modelo treinado em Date<2026-01-01) ---")
    resumo(sel[sel.Date < "2026-01-01"], "treino (in-sample)")
    oos = sel[sel.Date >= "2026-01-01"]
    resumo(oos, "2026 (OOS)")

    print("\n  --- o filtro de EV melhora sobre o universo? ---")
    resumo(d[(d.Date >= '2026-01-01')], "2026 SEM filtro")
    resumo(oos, "2026 COM EV>=5%")

    if len(oos) > 30:
        rng = np.random.default_rng(9)
        odd = oos[ODD_COL].values; y = oos.is_over15.values
        bt = []
        for _ in range(10000):
            i = rng.integers(0, len(y), len(y))
            p = pnl_lay(y[i], odd[i])
            bt.append(100 * p.sum() / (odd[i] - 1).sum())
        bt = np.sort(bt)
        print("\n  bootstrap IC95 do ROI OOS 2026: [%+.2f%%, %+.2f%%] | P(<=0)=%.4f"
              % (bt[250], bt[9750], (np.array(bt) <= 0).mean()))

    print("\n  --- break-even POR FAIXA DE ODD (a armadilha do Idea1) ---")
    for lo, hi in ((2.50, 3.00), (3.00, 3.50), (3.50, 4.00), (4.00, 4.50)):
        s = sel[(sel[ODD_COL] >= lo) & (sel[ODD_COL] < hi)]
        resumo(s, "  odd %.2f-%.2f" % (lo, hi))
except FileNotFoundError as e:
    print("  [PULADO] %s" % e)
