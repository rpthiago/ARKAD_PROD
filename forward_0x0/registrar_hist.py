# -*- coding: utf-8 -*-
"""
Anexa um snapshot datado do forward do Lay 0x0 a `forward_0x0_historico.csv`.

CORRECAO 10/09/2026: lia o `lay_0x0_real_oos_bets_xgb.csv` (a RE-SIMULACAO) e chamava aquilo de
forward. A simulacao treina numa janela diferente da do gerador ao vivo, produz um conjunto de
picks diferente (dos 5 picks enviados em 15/08 ela contem 3) e e regenerada toda semana — logo
nao e registro de forward. Agora le o LEDGER dos picks efetivamente enviados
(`ledger_forward_0x0.csv`, alimentado por `liquidar_picks_0x0.py`).

Grava o ROI nas duas convencoes com o nome explicito na coluna, porque a confusao entre elas foi
o que inflou o forward em ~29x no relatorio de 07/09 (ver EMENDA no pre-registro).
"""
import os, csv, sys
from datetime import datetime
import pandas as pd, numpy as np
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass

AQUI = os.path.dirname(os.path.abspath(__file__))
LED = os.path.join(AQUI, "ledger_forward_0x0.csv")
HIST = os.path.join(AQUI, "forward_0x0_historico.csv")
COLS = ["snapshot", "fonte", "fwd_N", "fwd_WR", "fwd_BE", "fwd_gap_pp",
        "fwd_ROI_liab", "fwd_ROI_stake", "greens", "reds", "pendentes", "fora_da_regra"]

if not os.path.exists(LED):
    print("ledger ausente — rode liquidar_picks_0x0.py primeiro"); sys.exit()

L = pd.read_csv(LED, encoding="utf-8-sig")
na = pd.to_numeric(L["na_regra"], errors="coerce")
lv = L[(L["status"] == "LIQUIDADO") & (na == 1)].copy()
pend = int((L["status"] == "PENDENTE").sum())
fora = int((na != 1).sum())

if len(lv) == 0:
    n = wr = be = gap = rl = rs = g = r = 0
else:
    for c in ("pnl_liab", "pnl_stake", "break_even", "target"):
        lv[c] = pd.to_numeric(lv[c], errors="coerce")
    n = len(lv)
    wr = round(100 * lv["target"].mean(), 2)
    be = round(100 * lv["break_even"].mean(), 2)
    gap = round(wr - be, 2)
    rl = round(100 * lv["pnl_liab"].mean(), 2)
    rs = round(100 * lv["pnl_stake"].mean(), 2)
    g = int(lv["target"].sum()); r = n - g

novo = not os.path.exists(HIST)
# o historico antigo tinha outro cabecalho (fwd_ROI sem convencao declarada); preserva como .bak
if not novo:
    velho = list(csv.reader(open(HIST, encoding="utf-8")))
    if velho and velho[0] != COLS:
        os.replace(HIST, HIST + ".bak_simulacao_20260910")
        print("historico antigo (era da simulacao) preservado em forward_0x0_historico.csv.bak_simulacao_20260910")
        novo = True

with open(HIST, "a", newline="", encoding="utf-8") as fh:
    w = csv.writer(fh)
    if novo: w.writerow(COLS)
    w.writerow([datetime.now().strftime("%Y-%m-%d %H:%M"), "ledger_picks_enviados",
                n, wr, be, gap, rl, rs, g, r, pend, fora])

print("historico do forward atualizado: N=%d (%dG/%dR) | WR %.2f%% vs BE %.2f%% (gap %+.2fpp)"
      % (n, g, r, wr, be, gap))
print("  ROI liability=%+.2f%% | ROI stake=%+.2f%% | pendentes=%d | fora da regra=%d"
      % (rl, rs, pend, fora))
