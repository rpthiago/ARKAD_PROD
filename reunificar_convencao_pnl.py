#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""reunificar_convencao_pnl.py — poe TODA a base mestre numa unica convencao de P&L.

PROBLEMA ENCONTRADO NA AUDITORIA (10/09/2026):
  A base `Sinais_Metodos_Aprovados_Odds_Reais_Betfair.csv` somava DUAS convencoes:
    * 189 linhas ate 02/09 : RED = -1,0            -> LIABILITY normalizada (liability = 1 u)
                             GREEN = (1-c)/(odd-1), comissao implicita 3,51%
    * 77 linhas de 03-09/09: RED = -(odd-1)        -> STAKE normalizada (stake = 1 u)
                             GREEN = 0,95, comissao 5%
  `+12,27 + 17,80 = +30,07 u` somava unidades diferentes: no 1o grupo cada aposta arrisca 1 u,
  no 2o arrisca (odd-1) u — na odd 6,8 isso e 5,8 u. Escalas ~5x distintas.

CONVENCAO ESCOLHIDA: **LIABILITY = 1 unidade**, comissao **5%** (Lei no 4 do GEMINI.md).
  Por que liability e nao stake: (a) e a regua economicamente correta para LAY, porque o capital
  em risco e a liability, nao o stake; (b) e o que as colunas em R$ dos 77 novos JA fazem
  (`Risco_Red_R$` = 100 fixo, `Stake_Sugerida_R$` = 100/(odd-1)); (c) e o que os 189 antigos
  ja usavam no `PnL_u`. Ou seja, e a convencao pretendida no arquivo — o desvio era so no PnL_u
  das 77 linhas novas.

  GREEN: PnL_u = (1 - 0,05) / (odd - 1)      RED: PnL_u = -1,0
  Em R$ com liability de R$100: stake = 100/(odd-1) · green = stake*0,95 · red = -100

Tambem grava `PnL_stake_u` (convencao stake, para quem quiser comparar) e `Convencao_PnL`.
Backup automatico antes de escrever.
"""
import shutil, sys
from datetime import datetime
import numpy as np, pandas as pd

CSV = "metodos_aprovados/Sinais_Metodos_Aprovados_Odds_Reais_Betfair.csv"
COMM = 0.05
LIAB_R = 100.0

d = pd.read_csv(CSV)
d.columns = [c.strip().replace("﻿", "") for c in d.columns]
bak = CSV.replace(".csv", ".bak_%s.csv" % datetime.now().strftime("%Y%m%d_%H%M"))
shutil.copy(CSV, bak)
print("backup: %s" % bak)

d["_dt"] = pd.to_datetime(d["Data"], errors="coerce", dayfirst=True)
d["Odd_Entrada"] = pd.to_numeric(d["Odd_Entrada"], errors="coerce")
d["_pnl_antigo"] = pd.to_numeric(d["PnL_u"], errors="coerce")
res = d["Resultado"].astype(str).str.upper()
d["_green"] = res.str.contains("GREEN")
d["_red"] = res.str.contains("RED")
d["_liq"] = d._green | d._red

print("\nlinhas: %d | liquidadas: %d | pendentes: %d"
      % (len(d), int(d._liq.sum()), int((~d._liq).sum())))
antes = d.loc[d._liq, "_pnl_antigo"].sum()
print("soma do PnL_u ANTES (misturado): %+.2f u" % antes)

ok = d._liq & d.Odd_Entrada.notna() & (d.Odd_Entrada > 1.0)
sem_odd = d._liq & ~ok
if sem_odd.any():
    print("\n[ATENCAO] %d linhas liquidadas SEM odd valida — ficam com PnL vazio (Lei no 3: nao fabricar)."
          % int(sem_odd.sum()))
    for _, r in d[sem_odd].head(5).iterrows():
        print("   %s | %s | %s" % (str(r["Data"])[:10], str(r["Jogo"])[:34], r["Método"]))

odd = d.Odd_Entrada
# --- convencao canonica: LIABILITY = 1 u
pnl_liab = pd.Series(np.nan, index=d.index)
pnl_liab[ok & d._green] = (1 - COMM) / (odd[ok & d._green] - 1.0)
pnl_liab[ok & d._red] = -1.0
# --- convencao alternativa (referencia): STAKE = 1 u
pnl_stake = pd.Series(np.nan, index=d.index)
pnl_stake[ok & d._green] = (1 - COMM)
pnl_stake[ok & d._red] = -(odd[ok & d._red] - 1.0)

d["PnL_u"] = pnl_liab.round(4)
d["PnL_stake_u"] = pnl_stake.round(4)
d["Convencao_PnL"] = np.where(ok, "LIABILITY=1u | comissao 5%", "")

# --- colunas em R$ coerentes com liability de R$100
d["Stake_Sugerida_R$"] = np.where(ok, (LIAB_R / (odd - 1.0)).round(2), np.nan)
d["Risco_Red_R$"] = np.where(ok, LIAB_R, np.nan)
d["PnL_R$"] = np.where(ok, (pnl_liab * LIAB_R).round(2), np.nan)

print("\n=== DEPOIS (tudo em LIABILITY=1u, comissao 5%) ===")
print("  soma do PnL_u: %+.2f u   (o arquivo declarava +30,07 misturando convencoes)" % d["PnL_u"].sum())
print("  soma em R$ com liability de R$100 por aposta: %+.2f" % d["PnL_R$"].sum())
print("  para referencia, em convencao STAKE=1u: %+.2f u" % d["PnL_stake_u"].sum())

print("\n=== por janela, na convencao unica ===")
for lab, m in (("antigos (ate 02/09)", d._dt < "2026-09-03"), ("os 77 (03-09/09)", d._dt >= "2026-09-03")):
    s = d[m & ok]
    if not len(s):
        continue
    o = s.Odd_Entrada
    be = 100 * np.mean((o - 1) / (o - COMM))
    wr = 100 * s._green.mean()
    print("  %-22s N=%3d | WR=%6.2f%% | BE=%6.2f%% | gap=%+5.2f pp | PnL=%+7.3f u"
          % (lab, len(s), wr, be, wr - be, s["PnL_u"].sum()))

print("\n=== por metodo ===")
for met, g in d[ok].groupby("Método"):
    o = g.Odd_Entrada
    be = 100 * np.mean((o - 1) / (o - COMM))
    wr = 100 * g._green.mean()
    print("  %-30s N=%3d | WR=%6.2f%% | BE=%6.2f%% | gap=%+5.2f pp | PnL=%+7.3f u"
          % (str(met)[:30], len(g), wr, be, wr - be, g["PnL_u"].sum()))

d.drop(columns=["_dt", "_pnl_antigo", "_green", "_red", "_liq"]).to_csv(CSV, index=False, encoding="utf-8-sig")
print("\ngravado %s" % CSV)
