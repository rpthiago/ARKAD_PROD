"""
run_engine_on_0x0.py — Roda a ENGINE REAL de gestão de banca sobre a série HONESTA do
Lay 0x0 (edge +23.5%, odd lay real, p calibrado). Objetivo: ver o edge virar curva de
banca sob Kelly 0.25 + teto 2.5% + circuit breaker — sem reimplementar nada, usando o
código auditado (`_run_cycle_no_monitor`).
"""
from __future__ import annotations
import sys
from pathlib import Path
import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import engine_ciclo_producao as E

BETS = HERE / "wf_0x0_prod_full_ctx_bets.csv"   # série honesta do 0x0 (reconciliação)

# ── Prepara a série no formato de entrada da engine ──────────────────────────
df = pd.read_csv(BETS)
df = df[df["mes"] >= "2025-08"].copy()           # janela confiável (edge valido)
df["Time"] = "12:00:00"                           # placeholder (só p/ ordenação intradia)
df["Liga"] = "NA"
df["Metodo"] = "Lay_0x0"
df["prob_ml"] = df["p"]                            # coluna de prob que o Kelly procura
print(f"Série 0x0: {len(df)} apostas | {df['mes'].min()}..{df['mes'].max()} | "
      f"green rate {df['target'].mean():.1%} | odd_lay med {df['odd_lay'].median():.1f}")

# ── Config da engine: Kelly puro 0.25, teto 2.5%, ruin 50, circuit breaker ────
cfg = {
    "environment": "historico",
    "input": {
        "datetime": {"date_col": "Date", "time_col": "Time"},
        "columns": {
            "league_col": "Liga", "method_col": "Metodo",
            "odd_signal_col": "odd_lay", "result_col": "target",
            "odd_exec_col": "odd_lay",
        },
    },
    "filters": {"exclude_leagues": [], "conditional_rules": [], "toxic_cuts": []},
    "execution_guards": {"slippage": {"enabled": False}, "liquidity": {"enabled": False}},
    "cycle": {
        "commission_rate": 0.05,
        "initial_base": 500.0,
        "teto": 2000.0,
        "ramp_transition": {"enabled": False},   # sem rampa (Kelly puro)
        "enable_kelly": True,
        "kelly_fraction": 0.25,
        "max_liability_pct": 0.025,              # teto 2.5% liability/aposta
        "ruin_floor": 50.0,
        "circuit_breaker": {"max_daily_drawdown": -1.5, "max_sequential_reds": 3},
    },
}

prepared = E.prepare_dataframe(df, cfg, environment="historico")
filtered = E.apply_config_filters(prepared, cfg)
out, summary = E._run_cycle_no_monitor(filtered, cfg, environment="historico")

# ── Resultados ───────────────────────────────────────────────────────────────
init = cfg["cycle"]["initial_base"]
lucro = summary["Lucro_Final"]
banca_final = init + lucro
executed = out[out["Status_Execucao"] == "EXECUTED"].copy()
skipped = out[out["Status_Execucao"] == "SKIPPED"]
liab_med = executed["Stake_Final_Aplicada"].median()

print("\n=== ENGINE REAL sobre o Lay 0x0 honesto (Kelly 0.25 | teto 2.5% | CB on) ===")
print(f"  Banca inicial:        R$ {init:,.0f}")
print(f"  Apostas executadas:   {summary['Entradas_Executadas']}  (skipadas: {summary['Entradas_Skipadas']})")
print(f"  Win rate:             {summary['Win_Rate_Executadas_%']:.1f}%")
print(f"  Lucro final:          R$ {lucro:,.0f}")
print(f"  Banca final:          R$ {banca_final:,.0f}  ({banca_final/init:.2f}x)")
print(f"  Max Drawdown:         {summary['Max_Drawdown_%']:.1f}%  (R$ {summary['Max_Drawdown_Abs']:,.0f})")
print(f"  Step-ups / downs:     {summary['Step_Ups']} / {summary['Step_Downs']}")
print(f"  Liability mediana:    R$ {liab_med:,.0f}  ({liab_med/banca_final*100:.2f}% da banca final)")
if len(skipped):
    print(f"  Motivos de skip:      {dict(skipped['Skip_Reason'].value_counts().head())}")

# Curva de banca por mês
executed["mes"] = executed["mes"] if "mes" in executed.columns else pd.to_datetime(executed["Date"]).dt.to_period("M").astype(str)
mm = executed.groupby("mes").agg(apostas=("PnL_Linha", "size"), pnl=("PnL_Linha", "sum")).reset_index()
mm["banca_fim"] = init + mm["pnl"].cumsum()
print("\n  Evolução mensal:")
print(f"  {'mes':<9}{'apostas':>8}{'pnl':>12}{'banca_fim':>12}")
for _, r in mm.iterrows():
    print(f"  {r['mes']:<9}{int(r['apostas']):>8}{r['pnl']:>+12.0f}{r['banca_fim']:>12.0f}")
