"""
Comparativo de GESTAO DE BANCA — mesmo filtro (Blacklist ARKAD_PROD, 357 entradas)

Cenarios:
  A) Rampa ARKAD_PROD    — base=500, teto=2000, warmup 30/60/100% nos primeiros 14 dias
  B) Compound puro       — igual ao A mas sem rampa (compound imediato desde o dia 1)
  C) Stake Fixo R$500    — sem compound, R$500 por aposta
  D) Kelly 25%           — fracao Kelly por aposta, banca inicial R$1.200 (agressivo)
  E) Kelly 12.5%         — fracao Kelly mais conservadora

Dataset: pacote_reproducao_todos_jogos_20250801_20260414.csv
"""
import copy
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from engine_ciclo_producao import (
    _current_dd,
    _run_cycle_no_monitor,
    _to_float,
    load_config,
    prepare_dataframe,
)

# ---------------------------------------------------------------------------
# Dados e filtro BL
# ---------------------------------------------------------------------------
CSV = Path("Arquivados_Apostas_Diarias/Relatorios/WalkForward/pacote_reproducao_todos_jogos_20250801_20260414.csv")
df_raw = pd.read_csv(CSV)
df_raw["Data_Arquivo"] = pd.to_datetime(df_raw["Data_Arquivo"], errors="coerce")
df_raw = df_raw[df_raw["1/0"].notna()].copy()

cfg = load_config(Path("config_backtest_exec.json"))
rodo = json.loads(Path("config_rodos_master.json").read_text(encoding="utf-8"))
cuts = rodo.get("filtros_rodo", [])

result_col = cfg["input"]["columns"]["result_col"]
if result_col not in df_raw.columns:
    df_raw[result_col] = df_raw["1/0"]


def _matches_cut(row: pd.Series, cut: dict, league_col: str, method_col: str, odd_col: str) -> bool:
    league = str(row.get(league_col, ""))
    method = str(row.get(method_col, ""))
    odd = pd.to_numeric(row.get(odd_col), errors="coerce")
    if cut.get("league") and str(cut["league"]).upper() != league.upper():
        return False
    if cut.get("method_equals"):
        meth_cut = str(cut["method_equals"])
        aliases = {
            "Lay_CS_0x1_B365": {"Lay_CS_0x1_B365", "Lay_0x1", "L0x1"},
            "Lay_CS_1x0_B365": {"Lay_CS_1x0_B365", "Lay_1x0", "L1x0"},
        }
        allowed = aliases.get(meth_cut, {meth_cut})
        if method not in allowed:
            return False
    if not pd.isna(odd):
        odd_min = cut.get("odd_min")
        odd_max = cut.get("odd_max")
        if odd_min is not None and float(odd) < float(odd_min):
            return False
        if odd_max is not None and float(odd) > float(odd_max):
            return False
    return True


df_raw["_bl"] = df_raw.apply(
    lambda r: not any(_matches_cut(r, c, "Liga", "Metodo", "Odd_Base") for c in cuts), axis=1
)
df_bl = df_raw[df_raw["_bl"]].copy().reset_index(drop=True)
print(f"Entradas BL filtradas: {len(df_bl)}\n")

# prepare_dataframe (mode historico: odd_exec = odd_signal, sem slippage skip)
df_bl[result_col] = df_bl["1/0"]
df_p = prepare_dataframe(df_bl.copy(), cfg, environment="historico")

# ---------------------------------------------------------------------------
# Cenario A: Rampa ARKAD_PROD
# ---------------------------------------------------------------------------
df_out_a, summ_a = _run_cycle_no_monitor(df_p.copy(), cfg, environment="historico")

# ---------------------------------------------------------------------------
# Cenario B: Compound puro (sem rampa)
# ---------------------------------------------------------------------------
cfg_b = copy.deepcopy(cfg)
cfg_b["cycle"]["ramp_transition"]["enabled"] = False
cfg_b["enable_rampa"] = False
df_out_b, summ_b = _run_cycle_no_monitor(df_p.copy(), cfg_b, environment="historico")

# ---------------------------------------------------------------------------
# Cenarios C, D, E: implementacao manual (stake fixo / Kelly)
# ---------------------------------------------------------------------------
COMMISSION = 0.065
BANCA_INICIAL = 1200.0


def simular_fixo(df_sim: pd.DataFrame, stake: float = 500.0) -> tuple[dict, list]:
    banca = BANCA_INICIAL
    equity = [banca]
    pnls: list[float] = []
    for _, row in df_sim.iterrows():
        odd = _to_float(row.get("Odd_Base"))
        res = int(row["1/0"])
        if odd is None or odd <= 1.0:
            pnls.append(0.0)
            continue
        if res == 1:
            pnl = stake / (odd - 1.0) * (1.0 - COMMISSION)
        else:
            pnl = -stake
        banca += pnl
        equity.append(banca)
        pnls.append(pnl)
    dd_abs, dd_pct = _current_dd(equity)
    wins = sum(1 for p in pnls if p > 0)
    losses = sum(1 for p in pnls if p < 0)
    total = wins + losses
    wr = wins / total * 100 if total > 0 else 0.0
    lucro_final = banca - BANCA_INICIAL
    score = abs(lucro_final / dd_abs) if dd_abs > 0 else float("inf")
    return {
        "Lucro_Final": lucro_final,
        "Win_Rate_Executadas_%": wr,
        "Max_Drawdown_Abs": dd_abs,
        "Max_Drawdown_%": dd_pct,
        "Entradas_Executadas": total,
        "Score": score,
    }, pnls


def _kelly_fraction(odd: float, p: float) -> float:
    """Fracao Kelly para uma aposta LAY (responsabilidade/liability)."""
    # Ganho por unidade de responsabilidade = 1/(odd-1) * (1-commission)
    b_net = (1.0 / (odd - 1.0)) * (1.0 - COMMISSION)
    q = 1.0 - p
    kf = p - q / b_net
    return max(0.0, kf)


def simular_kelly(df_sim: pd.DataFrame, kelly_frac: float = 0.25, p_global: float = 0.978) -> tuple[dict, list]:
    banca = BANCA_INICIAL
    equity = [banca]
    pnls: list[float] = []
    for _, row in df_sim.iterrows():
        odd = _to_float(row.get("Odd_Base"))
        res = int(row["1/0"])
        if odd is None or odd <= 1.0:
            pnls.append(0.0)
            continue
        kf_full = _kelly_fraction(odd, p_global)
        # responsabilidade (liability) = fracao da banca atual
        stake = max(1.0, banca * kelly_frac * kf_full)
        if res == 1:
            pnl = stake / (odd - 1.0) * (1.0 - COMMISSION)
        else:
            pnl = -stake
        banca += pnl
        equity.append(banca)
        pnls.append(pnl)
    dd_abs, dd_pct = _current_dd(equity)
    wins = sum(1 for p in pnls if p > 0)
    losses = sum(1 for p in pnls if p < 0)
    total = wins + losses
    wr = wins / total * 100 if total > 0 else 0.0
    lucro_final = banca - BANCA_INICIAL
    score = abs(lucro_final / dd_abs) if dd_abs > 0 else float("inf")
    return {
        "Lucro_Final": lucro_final,
        "Win_Rate_Executadas_%": wr,
        "Max_Drawdown_Abs": dd_abs,
        "Max_Drawdown_%": dd_pct,
        "Entradas_Executadas": total,
        "Score": score,
    }, pnls


summ_c, pnls_c = simular_fixo(df_bl, stake=500.0)
summ_d, pnls_d = simular_kelly(df_bl, kelly_frac=0.25)
summ_e, pnls_e = simular_kelly(df_bl, kelly_frac=0.125)

# ---------------------------------------------------------------------------
# Impressao
# ---------------------------------------------------------------------------

def print_summ(label: str, s: dict[str, Any]) -> None:
    print(f"  Entradas   : {s.get('Entradas_Executadas', '-')}")
    print(f"  Win Rate   : {s.get('Win_Rate_Executadas_%', 0):.1f}%")
    print(f"  Lucro Final: R$ {s.get('Lucro_Final', 0):+,.2f}")
    print(f"  DD Max     : R$ {s.get('Max_Drawdown_Abs', 0):,.2f}  ({s.get('Max_Drawdown_%', 0):.2f}%)")
    print(f"  Score L/DD : {s.get('Score', 0):.2f}")
    if "Step_Ups" in s:
        print(f"  Step-ups   : {s['Step_Ups']}")


def _enrich_summ(s: dict) -> dict:
    if "Score" not in s:
        dd = s.get("Max_Drawdown_Abs", 1) or 1
        s["Score"] = abs(s.get("Lucro_Final", 0) / dd)
    return s


summ_a = _enrich_summ(summ_a)
summ_b = _enrich_summ(summ_b)

print("=" * 65)
print("A) RAMPA ARKAD_PROD  (base=500, teto=2000, warmup 30/60/100%)")
print("=" * 65)
print_summ("Rampa", summ_a)

print()
print("=" * 65)
print("B) COMPOUND PURO     (base=500, teto=2000, sem warmup)")
print("=" * 65)
print_summ("Compound Puro", summ_b)

print()
print("=" * 65)
print("C) STAKE FIXO R$500  (sem compound, sem rampa)")
print("=" * 65)
print_summ("Fixo", summ_c)

print()
print("=" * 65)
print("D) KELLY 25%         (banca R$1.200, p=97.8%, frac=0.25)")
print("   Kelly full medio: {:.1f}% da banca por aposta".format(
    _kelly_fraction(9.0, 0.978) * 25  # odd media ~9 como referencia
))
print("=" * 65)
print_summ("Kelly 25%", summ_d)

print()
print("=" * 65)
print("E) KELLY 12.5%       (banca R$1.200, p=97.8%, frac=0.125)")
print("=" * 65)
print_summ("Kelly 12.5%", summ_e)

# ---------------------------------------------------------------------------
# Comparativo mensal
# ---------------------------------------------------------------------------
print()
print("=" * 65)
print("LUCRO MENSAL COMPARATIVO")
print("=" * 65)

df_bl_m = df_bl.copy()
df_bl_m["__mes"] = df_bl_m["Data_Arquivo"].dt.to_period("M")
df_bl_m = df_bl_m[df_bl_m["Odd_Base"].notna()].copy().reset_index(drop=True)

df_bl_m["pnl_fixo"] = pnls_c[: len(df_bl_m)]
df_bl_m["pnl_k25"] = pnls_d[: len(df_bl_m)]
df_bl_m["pnl_k125"] = pnls_e[: len(df_bl_m)]

# PnL mensal da rampa (usa df_out_a que tem __date e PnL_Linha)
df_out_a["__mes"] = pd.to_datetime(df_out_a["__date"], errors="coerce").dt.to_period("M")
df_out_b["__mes"] = pd.to_datetime(df_out_b["__date"], errors="coerce").dt.to_period("M")
rampa_m = df_out_a.groupby("__mes")["PnL_Linha"].sum().rename("A_Rampa")
comp_m = df_out_b.groupby("__mes")["PnL_Linha"].sum().rename("B_Compound")

mensal = pd.concat([
    rampa_m,
    comp_m,
    df_bl_m.groupby("__mes")["pnl_fixo"].sum().rename("C_Fixo_500"),
    df_bl_m.groupby("__mes")["pnl_k25"].sum().rename("D_Kelly_25pct"),
    df_bl_m.groupby("__mes")["pnl_k125"].sum().rename("E_Kelly_125pct"),
], axis=1).fillna(0).round(2)

mensal["Melhor"] = mensal.idxmax(axis=1)
pd.set_option("display.width", 140)
print(mensal.to_string())

# ---------------------------------------------------------------------------
# Resumo final comparativo
# ---------------------------------------------------------------------------
print()
print("=" * 65)
print("RESUMO FINAL — ordenado por Score L/DD")
print("=" * 65)
rows = [
    ("A) Rampa ARKAD_PROD ", summ_a),
    ("B) Compound Puro    ", summ_b),
    ("C) Stake Fixo R$500 ", summ_c),
    ("D) Kelly 25%        ", summ_d),
    ("E) Kelly 12.5%      ", summ_e),
]
rows_sorted = sorted(rows, key=lambda x: x[1].get("Score", 0), reverse=True)
print(f"  {'Cenario':<22} {'Lucro':>12}  {'WR':>6}  {'DD Abs':>10}  {'DD%':>6}  {'Score':>7}  {'Entradas':>8}")
print("  " + "-" * 80)
for label, s in rows_sorted:
    print(
        f"  {label:<22} R$ {s['Lucro_Final']:>+9,.0f}  "
        f"{s['Win_Rate_Executadas_%']:>5.1f}%  "
        f"R$ {s['Max_Drawdown_Abs']:>7,.0f}  "
        f"{s['Max_Drawdown_%']:>5.1f}%  "
        f"{s['Score']:>7.2f}  "
        f"{s['Entradas_Executadas']:>8}"
    )
