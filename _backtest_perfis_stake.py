from __future__ import annotations

import copy
import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from engine_ciclo_producao import apply_config_filters, load_config, prepare_dataframe, _run_cycle_no_monitor

ROOT = Path(__file__).resolve().parent
CSV_PATH = ROOT / "Arquivados_Apostas_Diarias/Relatorios/WalkForward/pacote_reproducao_todos_jogos_20250801_20260414.csv"
CFG_PATH = ROOT / "config_backtest_exec.json"
OUT_DIR = ROOT / "Arquivados_Apostas_Diarias/Relatorios/Comparativo_Automatizado"


def _run_profile(df_raw: pd.DataFrame, cfg_base: dict, profile_mode: str | None) -> dict[str, float | int | str]:
    cfg = copy.deepcopy(cfg_base)
    if profile_mode is None:
        cfg.pop("profile_mode", None)
        mode_name = "baseline_legacy"
    else:
        cfg["profile_mode"] = profile_mode
        mode_name = profile_mode

    df_prep = prepare_dataframe(df_raw.copy(), cfg, environment="historico")
    df_f = apply_config_filters(df_prep, cfg)
    ops, summary = _run_cycle_no_monitor(df_f, cfg, environment="historico")

    stake_series = pd.to_numeric(ops.get("Stake_Final_Aplicada"), errors="coerce").fillna(0.0)
    stake_total = float(stake_series.sum())
    lucro = float(summary.get("Lucro_Final", 0.0))
    roi_pct = (lucro / stake_total * 100.0) if stake_total > 0 else 0.0

    return {
        "Profile_Mode": mode_name,
        "PnL_Total": round(lucro, 2),
        "ROI_%": round(roi_pct, 4),
        "DD_Max": round(float(summary.get("Max_Drawdown_Abs", 0.0)), 2),
        "Total_Operacoes": int(summary.get("Entradas_Executadas", 0)),
        "Stake_Total_Aplicada": round(stake_total, 2),
        "Win_Rate_%": round(float(summary.get("Win_Rate_Executadas_%", 0.0)), 2),
    }


def main() -> None:
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV nao encontrado: {CSV_PATH}")
    if not CFG_PATH.exists():
        raise FileNotFoundError(f"Config nao encontrada: {CFG_PATH}")

    df_raw = pd.read_csv(CSV_PATH)
    cfg_base = load_config(CFG_PATH)

    rows = [
        _run_profile(df_raw, cfg_base, None),
        _run_profile(df_raw, cfg_base, "conservador"),
        _run_profile(df_raw, cfg_base, "moderado"),
        _run_profile(df_raw, cfg_base, "agressivo"),
    ]

    out_df = pd.DataFrame(rows)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_csv = OUT_DIR / f"comparativo_perfis_stake_{ts}.csv"
    out_df.to_csv(out_csv, index=False, encoding="utf-8-sig")

    print("=== COMPARATIVO PERFIS DE STAKE ===")
    print(out_df.to_string(index=False))
    print(f"\nCSV: {out_csv}")


if __name__ == "__main__":
    main()
