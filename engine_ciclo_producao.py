from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

try:
    import matplotlib.pyplot as plt
    _MPL_AVAILABLE = True
except ImportError:
    plt = None  # type: ignore[assignment]
    _MPL_AVAILABLE = False


def _to_float(v: Any) -> float | None:
    try:
        if pd.isna(v):
            return None
        return float(v)
    except Exception:
        return None


def load_config(config_path: Path) -> dict[str, Any]:
    with config_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _parse_time_to_seconds(v: Any) -> int | None:
    if v is None or pd.isna(v):
        return None
    s = str(v).strip()
    if not s:
        return None

    if "T" in s:
        s = s.split("T")[-1]
    if " " in s:
        s = s.split(" ")[-1]

    chunks = s.split(":")
    if len(chunks) == 2:
        h, m = chunks
        sec = "0"
    elif len(chunks) == 3:
        h, m, sec = chunks
    else:
        return None

    try:
        hh = int(float(h))
        mm = int(float(m))
        ss = int(float(sec))
    except Exception:
        return None

    if not (0 <= hh <= 23 and 0 <= mm <= 59 and 0 <= ss <= 59):
        return None
    return hh * 3600 + mm * 60 + ss


def prepare_dataframe(df: pd.DataFrame, cfg: dict[str, Any], environment: str = "producao") -> pd.DataFrame:
    col_cfg = cfg["input"]["columns"]
    dt_cfg = cfg["input"]["datetime"]

    date_col = dt_cfg["date_col"]
    time_col = dt_cfg["time_col"]
    odd_signal_col = col_cfg["odd_signal_col"]
    result_col = col_cfg["result_col"]
    odd_exec_col = col_cfg.get("odd_exec_col")
    odd_exec_candidates = list(col_cfg.get("odd_exec_col_candidates", []))
    liquidity_col = col_cfg.get("liquidity_col")
    liquidity_candidates = list(col_cfg.get("liquidity_col_candidates", []))

    env = str(environment).strip().lower()
    if env not in {"historico", "producao"}:
        raise ValueError("environment invalido. Use 'historico' ou 'producao'.")

    if env == "historico":
        resolved_odd_exec_col = odd_signal_col
    else:
        candidates = [odd_exec_col] + odd_exec_candidates + ["Odd_Execucao", "Odd_Exec", "Preco_Execucao_API"]
        resolved_odd_exec_col = next((c for c in candidates if c and c in df.columns), odd_exec_col)

    liq_candidates = [liquidity_col] + liquidity_candidates + ["Liquidez_Disponivel", "Liquidity_Matched"]
    resolved_liquidity_col = next((c for c in liq_candidates if c and c in df.columns), liquidity_col)

    df = df.dropna(subset=[date_col, time_col, odd_signal_col, result_col]).copy()
    df[odd_signal_col] = pd.to_numeric(df[odd_signal_col], errors="coerce")
    df[result_col] = pd.to_numeric(df[result_col], errors="coerce")
    df = df[df[result_col].isin([0, 1])].copy()

    df["__odd_signal"] = pd.to_numeric(df[odd_signal_col], errors="coerce")
    df["__result"] = pd.to_numeric(df[result_col], errors="coerce")
    if resolved_odd_exec_col and resolved_odd_exec_col in df.columns:
        df["__odd_exec"] = pd.to_numeric(df[resolved_odd_exec_col], errors="coerce")
    else:
        df["__odd_exec"] = pd.NA

    if resolved_liquidity_col and resolved_liquidity_col in df.columns:
        df["__liquidity"] = pd.to_numeric(df[resolved_liquidity_col], errors="coerce")
    else:
        df["__liquidity"] = pd.NA

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df["__date"] = df[date_col]
    df["__time_seconds"] = df[time_col].apply(_parse_time_to_seconds)
    df["__time_seconds"] = pd.to_numeric(df["__time_seconds"], errors="coerce").fillna(23 * 3600 + 59 * 60 + 59)

    df = df.dropna(subset=["__date", "__odd_signal", "__result"]).copy()
    df = df[df["__result"].isin([0, 1])].copy()
    df = df.sort_values(["__date", "__time_seconds"], ascending=[True, True]).reset_index(drop=True)
    df = df.drop(columns=["__time_seconds"])
    return df


def apply_config_filters(df: pd.DataFrame, cfg: dict[str, Any]) -> pd.DataFrame:
    col_cfg = cfg["input"]["columns"]
    league_col = col_cfg["league_col"]
    method_col = col_cfg.get("method_col", "Metodo")
    odd_signal_col = col_cfg["odd_signal_col"]

    filters = cfg["filters"]
    exclude_leagues = set(filters.get("exclude_leagues", []))
    cond_rules = filters.get("conditional_rules", [])

    league = df[league_col].astype(str)
    odd_signal = pd.to_numeric(df[odd_signal_col], errors="coerce")

    cond_remove = league.isin(exclude_leagues)
    for rule in cond_rules:
        leagues = set(rule.get("leagues", []))
        odd_gt = rule.get("odd_gt")
        if odd_gt is not None:
            cond_remove = cond_remove | (league.isin(leagues) & (odd_signal > float(odd_gt)))

    # Recortes toxicos por combinacao (Liga + Metodo + Faixa de Odd).
    if method_col in df.columns:
        method = df[method_col].astype(str)
    else:
        method = pd.Series([""] * len(df), index=df.index, dtype=str)

    toxic_cuts = filters.get("toxic_cuts", []) or filters.get("filtros_rodo", [])
    for cut in toxic_cuts:
        cut_leagues = set(cut.get("leagues", []))
        if cut.get("league"):
            cut_leagues.add(str(cut.get("league")))

        mask_cut = pd.Series(True, index=df.index)
        if cut_leagues:
            mask_cut = mask_cut & league.isin(cut_leagues)

        method_contains = cut.get("method_contains")
        method_equals = cut.get("method_equals")
        if method_contains:
            mask_cut = mask_cut & method.str.contains(str(method_contains), na=False)
        if method_equals:
            mask_cut = mask_cut & method.eq(str(method_equals))

        odd_min = cut.get("odd_min")
        odd_max = cut.get("odd_max")
        if odd_min is not None:
            mask_cut = mask_cut & (odd_signal >= float(odd_min))
        if odd_max is not None:
            mask_cut = mask_cut & (odd_signal <= float(odd_max))

        cond_remove = cond_remove | mask_cut

    return df.loc[~cond_remove].copy().reset_index(drop=True)


def _ensure_sqlite(conn: sqlite3.Connection, cfg: dict[str, Any]) -> tuple[str, str]:
    mon = cfg["monitoring"]
    kpi_table = mon["sqlite_table_kpis"]
    ops_table = mon["sqlite_table_ops"]

    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {kpi_table} (
            run_id TEXT,
            ts TEXT,
            processed_rows INTEGER,
            executed_rows INTEGER,
            skipped_rows INTEGER,
            wins INTEGER,
            losses INTEGER,
            win_rate REAL,
            lucro_acumulado REAL,
            drawdown_abs REAL,
            drawdown_pct REAL,
            current_base REAL,
            current_run REAL,
            level_profit REAL,
            step_ups INTEGER,
            step_downs INTEGER,
            saques_realizados INTEGER
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {ops_table} (
            run_id TEXT,
            ts TEXT,
            idx INTEGER,
            status TEXT,
            skip_reason TEXT,
            odd_signal REAL,
            odd_exec REAL,
            liquidity REAL,
            resultado INTEGER,
            pnl REAL,
            lucro_acumulado REAL,
            current_base REAL,
            current_run REAL,
            level_profit REAL
        )
        """
    )
    conn.commit()
    return kpi_table, ops_table


def _current_dd(equity: list[float]) -> tuple[float, float]:
    if not equity:
        return 0.0, 0.0
    s = pd.Series(equity, dtype=float)
    peak = s.cummax()
    dd = peak - s
    dd_abs = float(dd.max())
    peak_max = float(peak.max())
    dd_pct = float(dd_abs / peak_max * 100.0) if peak_max > 0 else 0.0
    return dd_abs, dd_pct


def _ramp_multiplier(day_number: int, phase2_profit: float, ramp_enabled: bool = True) -> tuple[float, str, bool]:
    if not ramp_enabled:
        return 1.00, "Sem_Rampa", True
    if day_number <= 7:
        return 0.30, "Fase_1_30pct", False
    if day_number <= 14:
        return 0.60, "Fase_2_60pct", False
    phase2_ok = phase2_profit > 0.0
    if phase2_ok:
        return 1.00, "Fase_3_100pct", True
    return 0.60, "Fase_3_Bloqueada_60pct", False


def _run_cycle_no_monitor(df: pd.DataFrame, cfg: dict[str, Any], environment: str = "producao") -> tuple[pd.DataFrame, dict[str, Any]]:
    col_cfg = cfg["input"]["columns"]
    dt_cfg = cfg["input"]["datetime"]
    cycle = cfg["cycle"]
    guards = cfg["execution_guards"]

    date_col = "__date" if "__date" in df.columns else dt_cfg["date_col"]
    odd_signal_col = col_cfg["odd_signal_col"]
    result_col = col_cfg["result_col"]
    odd_exec_col = col_cfg.get("odd_exec_col")
    liquidity_col = col_cfg.get("liquidity_col")

    commission_rate = float(cycle.get("commission_rate", cfg.get("commission_rate", 0.065)))
    initial_base = float(cycle.get("initial_base", cfg.get("base_stake", 500.0)))
    teto = float(cycle.get("teto", cfg.get("teto_stake", 2000.0)))
    compound_limit = float(cycle.get("compound_limit_multiplier", 2.0))
    step_up_mult = float(cycle.get("step_up_target_multiplier", 4.0))
    step_down_mult = float(cycle.get("step_down_limit_multiplier", -2.0))
    ramp_cfg = cycle.get("ramp_transition", {})
    ramp_enabled = bool(ramp_cfg.get("enabled", cfg.get("enable_rampa", True)))

    sl_cfg = guards.get("slippage", {})
    sl_enabled = bool(sl_cfg.get("enabled", cfg.get("enable_slippage_protection", True)))
    sl_tick_size = float(sl_cfg.get("odd_tick_size", 0.1))
    sl_max_ticks = float(sl_cfg.get("max_slippage_ticks", cfg.get("max_ticks", 0.0)))
    sl_max_delta = float(sl_cfg.get("max_delta_odd", sl_max_ticks * sl_tick_size))
    sl_skip_missing_exec = bool(sl_cfg.get("skip_if_exec_odd_missing", cfg.get("skip_if_exec_odd_missing", True)))
    if str(environment).strip().lower() == "producao":
        sl_skip_missing_exec = True

    liq_cfg = guards.get("liquidity", {})
    liq_enabled = bool(liq_cfg.get("enabled", cfg.get("enable_liquidity_filter", True)))
    liq_min_abs = float(liq_cfg.get("min_matched_liquidity", cfg.get("min_volume", 0.0)))
    liq_mult = float(liq_cfg.get("required_multiplier_of_run", 1.0))
    liq_skip_missing = bool(liq_cfg.get("skip_if_liquidity_missing", True))

    dts = pd.to_datetime(df[date_col], errors="coerce").dt.date
    day_map: dict[Any, int] = {}
    day_numbers: list[int] = []
    next_day = 1
    for d in dts:
        key = d if pd.notna(d) else "NA"
        if key not in day_map:
            day_map[key] = next_day
            next_day += 1
        day_numbers.append(day_map[key])

    current_base = initial_base
    current_run = initial_base
    level_profit = 0.0
    total_profit = 0.0
    phase2_profit = 0.0

    wins = 0
    losses = 0
    executed_rows = 0
    skipped_rows = 0
    step_ups = 0
    step_downs = 0
    saques_realizados = 0

    status_list: list[str] = []
    skip_reason_list: list[str] = []
    pnl_list: list[float] = []
    lucro_list: list[float] = []
    base_list: list[float] = []
    run_list: list[float] = []
    level_list: list[float] = []
    ramp_mult_list: list[float] = []
    ramp_phase_list: list[str] = []

    for idx, row in df.iterrows():
        day_number = day_numbers[idx]
        stake_mult, stake_phase, _ = _ramp_multiplier(day_number, phase2_profit, ramp_enabled=ramp_enabled)

        odd_signal = _to_float(row.get("__odd_signal", row.get(odd_signal_col)))
        odd_exec = _to_float(row.get("__odd_exec")) if "__odd_exec" in df.columns else (_to_float(row.get(odd_exec_col)) if odd_exec_col in df.columns else None)
        liquidity = _to_float(row.get("__liquidity")) if "__liquidity" in df.columns else (_to_float(row.get(liquidity_col)) if liquidity_col in df.columns else None)
        resultado = int(row.get("__result", row[result_col]))

        allowed = True
        reason = ""

        if odd_signal is None:
            allowed = False
            reason = "odd_signal_missing"

        if allowed and sl_enabled:
            if odd_exec is None:
                if sl_skip_missing_exec:
                    allowed = False
                    reason = "odd_exec_missing"
                else:
                    odd_exec = odd_signal
            if allowed and (odd_exec > odd_signal + sl_max_delta):
                allowed = False
                reason = "slippage_exceeded"

            # NOVO: Filtro de segurança absoluto na Odd de Execução (espelha main.py)
            if allowed:
                filtros_metodo = cfg.get("runtime_data", {}).get("filtros_metodo", {})
                m = str(row.get(method_col, ""))
                flt = filtros_metodo.get(m)
                if flt:
                    omx = flt.get("odd_max")
                    if omx is not None and float(odd_exec) > float(omx):
                        allowed = False
                        reason = f"odd_exec_too_high_{odd_exec}"

        effective_run = current_run * stake_mult
        if allowed and liq_enabled:
            if liquidity is None:
                if liq_skip_missing:
                    allowed = False
                    reason = "liquidity_missing"
            else:
                required_liquidity = max(effective_run * liq_mult, liq_min_abs)
                if liquidity < required_liquidity:
                    allowed = False
                    reason = "liquidity_insufficient"

        pnl = 0.0
        if allowed:
            odd_use = odd_exec if odd_exec is not None else odd_signal
            if odd_use is None or odd_use <= 1.0:
                allowed = False
                reason = "odd_invalid"

        if allowed:
            if resultado == 1:
                lay_stake = effective_run / (odd_use - 1.0)
                net_profit = lay_stake * (1.0 - commission_rate)
                total_profit += net_profit
                level_profit += net_profit
                current_run += net_profit
                pnl = net_profit
                wins += 1
                if current_run >= (compound_limit * current_base):
                    current_run = current_base
            else:
                total_profit -= effective_run
                level_profit -= effective_run
                pnl = -effective_run
                current_run = current_base
                losses += 1

            if ramp_enabled and day_number >= 8 and day_number <= 14:
                phase2_profit += pnl

            if level_profit >= (step_up_mult * current_base):
                level_profit -= (step_up_mult * current_base)
                if current_base < teto:
                    current_base *= 2.0
                    step_ups += 1
                else:
                    current_base = initial_base
                    level_profit = 0.0
                    saques_realizados += 1
                current_run = current_base

            if (level_profit <= (step_down_mult * current_base)) and (current_base > initial_base):
                current_base /= 2.0
                level_profit = 0.0
                current_run = current_base
                step_downs += 1

            status = "EXECUTED"
            skip_reason = ""
            executed_rows += 1
        else:
            status = "SKIPPED"
            skip_reason = reason
            skipped_rows += 1

        status_list.append(status)
        skip_reason_list.append(skip_reason)
        pnl_list.append(pnl)
        lucro_list.append(total_profit)
        base_list.append(current_base)
        run_list.append(current_run)
        level_list.append(level_profit)
        ramp_mult_list.append(stake_mult)
        ramp_phase_list.append(stake_phase)

    out = df.copy()
    out["Status_Execucao"] = status_list
    out["Skip_Reason"] = skip_reason_list
    out["PnL_Linha"] = pnl_list
    out["Lucro_Acumulado"] = lucro_list
    out["Base_Atual"] = base_list
    out["Run_Atual"] = run_list
    out["Level_Profit_Atual"] = level_list
    out["Stake_Multiplier"] = ramp_mult_list
    out["Stake_Phase"] = ramp_phase_list

    dd_abs, dd_pct = _current_dd(lucro_list)
    win_rate = (wins / executed_rows * 100.0) if executed_rows > 0 else 0.0
    summary = {
        "Total_Linhas_Filtradas": int(len(out)),
        "Entradas_Executadas": executed_rows,
        "Entradas_Skipadas": skipped_rows,
        "Win_Rate_Executadas_%": round(win_rate, 2),
        "Lucro_Final": round(total_profit, 2),
        "Max_Drawdown_Abs": round(dd_abs, 2),
        "Max_Drawdown_%": round(dd_pct, 2),
        "Step_Ups": step_ups,
        "Saques_Realizados": saques_realizados,
        "Step_Downs": step_downs,
        "Fase2_Lucro": round(phase2_profit, 2),
        "Fase2_KPI_Positivo": bool(phase2_profit > 0.0),
    }
    return out, summary


def generate_daily_mini_report(prepared_df: pd.DataFrame, cfg: dict[str, Any], output_dir: Path, run_id: str, environment: str = "producao") -> Path:
    date_col = cfg["input"]["datetime"]["date_col"]
    result_col = cfg["input"]["columns"]["result_col"]

    filtered_df = apply_config_filters(prepared_df, cfg)
    filtered_ops, filtered_summary = _run_cycle_no_monitor(filtered_df, cfg, environment=environment)
    baseline_ops, baseline_summary = _run_cycle_no_monitor(prepared_df, cfg, environment=environment)

    date_baseline = pd.to_datetime(baseline_ops[date_col], errors="coerce")
    last_day = date_baseline.dt.normalize().max()
    if pd.isna(last_day):
        base_day = baseline_ops.copy()
        filt_day = filtered_ops.copy()
    else:
        base_day = baseline_ops[date_baseline.dt.normalize() == last_day].copy()
        date_filtered = pd.to_datetime(filtered_ops[date_col], errors="coerce")
        filt_day = filtered_ops[date_filtered.dt.normalize() == last_day].copy()

    def _row(name: str, day_df: pd.DataFrame, ops_df: pd.DataFrame, summary: dict[str, Any]) -> dict[str, Any]:
        entradas = int((day_df["Status_Execucao"] == "EXECUTED").sum())
        wins = int(((pd.to_numeric(day_df[result_col], errors="coerce") == 1) & (day_df["Status_Execucao"] == "EXECUTED")).sum())
        win_rate = round((wins / entradas * 100.0), 2) if entradas > 0 else 0.0
        lucro_dia = round(float(pd.to_numeric(day_df["PnL_Linha"], errors="coerce").fillna(0.0).sum()), 2)
        lucro_total = round(float(pd.to_numeric(ops_df["Lucro_Acumulado"], errors="coerce").iloc[-1]), 2) if not ops_df.empty else 0.0
        dd_corrente = round(float((pd.to_numeric(ops_df["Lucro_Acumulado"], errors="coerce").cummax() - pd.to_numeric(ops_df["Lucro_Acumulado"], errors="coerce")).max()), 2) if not ops_df.empty else 0.0
        return {
            "Estrategia": name,
            "Entradas_Dia": entradas,
            "WinRate_Dia_%": win_rate,
            "Lucro_Dia": lucro_dia,
            "Lucro_Acumulado": lucro_total,
            "Drawdown_Corrente": dd_corrente,
            "Step_Ups": summary.get("Step_Ups", 0),
            "Step_Downs": summary.get("Step_Downs", 0),
            "Saques": summary.get("Saques_Realizados", 0),
        }

    mini_df = pd.DataFrame(
        [
            _row("Baseline_Sem_Filtro", base_day, baseline_ops, baseline_summary),
            _row("Atual_Filtrada", filt_day, filtered_ops, filtered_summary),
        ]
    )
    mini_df["Delta_vs_Baseline_Lucro_Dia"] = mini_df["Lucro_Dia"] - float(mini_df.loc[mini_df["Estrategia"] == "Baseline_Sem_Filtro", "Lucro_Dia"].iloc[0])
    mini_df["Delta_vs_Baseline_WinRate_Dia"] = mini_df["WinRate_Dia_%"] - float(mini_df.loc[mini_df["Estrategia"] == "Baseline_Sem_Filtro", "WinRate_Dia_%"].iloc[0])

    out_xlsx = output_dir / f"{run_id}_mini_relatorio_diario.xlsx"
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        mini_df.to_excel(writer, sheet_name="Mini_Relatorio", index=False)
        baseline_ops.to_excel(writer, sheet_name="Baseline_Ops", index=False)
        filtered_ops.to_excel(writer, sheet_name="Filtrada_Ops", index=False)

    return out_xlsx


def run_engine(df: pd.DataFrame, cfg: dict[str, Any], output_dir: Path, run_id: str, environment: str = "producao") -> tuple[pd.DataFrame, dict[str, Any]]:
    col_cfg = cfg["input"]["columns"]
    dt_cfg = cfg["input"]["datetime"]
    cycle = cfg["cycle"]
    guards = cfg["execution_guards"]
    mon = cfg["monitoring"]

    date_col = "__date" if "__date" in df.columns else dt_cfg["date_col"]
    odd_signal_col = col_cfg["odd_signal_col"]
    result_col = col_cfg["result_col"]
    odd_exec_col = col_cfg.get("odd_exec_col")
    liquidity_col = col_cfg.get("liquidity_col")

    commission_rate = float(cycle.get("commission_rate", cfg.get("commission_rate", 0.065)))
    initial_base = float(cycle.get("initial_base", cfg.get("base_stake", 500.0)))
    teto = float(cycle.get("teto", cfg.get("teto_stake", 2000.0)))
    compound_limit = float(cycle.get("compound_limit_multiplier", 2.0))
    step_up_mult = float(cycle.get("step_up_target_multiplier", 4.0))
    step_down_mult = float(cycle.get("step_down_limit_multiplier", -2.0))
    ramp_cfg = cycle.get("ramp_transition", {})
    ramp_enabled = bool(ramp_cfg.get("enabled", cfg.get("enable_rampa", True)))

    sl_cfg = guards.get("slippage", {})
    sl_enabled = bool(sl_cfg.get("enabled", cfg.get("enable_slippage_protection", True)))
    sl_tick_size = float(sl_cfg.get("odd_tick_size", 0.1))
    sl_max_ticks = float(sl_cfg.get("max_slippage_ticks", cfg.get("max_ticks", 0.0)))
    sl_max_delta = float(sl_cfg.get("max_delta_odd", sl_max_ticks * sl_tick_size))
    sl_skip_missing_exec = bool(sl_cfg.get("skip_if_exec_odd_missing", cfg.get("skip_if_exec_odd_missing", True)))
    if str(environment).strip().lower() == "producao":
        sl_skip_missing_exec = True

    liq_cfg = guards.get("liquidity", {})
    liq_enabled = bool(liq_cfg.get("enabled", cfg.get("enable_liquidity_filter", True)))
    liq_min_abs = float(liq_cfg.get("min_matched_liquidity", cfg.get("min_volume", 0.0)))
    liq_mult = float(liq_cfg.get("required_multiplier_of_run", 1.0))
    liq_skip_missing = bool(liq_cfg.get("skip_if_liquidity_missing", True))

    kpi_json_path = output_dir / mon["kpi_json_filename"]
    sqlite_path = output_dir / mon["sqlite_filename"]

    conn = sqlite3.connect(sqlite_path)
    kpi_table, ops_table = _ensure_sqlite(conn, cfg)

    current_base = initial_base
    current_run = initial_base
    level_profit = 0.0
    total_profit = 0.0
    phase2_profit = 0.0

    wins = 0
    losses = 0
    executed_rows = 0
    skipped_rows = 0
    step_ups = 0
    step_downs = 0
    saques_realizados = 0

    status_list: list[str] = []
    skip_reason_list: list[str] = []
    pnl_list: list[float] = []
    lucro_list: list[float] = []
    base_list: list[float] = []
    run_list: list[float] = []
    level_list: list[float] = []
    ramp_mult_list: list[float] = []
    ramp_phase_list: list[str] = []

    dts = pd.to_datetime(df[date_col], errors="coerce").dt.date
    day_map: dict[Any, int] = {}
    day_numbers: list[int] = []
    next_day = 1
    for d in dts:
        key = d if pd.notna(d) else "NA"
        if key not in day_map:
            day_map[key] = next_day
            next_day += 1
        day_numbers.append(day_map[key])

    for idx, row in df.iterrows():
        day_number = day_numbers[idx]
        stake_mult, stake_phase, phase2_kpi_positive = _ramp_multiplier(day_number, phase2_profit, ramp_enabled=ramp_enabled)

        odd_signal = _to_float(row.get("__odd_signal", row.get(odd_signal_col)))
        odd_exec = _to_float(row.get("__odd_exec")) if "__odd_exec" in df.columns else (_to_float(row.get(odd_exec_col)) if odd_exec_col in df.columns else None)
        liquidity = _to_float(row.get("__liquidity")) if "__liquidity" in df.columns else (_to_float(row.get(liquidity_col)) if liquidity_col in df.columns else None)
        resultado = int(row.get("__result", row[result_col]))

        allowed = True
        reason = ""

        if odd_signal is None:
            allowed = False
            reason = "odd_signal_missing"

        if allowed and sl_enabled:
            if odd_exec is None:
                if sl_skip_missing_exec:
                    allowed = False
                    reason = "odd_exec_missing"
                else:
                    odd_exec = odd_signal
            if allowed and (odd_exec > odd_signal + sl_max_delta):
                allowed = False
                reason = "slippage_exceeded"

        effective_run = current_run * stake_mult
        if allowed and liq_enabled:
            if liquidity is None:
                if liq_skip_missing:
                    allowed = False
                    reason = "liquidity_missing"
            else:
                required_liquidity = max(effective_run * liq_mult, liq_min_abs)
                if liquidity < required_liquidity:
                    allowed = False
                    reason = "liquidity_insufficient"

        pnl = 0.0
        if allowed:
            odd_use = odd_exec if odd_exec is not None else odd_signal
            if odd_use is None or odd_use <= 1.0:
                allowed = False
                reason = "odd_invalid"

        if allowed:
            if resultado == 1:
                lay_stake = effective_run / (odd_use - 1.0)
                net_profit = lay_stake * (1.0 - commission_rate)
                total_profit += net_profit
                level_profit += net_profit
                current_run += net_profit
                pnl = net_profit
                wins += 1
                if current_run >= (compound_limit * current_base):
                    current_run = current_base
            else:
                total_profit -= effective_run
                level_profit -= effective_run
                pnl = -effective_run
                current_run = current_base
                losses += 1

            if ramp_enabled and day_number >= 8 and day_number <= 14:
                phase2_profit += pnl

            if level_profit >= (step_up_mult * current_base):
                level_profit -= (step_up_mult * current_base)
                if current_base < teto:
                    current_base *= 2.0
                    step_ups += 1
                else:
                    current_base = initial_base
                    level_profit = 0.0
                    saques_realizados += 1
                current_run = current_base

            if (level_profit <= (step_down_mult * current_base)) and (current_base > initial_base):
                current_base /= 2.0
                level_profit = 0.0
                current_run = current_base
                step_downs += 1

            status = "EXECUTED"
            skip_reason = ""
            executed_rows += 1
        else:
            status = "SKIPPED"
            skip_reason = reason
            skipped_rows += 1

        status_list.append(status)
        skip_reason_list.append(skip_reason)
        pnl_list.append(pnl)
        lucro_list.append(total_profit)
        base_list.append(current_base)
        run_list.append(current_run)
        level_list.append(level_profit)
        ramp_mult_list.append(stake_mult)
        ramp_phase_list.append(stake_phase)

        dd_abs, dd_pct = _current_dd(lucro_list)
        processed_rows = idx + 1
        win_rate = (wins / executed_rows * 100.0) if executed_rows > 0 else 0.0

        kpi_payload = {
            "run_id": run_id,
            "ts": datetime.utcnow().isoformat(),
            "processed_rows": processed_rows,
            "executed_rows": executed_rows,
            "skipped_rows": skipped_rows,
            "wins": wins,
            "losses": losses,
            "win_rate": round(win_rate, 2),
            "lucro_acumulado": round(total_profit, 2),
            "drawdown_abs": round(dd_abs, 2),
            "drawdown_pct": round(dd_pct, 2),
            "current_base": round(current_base, 2),
            "current_run": round(current_run, 2),
            "level_profit": round(level_profit, 2),
            "stake_multiplier": round(stake_mult, 4),
            "stake_phase": stake_phase,
            "phase2_kpi_positive": bool(phase2_kpi_positive),
            "step_ups": step_ups,
            "step_downs": step_downs,
            "saques_realizados": saques_realizados,
        }

        kpi_json_path.write_text(json.dumps(kpi_payload, ensure_ascii=True, indent=2), encoding="utf-8")

        conn.execute(
            f"INSERT INTO {kpi_table} VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                kpi_payload["run_id"],
                kpi_payload["ts"],
                kpi_payload["processed_rows"],
                kpi_payload["executed_rows"],
                kpi_payload["skipped_rows"],
                kpi_payload["wins"],
                kpi_payload["losses"],
                kpi_payload["win_rate"],
                kpi_payload["lucro_acumulado"],
                kpi_payload["drawdown_abs"],
                kpi_payload["drawdown_pct"],
                kpi_payload["current_base"],
                kpi_payload["current_run"],
                kpi_payload["level_profit"],
                kpi_payload["step_ups"],
                kpi_payload["step_downs"],
                kpi_payload["saques_realizados"],
            ),
        )

        conn.execute(
            f"INSERT INTO {ops_table} VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                run_id,
                datetime.utcnow().isoformat(),
                int(idx),
                status,
                skip_reason,
                odd_signal,
                odd_exec,
                liquidity,
                resultado,
                round(pnl, 2),
                round(total_profit, 2),
                round(current_base, 2),
                round(current_run, 2),
                round(level_profit, 2),
            ),
        )
        conn.commit()

    conn.close()

    out = df.copy()
    out["Status_Execucao"] = status_list
    out["Skip_Reason"] = skip_reason_list
    out["PnL_Linha"] = pnl_list
    out["Lucro_Acumulado"] = lucro_list
    out["Base_Atual"] = base_list
    out["Run_Atual"] = run_list
    out["Level_Profit_Atual"] = level_list
    out["Stake_Multiplier"] = ramp_mult_list
    out["Stake_Phase"] = ramp_phase_list

    dd_abs, dd_pct = _current_dd(lucro_list)
    win_rate = (wins / executed_rows * 100.0) if executed_rows > 0 else 0.0
    summary = {
        "Total_Linhas_Filtradas": int(len(out)),
        "Entradas_Executadas": executed_rows,
        "Entradas_Skipadas": skipped_rows,
        "Win_Rate_Executadas_%": round(win_rate, 2),
        "Lucro_Final": round(total_profit, 2),
        "Max_Drawdown_Abs": round(dd_abs, 2),
        "Max_Drawdown_%": round(dd_pct, 2),
        "Step_Ups": step_ups,
        "Saques_Realizados": saques_realizados,
        "Step_Downs": step_downs,
        "Fase2_Lucro": round(phase2_profit, 2),
        "Fase2_KPI_Positivo": bool(phase2_profit > 0.0),
    }
    return out, summary


def plot_equity(df: pd.DataFrame, png_path: Path) -> None:
    if not _MPL_AVAILABLE:
        return
    plt.figure(figsize=(14, 7))
    x = range(len(df))
    plt.plot(x, df["Lucro_Acumulado"], linewidth=2.0, color="#1f77b4", label="Lucro Acumulado")
    plt.axhline(0, color="#333333", linewidth=1.0, linestyle="--")
    plt.title("Evolucao do Lucro Acumulado - Engine Producao")
    plt.xlabel("Linhas processadas")
    plt.ylabel("Lucro acumulado")
    plt.grid(alpha=0.25)
    plt.legend()
    plt.tight_layout()
    plt.savefig(png_path, dpi=150)
    plt.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Engine de producao para ciclo com filtros configuraveis")
    parser.add_argument("--input", required=True, help="CSV de entrada")
    parser.add_argument("--config", required=False, default="utilitarios/producao/config_ciclo_v1.json", help="JSON de configuracao")
    parser.add_argument("--output-dir", required=False, default="Arquivados_Apostas_Diarias/Relatorios/Producao_Ciclo", help="Diretorio de saida")
    parser.add_argument("--run-id", required=False, default=None, help="Identificador da execucao")
    parser.add_argument("--environment", required=False, default="producao", choices=["historico", "producao"], help="Ambiente de execucao")
    parser.add_argument("--skip-mini-report", action="store_true", help="Se informado, nao gera mini-relatorio diario baseline vs filtrada")
    args = parser.parse_args()

    input_path = Path(args.input)
    config_path = Path(args.config)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        raise FileNotFoundError(f"Arquivo de entrada nao encontrado: {input_path}")
    if not config_path.exists():
        raise FileNotFoundError(f"Arquivo de configuracao nao encontrado: {config_path}")

    run_id = args.run_id if args.run_id else datetime.utcnow().strftime("run_%Y%m%d_%H%M%S")

    cfg = load_config(config_path)
    raw_df = pd.read_csv(input_path)
    prepared_df = prepare_dataframe(raw_df, cfg, environment=args.environment)
    df = apply_config_filters(prepared_df, cfg)

    out_df, summary = run_engine(df, cfg, output_dir, run_id, environment=args.environment)

    out_csv = output_dir / f"{run_id}_ops.csv"
    out_png = output_dir / f"{run_id}_equity.png"
    out_summary = output_dir / f"{run_id}_summary.json"

    out_df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    plot_equity(out_df, out_png)
    out_summary.write_text(json.dumps(summary, ensure_ascii=True, indent=2), encoding="utf-8")

    mini_report_path = None
    if not args.skip_mini_report:
        mini_report_path = generate_daily_mini_report(prepared_df, cfg, output_dir, run_id, environment=args.environment)

    print("\n=== RESUMO FINAL (ENGINE PRODUCAO) ===")
    for k, v in summary.items():
        print(f"{k}: {v}")
    print(f"Run_ID: {run_id}")
    print(f"CSV: {out_csv}")
    print(f"PNG: {out_png}")
    print(f"Summary JSON: {out_summary}")
    print(f"KPI JSON em tempo real: {output_dir / cfg['monitoring']['kpi_json_filename']}")
    print(f"SQLite em tempo real: {output_dir / cfg['monitoring']['sqlite_filename']}")
    if mini_report_path is not None:
        print(f"Mini-relatorio diario: {mini_report_path}")


if __name__ == "__main__":
    main()
