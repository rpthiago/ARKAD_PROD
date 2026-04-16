from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from engine_ciclo_producao import (
    _run_cycle_no_monitor,
    apply_config_filters,
    load_config,
    prepare_dataframe,
)


def _profit_factor(ops_df: pd.DataFrame) -> float:
    if ops_df.empty or "Status_Execucao" not in ops_df.columns or "PnL_Linha" not in ops_df.columns:
        return 0.0
    executed = ops_df[ops_df["Status_Execucao"] == "EXECUTED"].copy()
    if executed.empty:
        return 0.0
    pnl = pd.to_numeric(executed["PnL_Linha"], errors="coerce").fillna(0.0)
    gross_win = float(pnl[pnl > 0].sum())
    gross_loss = float(-pnl[pnl < 0].sum())
    if gross_loss <= 0:
        return 999.0 if gross_win > 0 else 0.0
    return gross_win / gross_loss


def _summarize_fold(
    fold_id: int,
    strategy_name: str,
    segment: str,
    start_date: str,
    end_date: str,
    days_count: int,
    input_rows: int,
    filtered_rows: int,
    ops_df: pd.DataFrame,
    summary: dict[str, Any],
) -> dict[str, Any]:
    return {
        "Fold": fold_id,
        "Estrategia": strategy_name,
        "Segmento": segment,
        "Data_Inicio": start_date,
        "Data_Fim": end_date,
        "Dias": days_count,
        "Linhas_Entrada": int(input_rows),
        "Linhas_Apos_Filtro": int(filtered_rows),
        "Entradas_Executadas": int(summary.get("Entradas_Executadas", 0)),
        "Entradas_Skipadas": int(summary.get("Entradas_Skipadas", 0)),
        "Win_Rate_Executadas_%": float(summary.get("Win_Rate_Executadas_%", 0.0)),
        "Lucro_Final": float(summary.get("Lucro_Final", 0.0)),
        "Max_Drawdown_Abs": float(summary.get("Max_Drawdown_Abs", 0.0)),
        "Max_Drawdown_%": float(summary.get("Max_Drawdown_%", 0.0)),
        "Step_Ups": int(summary.get("Step_Ups", 0)),
        "Step_Downs": int(summary.get("Step_Downs", 0)),
        "Saques_Realizados": int(summary.get("Saques_Realizados", 0)),
        "Profit_Factor": round(_profit_factor(ops_df), 4),
    }


def _build_folds(unique_days: list[pd.Timestamp], train_days: int, test_days: int, step_days: int) -> list[tuple[int, list[pd.Timestamp], list[pd.Timestamp]]]:
    folds: list[tuple[int, list[pd.Timestamp], list[pd.Timestamp]]] = []
    i = 0
    fold_id = 1
    total = len(unique_days)
    while i + train_days + test_days <= total:
        train_block = unique_days[i : i + train_days]
        test_block = unique_days[i + train_days : i + train_days + test_days]
        folds.append((fold_id, train_block, test_block))
        fold_id += 1
        i += step_days
    return folds


def main() -> None:
    parser = argparse.ArgumentParser(description="Walk-forward backtest do metodo ARKAD")
    parser.add_argument("--input", required=True, help="CSV historico de entrada")
    parser.add_argument("--config", required=False, default="config_prod_v1.json", help="JSON de configuracao")
    parser.add_argument("--output-dir", required=False, default="Arquivados_Apostas_Diarias/Relatorios/WalkForward", help="Diretorio de saida")
    parser.add_argument("--run-id", required=False, default=None, help="Identificador da execucao")
    parser.add_argument("--environment", required=False, default="historico", choices=["historico", "producao"], help="Ambiente para simulacao")
    parser.add_argument("--train-days", type=int, default=90, help="Dias por janela de treino")
    parser.add_argument("--test-days", type=int, default=14, help="Dias por janela de teste")
    parser.add_argument("--step-days", type=int, default=14, help="Passo de avanço entre janelas")
    args = parser.parse_args()

    input_path = Path(args.input)
    config_path = Path(args.config)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        raise FileNotFoundError(f"Arquivo de entrada nao encontrado: {input_path}")
    if not config_path.exists():
        raise FileNotFoundError(f"Arquivo de configuracao nao encontrado: {config_path}")

    run_id = args.run_id if args.run_id else datetime.utcnow().strftime("wf_%Y%m%d_%H%M%S")

    cfg = load_config(config_path)
    raw_df = pd.read_csv(input_path)
    prepared_df = prepare_dataframe(raw_df, cfg, environment=args.environment)
    if prepared_df.empty:
        raise ValueError("Base preparada vazia. Verifique colunas e dados de entrada.")

    day_series = pd.to_datetime(prepared_df["__date"], errors="coerce").dt.normalize().dropna()
    unique_days = sorted(day_series.unique().tolist())
    folds = _build_folds(unique_days, args.train_days, args.test_days, args.step_days)
    if not folds:
        raise ValueError("Nao foi possivel montar janelas walk-forward com os parametros informados.")

    summary_rows: list[dict[str, Any]] = []
    ops_parts: list[pd.DataFrame] = []

    for fold_id, train_days_block, test_days_block in folds:
        for segment, block_days in (("treino", train_days_block), ("teste", test_days_block)):
            segment_df = prepared_df[pd.to_datetime(prepared_df["__date"], errors="coerce").dt.normalize().isin(block_days)].copy()
            segment_df = segment_df.sort_values("__date").reset_index(drop=True)
            if segment_df.empty:
                continue

            block_start = pd.Timestamp(block_days[0]).date().isoformat()
            block_end = pd.Timestamp(block_days[-1]).date().isoformat()

            strategies = [
                ("Baseline_Sem_Filtro", segment_df.copy()),
                ("Atual_Filtrada", apply_config_filters(segment_df.copy(), cfg)),
            ]

            for strategy_name, strategy_df in strategies:
                strategy_df = strategy_df.sort_values("__date").reset_index(drop=True)
                ops_df, summary = _run_cycle_no_monitor(strategy_df, cfg, environment=args.environment)
                summary_rows.append(
                    _summarize_fold(
                        fold_id=fold_id,
                        strategy_name=strategy_name,
                        segment=segment,
                        start_date=block_start,
                        end_date=block_end,
                        days_count=len(block_days),
                        input_rows=len(segment_df),
                        filtered_rows=len(strategy_df),
                        ops_df=ops_df,
                        summary=summary,
                    )
                )

                if not ops_df.empty:
                    tmp = ops_df.copy()
                    tmp["Fold"] = fold_id
                    tmp["Segmento"] = segment
                    tmp["Estrategia"] = strategy_name
                    ops_parts.append(tmp)

    summary_df = pd.DataFrame(summary_rows)
    if summary_df.empty:
        raise ValueError("Walk-forward sem resultados. Verifique os dados de entrada.")

    summary_path = output_dir / f"{run_id}_walkforward_resumo.csv"
    ops_path = output_dir / f"{run_id}_walkforward_ops.csv"
    aggregate_path = output_dir / f"{run_id}_walkforward_agregado.csv"
    meta_path = output_dir / f"{run_id}_walkforward_meta.json"

    summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")

    if ops_parts:
        ops_df = pd.concat(ops_parts, axis=0, ignore_index=True)
        ops_df.to_csv(ops_path, index=False, encoding="utf-8-sig")
    else:
        ops_df = pd.DataFrame()
        ops_df.to_csv(ops_path, index=False, encoding="utf-8-sig")

    agg = (
        summary_df.groupby(["Estrategia", "Segmento"], as_index=False)
        .agg(
            Folds=("Fold", "nunique"),
            Executadas_Media=("Entradas_Executadas", "mean"),
            WinRate_Medio=("Win_Rate_Executadas_%", "mean"),
            Lucro_Total=("Lucro_Final", "sum"),
            Drawdown_Max_Abs=("Max_Drawdown_Abs", "max"),
            Drawdown_Max_Pct=("Max_Drawdown_%", "max"),
            ProfitFactor_Medio=("Profit_Factor", "mean"),
        )
        .sort_values(["Segmento", "Estrategia"])
        .reset_index(drop=True)
    )
    agg.to_csv(aggregate_path, index=False, encoding="utf-8-sig")

    meta = {
        "run_id": run_id,
        "input": str(input_path),
        "config": str(config_path),
        "environment": args.environment,
        "train_days": args.train_days,
        "test_days": args.test_days,
        "step_days": args.step_days,
        "folds": len(folds),
        "generated_at_utc": datetime.utcnow().isoformat(),
    }
    meta_path.write_text(json.dumps(meta, ensure_ascii=True, indent=2), encoding="utf-8")

    print("\n=== WALK-FORWARD CONCLUIDO ===")
    print(f"Run_ID: {run_id}")
    print(f"Folds: {len(folds)}")
    print(f"Resumo: {summary_path}")
    print(f"Operacoes: {ops_path}")
    print(f"Agregado: {aggregate_path}")
    print(f"Meta: {meta_path}")


if __name__ == "__main__":
    main()
