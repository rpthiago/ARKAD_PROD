import json
import subprocess
import sys
from copy import deepcopy
from pathlib import Path

import pandas as pd


COMMIT_REF = "eb4e32e"
REL_DIR = Path("Arquivados_Apostas_Diarias/Relatorios/Backtest_Abril_2026")
BENCH_DIR = REL_DIR / "Benchmark_97"
REF_CSV_REPO_PATH = "Arquivados_Apostas_Diarias/Relatorios/Backtest_Abril_2026/abril_cenario_B_filtros.csv"
REF_SUMMARY_REPO_PATH = "Arquivados_Apostas_Diarias/Relatorios/Backtest_Abril_2026/abril_2026_B_filtros_summary.json"
REF_CONFIG_REPO_PATH = "config_backtest_exec.json"


def _git_show_text(path_in_commit: str) -> str:
    cmd = ["git", "show", f"{COMMIT_REF}:{path_in_commit}"]
    proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if proc.returncode != 0:
        raise RuntimeError(f"Falha ao ler {path_in_commit} no commit {COMMIT_REF}: {proc.stderr}")
    return proc.stdout


def _prepare_engine_config_without_filters(src_config: Path, out_config: Path) -> None:
    with src_config.open("r", encoding="utf-8") as f:
        cfg = json.load(f)

    cfg_no_filters = deepcopy(cfg)
    cfg_no_filters["filters"] = {
        "exclude_leagues": [],
        "conditional_rules": [],
        "odd_bands": cfg.get("filters", {}).get("odd_bands", []),
        "filtros_rodo": [],
        "toxic_cuts": [],
    }

    out_config.write_text(json.dumps(cfg_no_filters, ensure_ascii=True, indent=2), encoding="utf-8")


def main() -> None:
    BENCH_DIR.mkdir(parents=True, exist_ok=True)

    ref_csv_path = BENCH_DIR / "referencia_eb4e32e_abril_cenario_B_filtros.csv"
    ref_summary_path = BENCH_DIR / "referencia_eb4e32e_abril_2026_B_filtros_summary.json"
    replay_summary_path = BENCH_DIR / "replay_eb4e32e_cfg_commit_summary.json"
    replay_ops_path = BENCH_DIR / "replay_eb4e32e_cfg_commit_ops.csv"
    replay_equity_path = BENCH_DIR / "replay_eb4e32e_cfg_commit_equity.png"
    added_path = BENCH_DIR / "delta_atual_vs_ref_linhas_adicionadas.csv"
    removed_path = BENCH_DIR / "delta_atual_vs_ref_linhas_removidas.csv"
    report_path = BENCH_DIR / "benchmark97_report.json"

    ref_csv_text = _git_show_text(REF_CSV_REPO_PATH)
    ref_summary_text = _git_show_text(REF_SUMMARY_REPO_PATH)
    ref_cfg_text = _git_show_text(REF_CONFIG_REPO_PATH)

    ref_csv_path.write_text(ref_csv_text, encoding="utf-8")
    ref_summary_path.write_text(ref_summary_text, encoding="utf-8")
    ref_cfg_path = BENCH_DIR / "referencia_eb4e32e_config_backtest_exec.json"
    ref_cfg_path.write_text(ref_cfg_text, encoding="utf-8")

    run_cmd = [
        sys.executable,
        "engine_ciclo_producao.py",
        "--input",
        str(ref_csv_path),
        "--config",
        str(ref_cfg_path),
        "--environment",
        "historico",
        "--output-dir",
        str(BENCH_DIR),
        "--run-id",
        "replay_eb4e32e_cfg_commit",
        "--skip-mini-report",
    ]
    run = subprocess.run(run_cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if run.returncode != 0:
        raise RuntimeError(f"Falha ao executar replay: {run.stderr}")

    current_summary_generated = BENCH_DIR / "replay_eb4e32e_cfg_commit_summary.json"
    if not current_summary_generated.exists():
        raise FileNotFoundError("Summary do replay nao foi gerado.")

    # Normaliza nomes esperados no relatorio.
    if current_summary_generated != replay_summary_path:
        current_summary_generated.replace(replay_summary_path)
    generated_ops = BENCH_DIR / "replay_eb4e32e_cfg_commit_ops.csv"
    generated_equity = BENCH_DIR / "replay_eb4e32e_cfg_commit_equity.png"
    if generated_ops.exists() and generated_ops != replay_ops_path:
        generated_ops.replace(replay_ops_path)
    if generated_equity.exists() and generated_equity != replay_equity_path:
        generated_equity.replace(replay_equity_path)

    with ref_summary_path.open("r", encoding="utf-8") as f:
        summary_ref = json.load(f)
    with replay_summary_path.open("r", encoding="utf-8") as f:
        summary_replay = json.load(f)

    # Delta entre recorte atual e referencia.
    current_csv_path = REL_DIR / "abril_cenario_B_filtros.csv"
    if not current_csv_path.exists():
        raise FileNotFoundError("CSV atual nao encontrado. Rode _backtest_abril_2026.py antes.")

    df_ref = pd.read_csv(ref_csv_path)
    df_cur = pd.read_csv(current_csv_path)
    key = ["Data_Arquivo", "Liga", "Jogo", "Metodo", "Odd_Base", "Odd_Real_Pega", "1/0"]

    for d in (df_ref, df_cur):
        for c in key:
            d[c] = d[c].astype(str).str.strip()

    s_ref = set(map(tuple, df_ref[key].values.tolist()))
    s_cur = set(map(tuple, df_cur[key].values.tolist()))
    df_added = pd.DataFrame(list(s_cur - s_ref), columns=key).sort_values(key).reset_index(drop=True)
    df_removed = pd.DataFrame(list(s_ref - s_cur), columns=key).sort_values(key).reset_index(drop=True)
    df_added.to_csv(added_path, index=False, encoding="utf-8-sig")
    df_removed.to_csv(removed_path, index=False, encoding="utf-8-sig")

    wr_ref = float(summary_ref.get("Win_Rate_Executadas_%", 0.0))
    wr_replay = float(summary_replay.get("Win_Rate_Executadas_%", 0.0))

    report = {
        "commit_referencia": COMMIT_REF,
        "wr_referencia": wr_ref,
        "wr_replay_benchmark": wr_replay,
        "wr_replay_igual_referencia": abs(wr_ref - wr_replay) < 1e-9,
        "executadas_referencia": int(summary_ref.get("Entradas_Executadas", 0)),
        "executadas_replay": int(summary_replay.get("Entradas_Executadas", 0)),
        "linhas_adicionadas_vs_referencia": int(len(df_added)),
        "linhas_removidas_vs_referencia": int(len(df_removed)),
        "arquivos": {
            "referencia_csv": str(ref_csv_path).replace("\\", "/"),
            "referencia_summary": str(ref_summary_path).replace("\\", "/"),
            "referencia_config": str(ref_cfg_path).replace("\\", "/"),
            "replay_summary": str(replay_summary_path).replace("\\", "/"),
            "delta_adicionadas": str(added_path).replace("\\", "/"),
            "delta_removidas": str(removed_path).replace("\\", "/"),
        },
    }

    report_path.write_text(json.dumps(report, ensure_ascii=True, indent=2), encoding="utf-8")

    print("=== BENCHMARK 97% (REPLAY TRAVADO) ===")
    print(json.dumps(report, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
