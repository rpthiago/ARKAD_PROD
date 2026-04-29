import subprocess
import sys
from pathlib import Path

BENCH = Path("Arquivados_Apostas_Diarias/Relatorios/Backtest_Abril_2026/Benchmark_97")
CSV = BENCH / "universo_97_96_exec.csv"
CFG = Path("config_universo_97.json")
OUT = BENCH
RUN_ID = "universo_97_96_exec"


def main() -> None:
    if not CSV.exists():
        raise FileNotFoundError(f"CSV do universo 97/96 nao encontrado: {CSV}")
    if not CFG.exists():
        raise FileNotFoundError(f"Config de referencia nao encontrada: {CFG}")

    cmd = [
        sys.executable,
        "engine_ciclo_producao.py",
        "--input",
        str(CSV),
        "--config",
        str(CFG),
        "--environment",
        "historico",
        "--output-dir",
        str(OUT),
        "--run-id",
        RUN_ID,
        "--skip-mini-report",
    ]

    r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if r.returncode != 0:
        raise RuntimeError(r.stderr)

    print("=== UNIVERSO 97/96 EXECUTADO ===")
    print(r.stdout)
    print(f"Summary: {OUT / (RUN_ID + '_summary.json')}")


if __name__ == "__main__":
    main()
