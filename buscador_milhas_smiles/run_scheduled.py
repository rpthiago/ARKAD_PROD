"""
Script para execução silenciosa via Windows Task Scheduler com pythonw.exe.
Protegido contra crashes de NoneType em sys.stdout / sys.stderr.
"""
import os
import sys
import logging
from pathlib import Path

# Safe stdout/stderr redirection for pythonw.exe
if sys.stdout is None:
    sys.stdout = open(os.devnull, "w", encoding="utf-8")
elif getattr(sys.stdout, "encoding", None) and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

if sys.stderr is None:
    sys.stderr = open(os.devnull, "w", encoding="utf-8")
elif getattr(sys.stderr, "encoding", None) and sys.stderr.encoding.lower() != "utf-8":
    try:
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
os.chdir(str(ROOT_DIR))

log_file = ROOT_DIR / "buscador_milhas_smiles" / "execucoes_smiles.log"
logging.basicConfig(
    filename=str(log_file),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    encoding="utf-8"
)

from buscador_milhas_smiles.main import executar_busca

def main():
    logging.info("--- INICIANDO CHECAGEM AGENDADA: MILHAS SMILES & PARCEIRAS ---")
    try:
        executar_busca(dry_run=False, enviar_resumo=False)
        logging.info("Checagem de milhas Smiles concluída com sucesso.")
    except Exception as e:
        logging.exception("Erro na execução agendada de milhas Smiles: %s", e)

if __name__ == "__main__":
    main()
