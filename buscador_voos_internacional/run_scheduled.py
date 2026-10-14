import sys
import os

# Garante carregamento das DLLs OpenSSL do Anaconda em segundo plano (Task Scheduler sem conda activate)
def _configurar_dlls_anaconda():
    prefixos = [getattr(sys, "prefix", None), getattr(sys, "base_prefix", None)]
    for p in prefixos:
        if not p:
            continue
        lib_bin = os.path.join(p, "Library", "bin")
        if os.path.isdir(lib_bin):
            if hasattr(os, "add_dll_directory"):
                try:
                    os.add_dll_directory(lib_bin)
                except Exception:
                    pass
            if lib_bin not in os.environ.get("PATH", ""):
                os.environ["PATH"] = lib_bin + os.pathsep + os.environ.get("PATH", "")

_configurar_dlls_anaconda()

import logging
from pathlib import Path

# Proteção stdout/stderr para pythonw.exe sem console
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

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.chdir(str(ROOT))

log_file = ROOT / "buscador_voos_internacional" / "execucoes_internacional.log"
logging.basicConfig(
    filename=str(log_file),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    encoding="utf-8"
)

from buscador_voos_internacional.main import carregar_config, executar_varredura_internacional

def main():
    logging.info("--- INICIANDO CHECAGEM AGENDADA: EUROPA, TURQUIA & JAPÃO ---")
    try:
        config = carregar_config()
        resultados = executar_varredura_internacional(config, enviar_resumo=False)
        logging.info("Checagem internacional concluída. %d cotações avaliadas.", len(resultados))
    except Exception as e:
        logging.exception("Erro na execução agendada internacional: %s", e)

if __name__ == "__main__":
    main()
