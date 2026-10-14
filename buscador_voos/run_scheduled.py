import sys
import os
import traceback

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

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CRASH_LOG = os.path.join(BASE_DIR, "crash.log")

try:
    from pathlib import Path
    import logging

    ROOT = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(ROOT))
    os.chdir(str(ROOT))

    log_file = ROOT / "buscador_voos" / "execucoes.log"
    logging.basicConfig(
        filename=str(log_file),
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        encoding="utf-8"
    )

    from buscador_voos.main import carregar_config, executar_varredura

    def main():
        logging.info("--- INICIANDO CHECAGEM AGENDADA: VOLTA SALVADOR (SSA) ➔ CONFINS (CNF) ---")
        try:
            config = carregar_config()
            resultados = executar_varredura(config, enviar_resumo=False)
            logging.info("Varredura de volta concluída com sucesso. %d trechos avaliados.", len(resultados))
        except Exception as e:
            logging.exception("Erro crítico na execução agendada: %s", e)

    if __name__ == "__main__":
        main()

except Exception:
    with open(CRASH_LOG, "a", encoding="utf-8") as f:
        import datetime
        f.write(f"[{datetime.datetime.now()}] CRASH:\n" + traceback.format_exc() + "\n")
    sys.exit(1)
