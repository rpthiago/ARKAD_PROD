import sys
import os
import traceback

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
        logging.info("--- INICIANDO CHECAGEM AGENDADA DE VOOS BH -> NORDESTE ---")
        try:
            config = carregar_config()
            resultados = executar_varredura(config, enviar_resumo=False)
            logging.info("Varredura concluída com sucesso. %d destinos processados.", len(resultados))
        except Exception as e:
            logging.exception("Erro crítico na execução agendada: %s", e)

    if __name__ == "__main__":
        main()

except Exception:
    with open(CRASH_LOG, "a", encoding="utf-8") as f:
        import datetime
        f.write(f"[{datetime.datetime.now()}] CRASH:\n" + traceback.format_exc() + "\n")
    sys.exit(1)
