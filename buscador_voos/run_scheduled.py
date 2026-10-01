import sys
import os
import logging
from pathlib import Path

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
