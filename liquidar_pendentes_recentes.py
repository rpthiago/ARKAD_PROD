# -*- coding: utf-8 -*-
"""
liquidar_pendentes_recentes.py — retenta a liquidacao das planilhas diarias dos ultimos N dias.
As planilhas Sinais_Metodos_Aprovados_<data>.xlsx so eram liquidadas por botao na pagina 01, e a
base de placares publica com atraso: um jogo sem placar hoje precisa ser retentado amanha.
Roda no job das 23:30 (relatorio_forward_5metodos.bat), sem Telegram (o relatorio ja manda o resumo).
"""
import sys, os, warnings
from datetime import date, timedelta
warnings.filterwarnings("ignore"); sys.stdout.reconfigure(encoding="utf-8")
ROOT = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, ROOT)
from automacao_diaria_aprovados import liquidar_resultados_noite
N = int(sys.argv[1]) if len(sys.argv) > 1 else 10
for i in range(N, -1, -1):
    ds = (date.today() - timedelta(days=i)).isoformat()
    try:
        liquidar_resultados_noite(ds, enviar_telegram=False)
    except PermissionError:
        print("[%s] planilha aberta no Excel — pulada" % ds)
    except Exception as e:
        print("[%s] erro: %s" % (ds, str(e)[:80]))
