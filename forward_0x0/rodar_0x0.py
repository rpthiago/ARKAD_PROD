# -*- coding: utf-8 -*-
"""
rodar_0x0.py — UM comando para o dia inteiro do Lay 0x0 XGBoost.

  python forward_0x0/rodar_0x0.py [YYYY-MM-DD] [--clv] [--hist] [--retro]

Faz, em ordem: (1) picks do dia, (2) incorpora no ledger e liquida o que a base ja alcancou,
(3) opcionalmente CLV e snapshot historico. Um log unico (rodar_0x0.log) e um RESUMO de duas
linhas explicando o dia — inclusive quando nao ha pick, que e o caso mais comum.

NAO mexe na regra: chama os proprios scripts ja validados (gerar_picks_dia.py e
liquidar_picks_0x0.py) sem reimplementar nada. Regra congelada (pre-registro 2026-08-02):
liga_0x0_rate < 0.08 · mkt_prob_0x0 < 0.10 (odd de 0x0 do b365) · odd de lay 10-20 na Betfair · EV > 2%.
"""
import os, sys, io, re, runpy, contextlib
from datetime import datetime

AQUI = os.path.dirname(os.path.abspath(__file__))
LOG = os.path.join(AQUI, "rodar_0x0.log")
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass

datas = [a for a in sys.argv[1:] if re.fullmatch(r"\d{4}-\d{2}-\d{2}", a)]
dia = datas[0] if datas else datetime.now().strftime("%Y-%m-%d")
flags = [a for a in sys.argv[1:] if a.startswith("--")]


def passo(nome, script, argv):
    """roda um script do forward_0x0 como se fosse pela linha de comando; devolve a saida dele."""
    buf = io.StringIO(); velho = sys.argv[:]
    sys.argv = [os.path.join(AQUI, script)] + argv
    try:
        with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
            runpy.run_path(os.path.join(AQUI, script), run_name="__main__")
    except SystemExit:
        pass                              # os scripts usam sys.exit() em caminhos normais
    except Exception as e:
        buf.write("\n[ERRO em %s] %s: %s\n" % (script, type(e).__name__, str(e)[:200]))
    finally:
        sys.argv = velho
    return buf.getvalue()


def num(txt, padrao, grupo=1, default="?"):
    m = re.search(padrao, txt)
    return m.group(grupo) if m else default


saidas = []
t0 = datetime.now()
g = passo("picks", "gerar_picks_dia.py", [dia] + [f for f in flags if f == "--retro"])
saidas.append(("PICKS", g))
q = passo("ledger", "liquidar_picks_0x0.py", [])
saidas.append(("LEDGER", q))
if "--clv" in flags: saidas.append(("CLV", passo("clv", "atualizar_clv.py", [])))
if "--hist" in flags: saidas.append(("HIST", passo("hist", "registrar_hist.py", [])))

# ---------------------------------------------------------------- RESUMO (o que voce le)
grade = num(g, r"grade do dia: (\d+) jogos")
com0x0 = num(g, r"com odd de 0x0 do b365 [^:]*: (\d+)")
feat = num(g, r"jogos hoje featurizados=(\d+)")
casou = int(num(g, r"casamento com o feed Betfair: (\d+) exatos", default="0") or 0) + int(num(g, r"exatos \+ (\d+) por semelhanca", default="0") or 0)
ko_pass = num(g, r"KO ja passado: (\d+)", default="0")
picks = num(g, r"(\d+) pick\(s\) nesta rodada", default=None)
novos = num(g, r", (\d+) novos no dia", default="?")
no_dia = num(g, r"com (\d+) pick\(s\)\.", default="?")
bloco = num(g, r"nesta rodada \(([a-z]+)\)", default="?")
if picks is None:
    picks = "0" if "Nenhum jogo bate a regra" in g else "?"
    novos, no_dia, bloco = "0", no_dia, "?"
lig = num(q, r"ledger gravado: [^(]*\((\d+) linhas\)")
fwd = re.search(r"N=(\d+) \| (\d+)G/(\d+)R \| WR=\s*([\d.]+)% vs break-even medio\s*([\d.]+)%", q)
roi = num(q, r"LIABILITY=1u: soma [^|]*\| ROI\s*([+-]?[\d.]+%)", default="?")

linhas = ["", "=" * 78,
          "RESUMO %s — Lay 0x0 XGBoost (stake-zero, regra congelada)" % dia,
          "=" * 78,
          "  dia:    %s jogos na grade -> %s com odd de 0x0 do b365 -> %s com features -> %s casados com a Betfair -> %s pick(s)"
          % (grade, com0x0, feat, casou, picks),
          "  rodada: bloco %s | %s pick(s) novos | o dia acumula %s pick(s) (voce entra em blocos: manha/tarde/noite)"
          % (bloco, novos, no_dia),
          "          (descartados por KO ja passado: %s)" % ko_pass]
if fwd:
    linhas.append("  forward: N=%s liquidadas (%sG/%sR) | WR %s%% vs BE %s%% | ROI liability %s | ledger %s linhas"
                  % (fwd.group(1), fwd.group(2), fwd.group(3), fwd.group(4), fwd.group(5), roi, lig))
else:
    linhas.append("  forward: ledger %s linhas (resumo nao encontrado na saida do liquidador)" % lig)
if "[ERRO" in g or "[ERRO" in q:
    linhas.append("  ATENCAO: houve erro em um dos passos — ver %s" % os.path.basename(LOG))
linhas += ["  detalhe completo: %s" % os.path.basename(LOG), "=" * 78]
resumo = "\n".join(linhas)

with io.open(LOG, "a", encoding="utf-8") as fh:
    fh.write("\n\n" + "#" * 78 + "\n# %s  (dia %s, flags %s, %.0fs)\n" % (t0.strftime("%Y-%m-%d %H:%M:%S"), dia, " ".join(flags) or "-", (datetime.now() - t0).total_seconds()))
    for nome, txt in saidas: fh.write("\n----- %s -----\n%s" % (nome, txt))
    fh.write(resumo + "\n")
print(resumo)
