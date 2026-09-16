#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tracker_trader_inplay.py — daemon stake-zero dos métodos trader in-play (VPS).
Autoridade: PREREGISTRO_SUITE_TRADER_INPLAY.md (+ emenda 16/09) e GEMINI.md. Núcleo: trader_inplay_core.py
(o MESMO que roda no histórico em trader_inplay_olhar.py).

Fonte ÚNICA: betfair_live_odds.csv do coletor, lido de forma incremental (offset persistido) — nunca o feed
pré-jogo, nunca o Correct Score de menor lay como placar. Odd de entrada = primeira captura elegível da janela;
odd de saída = primeira captura com preço depois do evento de saída. P&L das duas pernas com comissão 5%.

Uso:
  --once        uma passagem (lê o que há de novo no coletor) e sai
  --loop 60     daemon: a cada 60 s lê o incremento do coletor e avança as máquinas de estado
  --settle      (manual, com o serviço parado) fecha jogos sem captura há > 150 min: trades abertos viram
                SEM_ODD_SAIDA (fora da conta). Com o daemon ativo isso já acontece a cada 30 min dentro dele.
Log: trader_inplay_log.csv (stake 0.0, tipo_registro OBSERVACAO_STAKE_ZERO).
"""
import os, sys, csv, json, time, argparse
from datetime import datetime, timezone
from pathlib import Path
AQUI = Path(__file__).resolve().parent; sys.path.insert(0, str(AQUI))
import trader_inplay_core as C

COLETOR = Path(os.environ.get("ARKAD_COLETOR", "/home/ubuntu/betfair-collector/betfair_live_odds.csv"))
LOG = AQUI / "trader_inplay_log.csv"
ESTADO = AQUI / "trader_inplay_estado.json"          # offset do coletor (sobrevive a restart)
MTYPES = {"MATCH_ODDS", "OVER_UNDER_05", "OVER_UNDER_15", "OVER_UNDER_25", "OVER_UNDER_35", "CORRECT_SCORE"}
CS_RUNNERS = {"1 - 0", "0 - 1", "1 - 1", "2 - 0", "0 - 2"}
COLS = ["timestamp_utc", "data", "ko", "home", "away", "id_metodo", "nome_metodo", "mercado", "lado", "runner", "fav_pre",
        "minuto_entrada", "gols_entrada", "odd_entrada", "liq_entrada", "ts_entrada", "evento_saida", "minuto_saida", "gols_saida",
        "odd_saida", "ts_saida", "pnl_u_risco", "resultado", "status", "stake", "tipo_registro"]
GAP_S = 90


def _f(x):
    try:
        v = float(x); return v if v > 0 else None
    except Exception:
        return None


class Estado:
    def __init__(self):
        self.offset = 0; self.jogos = {}; self.buf = {}; self.buf_t0 = {}; self.ult_ts = {}
        if ESTADO.exists():
            try:
                self.offset = int(json.load(open(ESTADO, encoding="utf-8")).get("offset", 0))
            except Exception:
                pass

    def salvar(self):
        json.dump({"offset": self.offset, "salvo_em": datetime.now(timezone.utc).isoformat()}, open(ESTADO, "w", encoding="utf-8"))


def gravar(J, trades):
    novo = not LOG.exists()
    with open(LOG, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if novo: w.writerow(COLS)
        for t in trades:
            pnl = t.get("pnl")
            w.writerow([datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), J.ko[:10], J.ko, J.home, J.away, t["id"], t["nome"],
                        t.get("mercado", ""), t.get("lado", ""), t.get("runner", ""), J.fav_pre[0] if J.fav_pre else "",
                        t.get("min_in", ""), t.get("gols_in", ""), t.get("odd_in", ""), t.get("liq_in", ""), t.get("ts_in", ""),
                        t.get("motivo", t.get("evento", "")), t.get("min_out", ""), t.get("gols_out", ""), t.get("odd_out", ""), t.get("ts_out", ""),
                        "" if pnl is None else round(pnl, 5), ("" if pnl is None else ("GREEN" if pnl > 0 else "RED")), t["status"],
                        0.0, "OBSERVACAO_STAKE_ZERO"])


def _flush(E, k, ts, mtk, mk):
    J = E.jogos.get(k)
    if J is None:
        J = E.jogos[k] = C.Jogo(k[0], k[1], k[2])
    antes = len(J.fechados)
    J.captura(ts, mtk, mk)
    if len(J.fechados) > antes:
        gravar(J, J.fechados[antes:])
    E.ult_ts[k] = ts


def passagem(E, verbose=True):
    """lê o incremento do coletor desde o offset e alimenta as máquinas de estado."""
    if not COLETOR.exists(): print("coletor não encontrado: %s" % COLETOR); return 0
    tam = COLETOR.stat().st_size
    if tam < E.offset: E.offset = 0                                  # arquivo rotacionado
    n = 0
    with open(COLETOR, "r", encoding="utf-8", errors="replace") as f:
        f.seek(E.offset)
        if E.offset == 0: f.readline()                                # cabeçalho
        for line in f:
            if not line.endswith("\n"): break                         # linha parcial: fica para a próxima
            E.offset += len(line.encode("utf-8"))
            p = line.rstrip("\n").split(",")
            if len(p) < 13 or p[1] not in MTYPES: continue
            if p[1] == "CORRECT_SCORE" and p[7] not in CS_RUNNERS: continue
            try: mtk = float(p[6])
            except Exception: continue
            if mtk > 10 or mtk < -115: continue
            k = (p[5], p[3], p[4]); ts = p[0]
            try: te = datetime.strptime(ts[:19], "%Y-%m-%d %H:%M:%S").timestamp()
            except Exception: continue
            if k in E.buf and te - E.buf_t0[k] > GAP_S:
                bts, bmtk, bmk = E.buf.pop(k); E.buf_t0.pop(k); _flush(E, k, bts, bmtk, bmk)
            if k not in E.buf: E.buf[k] = (ts, mtk, {}); E.buf_t0[k] = te
            E.buf[k][2].setdefault(p[1], {})[p[7]] = (_f(p[9]), _f(p[10]) or 0.0, _f(p[11]), _f(p[12]) or 0.0)
            n += 1
    # passes completos: quem não recebeu linha há > GAP_S segundos (relógio do coletor = UTC, igual ao da VPS)
    agora = time.time()
    for k in list(E.buf):
        if agora - E.buf_t0[k] > GAP_S:
            bts, bmtk, bmk = E.buf.pop(k); E.buf_t0.pop(k); _flush(E, k, bts, bmtk, bmk)
    E.salvar()
    if verbose:
        ab = sum(len(J.abertos) for J in E.jogos.values())
        print("%s linhas lidas %d | jogos em memória %d | trades abertos %d | offset %d" % (datetime.now().strftime("%H:%M:%S"), n, len(E.jogos), ab, E.offset), flush=True)
    return n


def settle(E):
    """jogos sem captura há > 150 min: fecha (trades abertos -> SEM_ODD_SAIDA) e libera memória."""
    agora = time.time(); n = 0
    for k in list(E.jogos):
        ts = E.ult_ts.get(k)
        if ts is None: continue
        try: te = datetime.strptime(ts[:19], "%Y-%m-%d %H:%M:%S").timestamp()
        except Exception: te = 0
        if agora - te > 150 * 60:
            J = E.jogos.pop(k); antes = len(J.fechados); J.encerrar()
            if len(J.fechados) > antes: gravar(J, J.fechados[antes:]); n += len(J.fechados) - antes
            E.ult_ts.pop(k, None)
    print("settle: %d trades sem odd de saída marcados; jogos em memória: %d" % (n, len(E.jogos)), flush=True)


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--once", action="store_true"); ap.add_argument("--loop", type=int, default=0); ap.add_argument("--settle", action="store_true")
    a = ap.parse_args(); E = Estado()
    if E.offset == 0 and COLETOR.exists():
        E.offset = max(0, COLETOR.stat().st_size - 50 * 1024 * 1024)   # 1a partida: começa nos últimos ~50 MB (jogos em curso)
        with open(COLETOR, "rb") as f: f.seek(E.offset); f.readline(); E.offset = f.tell()
        print("primeira partida: offset em %d (últimos ~50 MB do coletor)" % E.offset, flush=True)
    if a.settle:
        # a liquidação de jogos parados roda DENTRO do daemon (a cada 30 min): um segundo processo lendo o mesmo
        # offset roubaria linhas do daemon. Este modo só existe para uso manual com o serviço parado.
        if os.system("systemctl is-active --quiet trader-inplay") == 0:
            print("daemon ativo: settle já roda dentro dele a cada 30 min; nada a fazer."); return
        passagem(E); settle(E); return
    if a.once or a.loop <= 0:
        passagem(E); return
    print("=== TRADER IN-PLAY (stake-zero) === loop %ds | métodos: %s" % (a.loop, ", ".join(m["id"] for m in C.METODOS)), flush=True)
    ult_settle = time.time()
    while True:
        try:
            passagem(E)
            if time.time() - ult_settle > 1800: settle(E); ult_settle = time.time()
        except Exception as e:
            print("erro na passagem: %s" % str(e)[:200], flush=True)
        time.sleep(a.loop)


if __name__ == "__main__":
    main()
