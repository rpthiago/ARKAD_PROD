#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
saida_zebra_vps.py — regra de SAIDA do Lay Draw (PREREGISTRO_lay_draw_saida_zebra.md): nos sinais de Lay Draw do
ledger do KO-10, quando a ZEBRA marca o 1o gol, manda no Telegram a odd de back do empate da primeira captura com preco
e grava em saida_zebra_ledger.csv (pnl_saida fechado na hora; pnl_hold e preenchido pelo relatorio das 06:00 com o FT).
Fatos: gols pelas linhas O/U, lado pela direcao da odd do favorito no Match Odds (trader_inplay_core.Jogo, so a parte
de estado — entradas/saidas do core desligadas). Leitura incremental do coletor. Uso: --loop 60 | --once
"""
import os, sys, csv, json, time, argparse, urllib.parse, urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path
AQUI = Path(__file__).resolve().parent; sys.path.insert(0, str(AQUI))
import trader_inplay_core as C

COLETOR = AQUI / "betfair_live_odds.csv"
KO_LEDGER = AQUI / "forward_ko_ledger.csv"
LEDGER = AQUI / "saida_zebra_ledger.csv"
ESTADO = AQUI / "saida_zebra_estado.json"
ENVF = AQUI / "alerta.env"
MTYPES = {"MATCH_ODDS", "OVER_UNDER_05", "OVER_UNDER_15", "OVER_UNDER_25", "OVER_UNDER_35"}
COLS = ["Data", "Home", "Away", "Hora", "fav_lado", "fav_odd", "odd_in", "evento", "minuto_gol", "odd_out", "ts_out", "pnl_saida",
        "pnl_hold", "resultado_ft", "status"]
GAP_S = 90


class JogoFatos(C.Jogo):
    def _entradas(self, ts, minuto, mk): pass
    def _saidas(self, ts, minuto, mk): pass


def _f(x):
    try:
        v = float(x); return v if v > 0 else None
    except Exception:
        return None


TG_ON = os.environ.get("ARKAD_TG_ALERTAS", "0") == "1"   # 18/09 (Thiago): so o relatorio das 06:00 e o stop diario avisam; aqui so ledger


def tg(msg):
    if not TG_ON: return
    tok = chat = None
    if ENVF.exists():
        for ln in open(ENVF):
            if ln.startswith("TG_TOKEN="): tok = ln.split("=", 1)[1].strip()
            elif ln.startswith("TG_CHAT="): chat = ln.split("=", 1)[1].strip()
    if not (tok and chat): return
    try:
        data = urllib.parse.urlencode({"chat_id": chat, "text": msg, "parse_mode": "HTML"}).encode()
        urllib.request.urlopen(urllib.request.Request("https://api.telegram.org/bot%s/sendMessage" % tok, data=data), timeout=15)
    except Exception as e:
        print("telegram falhou: %s" % str(e)[:80], flush=True)


def _esc(t): return str(t).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def sinais_lay_draw():
    """(ko_utc, home, away) -> odd_in dos sinais de Lay Draw do KO-10 de ontem/hoje (nomes da Betfair, mesmos do coletor)."""
    out = {}
    if not KO_LEDGER.exists(): return out
    hoje = datetime.now(timezone.utc).date()
    for r in csv.DictReader(open(KO_LEDGER, encoding="utf-8")):
        if r.get("Metodo") != "Lay Draw (Fav<=1.40)": continue
        try:
            ko_local = datetime.strptime(r["Data"] + " " + r["Hora"], "%Y-%m-%d %H:%M"); ko_utc = ko_local + timedelta(hours=3)
        except Exception:
            continue
        if (hoje - ko_utc.date()).days > 1: continue
        out[(ko_utc.strftime("%Y-%m-%d %H:%M"), r["Home"], r["Away"])] = float(r["Odd_Lay"])
    return out


class Estado:
    def __init__(self):
        self.offset = 0; self.buf = {}; self.buf_t0 = {}; self.jogos = {}; self.feitos = set(); self.pend_out = {}; self.esperando_ht = {}
        if ESTADO.exists():
            try:
                e = json.load(open(ESTADO, encoding="utf-8")); self.offset = int(e.get("offset", 0)); self.feitos = set(e.get("feitos", []))
            except Exception:
                pass

    def salvar(self):
        json.dump({"offset": self.offset, "feitos": sorted(self.feitos)[-2000:], "salvo_em": datetime.now(timezone.utc).isoformat()}, open(ESTADO, "w", encoding="utf-8"))


def gravar(row):
    novo = not LEDGER.exists()
    with open(LEDGER, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if novo: w.writerow(COLS)
        w.writerow(row)


def _fechar(E, k, J, chave, ts, mk, odd_in, ko_local):
    ob, _ = C._preco(mk, "MATCH_ODDS", "The Draw", "back")
    if ob is None: return                                         # mercado suspenso: espera a proxima captura
    min_gol = E.pend_out.pop(chave); s_in = 1.0 / (odd_in - 1.0); pnl = s_in * (1.0 - odd_in / ob)
    gravar([ko_local.strftime("%Y-%m-%d"), k[1], k[2], ko_local.strftime("%H:%M"), J.fav_pre[1], J.fav_pre[0], odd_in, "GOL_ZEBRA", min_gol, ob, ts, round(pnl, 5), "", "", "SAIDA"])
    tg("<b>SAÍDA Lay Draw · zebra marcou (%s')</b>\n%s x %s\nback no empate @<b>%.2f</b> (entrada lay %.2f) → fecha em %+.3fu por 1u de risco"
       % (min_gol, _esc(k[1]), _esc(k[2]), ob, odd_in, pnl))
    E.feitos.add(chave); E.esperando_ht[chave] = (min_gol, odd_in)
    print("%s SAIDA %s x %s: gol zebra %s' | empate back %.2f | pnl %+.3f" % (datetime.now().strftime("%H:%M:%S"), k[1], k[2], min_gol, ob, pnl), flush=True)


def _flush(E, k, ts, mtk, mk, odd_in):
    J = E.jogos.get(k)
    if J is None: J = E.jogos[k] = JogoFatos(k[0], k[1], k[2])
    J.captura(ts, float(mtk), mk)
    minuto = round(-mtk - 15, 1)
    if minuto <= 0 or J.fav_pre is None: return
    chave = "%s|%s|%s" % k
    ko_local = datetime.strptime(k[0], "%Y-%m-%d %H:%M") - timedelta(hours=3)
    # saida pendente: esperando a primeira captura com preco no empate
    if chave in E.pend_out:
        _fechar(E, k, J, chave, ts, mk, odd_in, ko_local); return
    # alternativa medida (sem alerta): odd do empate na 1a captura do intervalo (47-58 min de relogio), estado no HT
    if chave in E.esperando_ht and 47 <= minuto <= 58:
        ob, _ = C._preco(mk, "MATCH_ODDS", "The Draw", "back")
        if ob is None: return
        min_gol, oi = E.esperando_ht.pop(chave); s_in = 1.0 / (oi - 1.0); pnl = s_in * (1.0 - oi / ob)
        est = "zebra_na_frente" if J.gols == 1 else ("empatou" if (J.gols == 2 and J.lado_gol.get(2) in ("empate2", None)) else "2+gols")
        gravar([ko_local.strftime("%Y-%m-%d"), k[1], k[2], ko_local.strftime("%H:%M"), J.fav_pre[1], J.fav_pre[0], oi, "HT_ODD", min_gol, ob, ts, round(pnl, 5), "", est, "MEDIDA"])
        return
    if chave in E.esperando_ht and minuto > 58: E.esperando_ht.pop(chave, None)
    if chave in E.feitos: return
    if J.gols is not None and J.gols >= 1:
        lado = J.lado_gol.get(1)
        if lado is None:
            if J.gols >= 2:                                    # 2 gols sem lado do 1o: registra e encerra
                gravar([ko_local.strftime("%Y-%m-%d"), k[1], k[2], ko_local.strftime("%H:%M"), J.fav_pre[1], J.fav_pre[0], odd_in, "LADO_INDETERMINADO", minuto, "", ts, "", "", "", "SEM_ACAO"])
                E.feitos.add(chave)
            return
        if lado == J.fav_pre[1]:
            gravar([ko_local.strftime("%Y-%m-%d"), k[1], k[2], ko_local.strftime("%H:%M"), J.fav_pre[1], J.fav_pre[0], odd_in, "GOL_FAV", minuto, "", ts, "", "", "", "SEM_ACAO"])
            E.feitos.add(chave)
        else:
            E.pend_out[chave] = minuto
            _fechar(E, k, J, chave, ts, mk, odd_in, ko_local)             # se ja houver preco nesta captura, fecha agora


def passagem(E):
    sig = sinais_lay_draw()
    if not COLETOR.exists() or not sig: return 0
    if COLETOR.stat().st_size < E.offset: E.offset = 0
    n = 0
    with open(COLETOR, "r", encoding="utf-8", errors="replace") as f:
        f.seek(E.offset)
        if E.offset == 0: f.readline()
        for line in f:
            if not line.endswith("\n"): break
            E.offset += len(line.encode("utf-8"))
            p = line.rstrip("\n").split(",")
            if len(p) < 13 or p[1] not in MTYPES: continue
            k = (p[5], p[3], p[4])
            if k not in sig: continue
            try: mtk = float(p[6])
            except Exception: continue
            if mtk > 10 or mtk < -115: continue
            ts = p[0]
            try: te = datetime.strptime(ts[:19], "%Y-%m-%d %H:%M:%S").timestamp()
            except Exception: continue
            if k in E.buf and te - E.buf_t0[k] > GAP_S:
                bts, bmtk, bmk = E.buf.pop(k); E.buf_t0.pop(k); _flush(E, k, bts, bmtk, bmk, sig[k])
            if k not in E.buf: E.buf[k] = (ts, mtk, {}); E.buf_t0[k] = te
            E.buf[k][2].setdefault(p[1], {})[p[7]] = (_f(p[9]), _f(p[10]) or 0.0, _f(p[11]), _f(p[12]) or 0.0)
            n += 1
    agora = time.time()
    for k in list(E.buf):
        if agora - E.buf_t0[k] > GAP_S and k in sig:
            bts, bmtk, bmk = E.buf.pop(k); E.buf_t0.pop(k); _flush(E, k, bts, bmtk, bmk, sig[k])
    for k in list(E.jogos):                                     # libera jogos antigos
        if k not in sig: E.jogos.pop(k, None)
    E.salvar(); return n


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--once", action="store_true"); ap.add_argument("--loop", type=int, default=0); a = ap.parse_args()
    E = Estado()
    if E.offset == 0 and COLETOR.exists():
        E.offset = max(0, COLETOR.stat().st_size - 20 * 1024 * 1024)
        with open(COLETOR, "rb") as f: f.seek(E.offset); f.readline(); E.offset = f.tell()
    if a.loop <= 0: print("linhas %d | jogos acompanhados %d" % (passagem(E), len(E.jogos))); return
    print("=== SAIDA ZEBRA (Lay Draw) === loop %ds" % a.loop, flush=True)
    while True:
        try: passagem(E)
        except Exception as e: print("erro: %s" % str(e)[:200], flush=True)
        time.sleep(a.loop)


if __name__ == "__main__":
    main()
