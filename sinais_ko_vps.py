#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sinais_ko_vps.py — sinais dos 5 metodos no KO-10 (odd de lay real do coletor), com alerta no Telegram.
Le betfair_live_odds.csv de forma incremental (offset persistido, igual ao tracker_trader_inplay.py).
Um jogo e avaliado UMA vez: na primeira captura com min_to_ko dentro de JANELA_KO. Ledger: forward_ko_ledger.csv
(append-only; liquidado pelo relatorio das 06:00 com o placar oficial). Regras: sinais_ko_core.py.
Uso: --loop 60 (servico) | --once
"""
import os, sys, csv, json, time, argparse, urllib.parse, urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path
AQUI = Path(__file__).resolve().parent; sys.path.insert(0, str(AQUI))
import sinais_ko_core as K

COLETOR = AQUI / "betfair_live_odds.csv"
LEDGER = AQUI / "forward_ko_ledger.csv"
ESTADO = AQUI / "sinais_ko_estado.json"
ENVF = AQUI / "alerta.env"
MTYPES = {"MATCH_ODDS", "OVER_UNDER_25", "OVER_UNDER_45", "CORRECT_SCORE"}
CS_RUNNERS = {"0 - 3", "2 - 2"}
COLS = ["Data", "Metodo", "Liga", "Home", "Away", "Hora", "Odd_Lay", "Odd_Fav", "liq_lay", "min_to_ko", "ts_captura", "origem",
        "status", "gols_H", "gols_A", "placar", "resultado", "pnl_u", "pnl_rs", "break_even", "liquidado_em", "fonte_placar", "universo", "no_0600"]
S0600 = AQUI / "sinais_0600.csv"       # lista das 06:00 do dia (Data, Metodo, Home, Away no nome do feed), enviada pelo relatorio


def _canon(t):
    import unicodedata, re
    t = unicodedata.normalize("NFKD", str(t)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", t)


def sinais_0600():
    out = {}
    if not S0600.exists(): return out
    for r in csv.DictReader(open(S0600, encoding="utf-8")):
        out.setdefault((r["Data"], r["Metodo"]), []).append((_canon(r["Home"]), _canon(r["Away"])))
    return out


def estava_nas_0600(s06, data, metodo, home, away):
    """True/False, ou None se nao ha lista das 06:00 para esse dia."""
    from difflib import SequenceMatcher
    if not any(d == data for d, _ in s06): return None
    h, a = _canon(home), _canon(away)
    for ch, ca in s06.get((data, metodo), []):
        if (ch, ca) == (h, a): return True
        rh, ra = SequenceMatcher(None, h, ch).ratio(), SequenceMatcher(None, a, ca).ratio()
        if min(rh, ra) >= 0.60 and max(rh, ra) >= 0.80: return True
    return False
GAP_S = 90


def _f(x):
    try:
        v = float(x); return v if v > 0 else None
    except Exception:
        return None


def tg(msg):
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


class Estado:
    def __init__(self):
        self.offset = 0; self.buf = {}; self.buf_t0 = {}; self.avaliados = set(); self.top3 = {}; self.dia_top3 = None
        self.proximos = {}      # dia_local -> {(ko,home,away): {metodo: odd}} odds atuais dos elegiveis que ainda vao comecar
        self.avaliados_odd = {} # dia_local -> {metodo: [odds no KO dos ja avaliados]}
        if ESTADO.exists():
            try:
                e = json.load(open(ESTADO, encoding="utf-8")); self.offset = int(e.get("offset", 0))
                self.avaliados = set(e.get("avaliados", [])); self.top3 = e.get("top3", {}); self.dia_top3 = e.get("dia_top3")
            except Exception:
                pass

    def salvar(self):
        json.dump({"offset": self.offset, "avaliados": sorted(self.avaliados)[-3000:], "top3": self.top3, "dia_top3": self.dia_top3,
                   "salvo_em": datetime.now(timezone.utc).isoformat()}, open(ESTADO, "w", encoding="utf-8"))


def _esc(t): return str(t).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _flush(E, k, ts, mtk, mk, comp):
    ko, home, away = k
    kodt = datetime.strptime(ko, "%Y-%m-%d %H:%M") - timedelta(hours=3)   # hora LOCAL (Brasilia) do KO
    dia = kodt.strftime("%Y-%m-%d")
    if mtk > K.JANELA_KO[1]:
        # ainda vai comecar: guarda a odd atual dos elegiveis para o conjunto do dia local (ranking TOP 3)
        c = K.candidatos(mk, home, away, comp)
        E.proximos.setdefault(dia, {})[k] = {m: o for m, o in c.items() if o}
        return
    if not (K.JANELA_KO[0] <= mtk <= K.JANELA_KO[1]): return
    chave = "%s|%s|%s" % k
    if chave in E.avaliados: return
    E.avaliados.add(chave)
    E.proximos.get(dia, {}).pop(k, None)
    # conjunto do dia local = odds no KO dos ja avaliados + odd atual dos proximos (sem este jogo)
    conj = {}
    for m in (K.M_0X3, K.M_2X2):
        conj[m] = list(E.avaliados_odd.get(dia, {}).get(m, [])) + [o for g in E.proximos.get(dia, {}).values() for mm, o in g.items() if mm == m]
    sinais = K.avaliar(mk, home, away, comp, conj)
    c = K.candidatos(mk, home, away, comp)
    for m, o in c.items():
        if o: E.avaliados_odd.setdefault(dia, {}).setdefault(m, []).append(o)
    for d in [d for d in list(E.proximos) if d < dia]: E.proximos.pop(d, None); E.avaliados_odd.pop(d, None)
    if not sinais: return
    novo = not LEDGER.exists(); s06 = sinais_0600(); dstr = kodt.strftime("%Y-%m-%d")
    tags = {}
    with open(LEDGER, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if novo: w.writerow(COLS)
        for metodo, odd, liq, fav in sinais:
            be = (odd - 1) / (odd - 0.05)
            e6 = estava_nas_0600(s06, dstr, metodo, home, away); tags[metodo] = e6
            w.writerow([dstr, metodo, comp, home, away, kodt.strftime("%H:%M"), round(odd, 2), round(fav, 2) if fav else "",
                        round(liq, 0), round(mtk, 1), ts, "live", "PENDENTE", "", "", "", "", "", "", round(be, 4), "", "", "",
                        "" if e6 is None else ("sim" if e6 else "nao")])
    linhas = ["<b>KO−10 · %s</b> %s x %s" % (kodt.strftime("%H:%M"), _esc(home), _esc(away)), "<i>%s</i>" % _esc(comp)]
    for metodo, odd, liq, fav in sinais:
        e6 = tags.get(metodo); tag = "" if e6 is None else ("  ✔ 06:00" if e6 else "  ⚠ <b>novo no KO</b>")
        linhas.append("• <b>%s</b> — lay @<b>%.2f</b> (liq %.0f) · fav %.2f%s" % (_esc(metodo), odd, liq, fav or 0, tag))
    tg("\n".join(linhas))
    print("%s SINAL %s x %s: %s" % (datetime.now().strftime("%H:%M:%S"), home, away, ", ".join("%s@%.2f" % (m, o) for m, o, _, _ in sinais)), flush=True)


def passagem(E):
    if not COLETOR.exists(): return 0
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
            if p[1] == "CORRECT_SCORE" and p[7] not in CS_RUNNERS: continue
            try: mtk = float(p[6])
            except Exception: continue
            if mtk < K.JANELA_KO[0] - 1 or mtk > 400: continue     # perto do KO (avalia) ou ate ~6h antes (conjunto do dia)
            k = (p[5], p[3], p[4]); ts = p[0]
            try: te = datetime.strptime(ts[:19], "%Y-%m-%d %H:%M:%S").timestamp()
            except Exception: continue
            if k in E.buf and te - E.buf_t0[k] > GAP_S:
                bts, bmtk, bmk, bcomp = E.buf.pop(k); E.buf_t0.pop(k); _flush(E, k, bts, bmtk, bmk, bcomp)
            if k not in E.buf: E.buf[k] = (ts, mtk, {}, p[2]); E.buf_t0[k] = te
            E.buf[k][2].setdefault(p[1], {})[p[7]] = (_f(p[9]), _f(p[10]) or 0.0, _f(p[11]), _f(p[12]) or 0.0)
            n += 1
    agora = time.time()
    for k in list(E.buf):
        if agora - E.buf_t0[k] > GAP_S:
            bts, bmtk, bmk, bcomp = E.buf.pop(k); E.buf_t0.pop(k); _flush(E, k, bts, bmtk, bmk, bcomp)
    E.salvar(); return n


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--once", action="store_true"); ap.add_argument("--loop", type=int, default=0); a = ap.parse_args()
    E = Estado()
    if E.offset == 0 and COLETOR.exists():
        E.offset = max(0, COLETOR.stat().st_size - 20 * 1024 * 1024)
        with open(COLETOR, "rb") as f: f.seek(E.offset); f.readline(); E.offset = f.tell()
    if a.loop <= 0: n = passagem(E); print("linhas %d | avaliados %d" % (n, len(E.avaliados))); return
    print("=== SINAIS KO-10 (5 metodos, odd real do coletor) === loop %ds" % a.loop, flush=True)
    while True:
        try: passagem(E)
        except Exception as e: print("erro: %s" % str(e)[:200], flush=True)
        time.sleep(a.loop)


if __name__ == "__main__":
    main()
