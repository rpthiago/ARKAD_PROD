# -*- coding: utf-8 -*-
"""
liquidar_betfair_oficial.py — PLACAR OFICIAL de cada jogo, direto da liquidacao da Betfair.
==========================================================================================
O coletor (coletar_betfair_direto.py) grava ODDS e so enxerga cada mercado ate ~2,5h depois do
apito; ele nunca grava o status do runner. Reconstruir o placar pelo "runner de menor lay no fim"
erra 27% (medido em 11/09: 143 de 527, sempre com menos gols — o mercado suspende no gol tardio).

Este servico faz o que falta:
  1. Le market_map.csv (market_id -> jogo) a cada minuto e ACUMULA num registro proprio
     (o coletor sobrescreve o mapa a cada ciclo; aqui nada e esquecido).
  2. Para cada jogo com KO ha mais de 105 min, consulta listMarketBook SEM precos (barato) ate o
     mercado CORRECT_SCORE aparecer como CLOSED. Nesse momento a Betfair ja liquidou: o runner
     WINNER e o placar oficial ("2 - 1", ou "Any Other Home Win" quando > 3 gols de um lado).
  3. Grava UMA linha por jogo em placares_ft.csv com o vencedor de cada mercado que interessa
     (CS, Match Odds, Over/Under 0.5-3.5, BTTS, HT 0.5). E o resultado que a Betfair pagou, nao
     uma inferencia. Nunca reescreve linha ja gravada.
  4. Jogo sem CLOSED 30h depois do KO (adiado/anulado) e gravado como NAO_LIQUIDADO_30H.

Roda como systemd (liquidador-betfair), no venv do coletor. Somente leitura na Betfair.
"""
import os, sys, csv, json, time, re, argparse, datetime as dt
ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
import pandas as pd

MAPA = os.path.join(ROOT, "market_map.csv")
REG = os.path.join(ROOT, "liquidacao_registro.json")
OUT = os.path.join(ROOT, "placares_ft.csv")
TIPOS = ["CORRECT_SCORE", "MATCH_ODDS", "OVER_UNDER_05", "OVER_UNDER_15", "OVER_UNDER_25",
         "OVER_UNDER_35", "BOTH_TEAMS_TO_SCORE", "FIRST_HALF_GOALS_05"]
MIN_APOS_KO = 105        # minutos depois do KO para comecar a perguntar
DESISTE_H = 30           # horas depois do KO sem CLOSED -> adiado/anulado
COLS = ["settled_ts", "ko", "home", "away", "status", "cs_winner", "gh", "ga", "mo_winner",
        "ou05", "ou15", "ou25", "ou35", "btts", "ht05", "mercados_fechados", "fonte"]
RE_CS = re.compile(r"^\s*(\d+)\s*-\s*(\d+)\s*$")


def log(msg):
    print(dt.datetime.now().strftime("%H:%M:%S"), msg, flush=True)


def chave(ko, home, away):
    return "%s|%s|%s" % (ko, home, away)


def carregar_registro():
    if os.path.exists(REG):
        try:
            return json.load(open(REG, encoding="utf-8"))
        except Exception:
            pass
    return {}


def salvar_registro(reg):
    tmp = REG + ".tmp"
    json.dump(reg, open(tmp, "w", encoding="utf-8"), ensure_ascii=False)
    os.replace(tmp, REG)


def buscar_nomes(trading, reg, mids):
    """Nomes dos runners (selection_id -> nome) de mercados AINDA ABERTOS. Mercado fechado some do
    catalogo, por isso isto roda na entrada do jogo no registro, nunca depois."""
    from betfairlightweight import filters
    dono = {mid: k for k, e in reg.items() for t, m in e["markets"].items() if m in mids for mid in [m]}
    for i in range(0, len(mids), 25):
        chunk = mids[i:i + 25]
        try:
            cat = trading.betting.list_market_catalogue(
                filter=filters.market_filter(market_ids=chunk), max_results=25,
                market_projection=["RUNNER_DESCRIPTION"])
            for m in cat:
                k = dono.get(m.market_id)
                if k:
                    reg[k].setdefault("nomes", {})[m.market_id] = {str(r.selection_id): r.runner_name for r in (m.runners or [])}
        except Exception as ex:
            log("[nomes] erro: %s" % str(ex)[:70])
        time.sleep(0.4)


def nomes_do_coletor(home, away):
    """Fallback: selection_id -> runner a partir do CSV do coletor (ultimas ~300k linhas), para
    mercado que ja estava fechado quando entrou no registro (catalogo nao devolve mais)."""
    import subprocess
    csvp = os.path.join(ROOT, "betfair_live_odds.csv")
    if not os.path.exists(csvp):
        return {}
    awk = r'BEGIN{FS=","} $4==h && $5==a {print $2"|"$9"|"$8}'
    try:
        out = subprocess.run("tail -300000 %s | awk -v h=%s -v a=%s '%s'" % (csvp, json.dumps(home), json.dumps(away), awk),
                             shell=True, capture_output=True, text=True, timeout=60).stdout
    except Exception:
        return {}
    por_tipo = {}
    for ln in out.splitlines():
        pr = ln.split("|", 2)
        if len(pr) == 3:
            por_tipo.setdefault(pr[0], {})[pr[1]] = pr[2]
    return por_tipo


def ingerir_mapa(reg, trading=None):
    """Acumula os market_ids do mapa do coletor no registro. Nao remove nada."""
    if not os.path.exists(MAPA):
        return 0
    try:
        m = pd.read_csv(MAPA, dtype=str).fillna("")
    except Exception as e:
        log("[mapa] erro: %s" % str(e)[:60]); return 0
    novos = 0
    for _, r in m.iterrows():
        if r["market_type"] not in TIPOS or not r["ko"]:
            continue
        k = chave(r["ko"], r["home"], r["away"])
        e = reg.setdefault(k, dict(ko=r["ko"], home=r["home"], away=r["away"], markets={},
                                   first_seen=dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M"), status="PENDENTE"))
        if r["market_type"] not in e["markets"]:
            e["markets"][r["market_type"]] = r["market_id"]; novos += 1
    if trading is not None:
        sem_nome = [m for e in reg.values() if e.get("status") == "PENDENTE"
                    for m in e["markets"].values() if m not in e.get("nomes", {})]
        if sem_nome:
            buscar_nomes(trading, reg, sem_nome)
    return novos


def ja_gravados():
    s = set()
    if os.path.exists(OUT):
        for r in csv.DictReader(open(OUT, encoding="utf-8")):
            s.add(chave(r["ko"], r["home"], r["away"]))
    return s


def gravar(row):
    novo = not os.path.exists(OUT) or os.path.getsize(OUT) == 0
    with open(OUT, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLS)
        if novo: w.writeheader()
        w.writerow({c: row.get(c, "") for c in COLS})


def _winner(book, nomes):
    """Nome do runner WINNER de um book CLOSED (ou '' se nao houver)."""
    for r in (book.runners or []):
        if getattr(r, "status", "") == "WINNER":
            return nomes.get(str(r.selection_id)) or ("id:%s" % r.selection_id)
    return ""


def passar_liquidacao(trading, reg, gravados):
    agora = dt.datetime.utcnow()
    alvo = []
    for k, e in reg.items():
        if e.get("status") != "PENDENTE" or k in gravados:
            continue
        try:
            ko = dt.datetime.strptime(e["ko"], "%Y-%m-%d %H:%M")
        except Exception:
            continue
        if (agora - ko).total_seconds() / 60.0 >= MIN_APOS_KO:
            alvo.append((k, e, ko))
    if not alvo:
        return 0, 0
    ids = []
    for k, e, ko in alvo:
        for t, mid in e["markets"].items():
            if t in TIPOS:
                ids.append((k, t, mid))
    books = {}
    for i in range(0, len(ids), 25):
        chunk = [mid for _, _, mid in ids[i:i + 25]]
        try:
            for b in trading.betting.list_market_book(market_ids=chunk):
                books[b.market_id] = b
        except Exception as ex:
            log("[book] erro no lote: %s" % str(ex)[:70])
        time.sleep(0.4)
    liq = desist = 0
    for k, e, ko in alvo:
        mids = e["markets"]
        cs_id = mids.get("CORRECT_SCORE")
        cs_book = books.get(cs_id) if cs_id else None
        cs_closed = cs_book is not None and getattr(cs_book, "status", "") == "CLOSED"
        horas = (agora - ko).total_seconds() / 3600.0
        n_closed = sum(1 for t, mid in mids.items() if mid in books and getattr(books[mid], "status", "") == "CLOSED")
        todos_closed = n_closed == len(mids)
        # grava quando TODOS os mercados fecharam (a Betfair liquida cada um num momento), ou, se o
        # CS ja fechou, no maximo 4h depois do KO (grava o que houver; o resto fica em branco)
        if cs_closed and not todos_closed and horas < 4.0:
            continue
        if not cs_closed:
            if horas >= DESISTE_H:
                gravar(dict(settled_ts=agora.strftime("%Y-%m-%d %H:%M"), ko=e["ko"], home=e["home"], away=e["away"],
                            status="NAO_LIQUIDADO_30H", fonte="betfair_oficial"))
                e["status"] = "DESISTIDO"; gravados.add(k); desist += 1
            continue
        nomes = e.get("nomes", {})
        faltam = [mid for mid in mids.values() if mid not in nomes]
        if faltam:                                   # fallback: nomes pelo CSV do coletor
            pt = nomes_do_coletor(e["home"], e["away"])
            for t, mid in mids.items():
                if mid not in nomes and pt.get(t):
                    nomes[mid] = pt[t]
        def win(t):
            mid = mids.get(t); b = books.get(mid) if mid else None
            return _winner(b, nomes.get(mid, {})) if (b is not None and getattr(b, "status", "") == "CLOSED") else ""
        cs = win("CORRECT_SCORE")
        mm = RE_CS.match(cs or "")
        row = dict(settled_ts=agora.strftime("%Y-%m-%d %H:%M"), ko=e["ko"], home=e["home"], away=e["away"],
                   status="LIQUIDADO", cs_winner=cs, gh=int(mm.group(1)) if mm else "", ga=int(mm.group(2)) if mm else "",
                   mo_winner=win("MATCH_ODDS"), ou05=win("OVER_UNDER_05"), ou15=win("OVER_UNDER_15"),
                   ou25=win("OVER_UNDER_25"), ou35=win("OVER_UNDER_35"), btts=win("BOTH_TEAMS_TO_SCORE"),
                   ht05=win("FIRST_HALF_GOALS_05"),
                   mercados_fechados="%d/%d" % (sum(1 for t, mid in mids.items() if mid in books and getattr(books[mid], "status", "") == "CLOSED"), len(mids)),
                   fonte="betfair_oficial")
        gravar(row); e["status"] = "LIQUIDADO"; e.pop("nomes", None); gravados.add(k); liq += 1
        log("  LIQUIDADO %s x %s -> CS %s | MO %s | O/U2.5 %s" % (e["home"], e["away"], cs or "?", row["mo_winner"] or "?", row["ou25"] or "?"))
    # poda: liquidados/desistidos ha mais de 3 dias saem do registro
    corte = (agora - dt.timedelta(days=3)).strftime("%Y-%m-%d %H:%M")
    for k in [k for k, e in reg.items() if e.get("status") != "PENDENTE" and e.get("ko", "") < corte]:
        reg.pop(k, None)
    return liq, desist


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--intervalo", type=int, default=300, help="segundos entre passagens de liquidacao")
    a = ap.parse_args()
    import coletar_betfair_direto as C
    log("login na Betfair (somente leitura)...")
    trading = C.login()
    reg = carregar_registro(); gravados = ja_gravados()
    log("registro: %d jogos | placares ja gravados: %d" % (len(reg), len(gravados)))
    while True:
        t0 = time.time()
        try:
            n = ingerir_mapa(reg, trading)
            liq, des = passar_liquidacao(trading, reg, gravados)
            salvar_registro(reg)
            pend = sum(1 for e in reg.values() if e.get("status") == "PENDENTE")
            log("[passagem] mapa +%d mercados | liquidados %d | desistidos %d | pendentes %d" % (n, liq, des, pend))
        except Exception as ex:
            log("[erro] %s" % str(ex)[:120])
            if "SESSION" in str(ex).upper():
                try: trading = C.login(); log("re-login ok")
                except Exception as ex2: log("re-login falhou: %s" % str(ex2)[:80])
        if a.once:
            break
        # ingere o mapa a cada 60s entre passagens (o coletor sobrescreve a cada ciclo)
        prox = t0 + a.intervalo
        while time.time() + 60 < prox:
            time.sleep(60)
            try: ingerir_mapa(reg, trading)
            except Exception: pass
        time.sleep(max(0, prox - time.time()))


if __name__ == "__main__":
    main()
