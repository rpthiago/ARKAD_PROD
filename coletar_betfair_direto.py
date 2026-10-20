"""
coletar_betfair_direto.py — Coletor de odds AO VIVO direto da Betfair (API oficial).
SOMENTE DADOS — nenhuma ordem e enviada (nao chama place_orders). Independente do
provedor FutPythonTrader (que fica caindo em 503). Alimenta o paper + o painel de CLV,
inclusive o fechamento do CS 0x0 perto do apito (que o football-data nao tem).

Mercados capturados (back E lay, top do book):
  MATCH_ODDS (1X2), CORRECT_SCORE (0-0, 0-1, 1-0, goleadas/Any Other),
  OVER_UNDER_05 e OVER_UNDER_25 (FT), BOTH_TEAMS_TO_SCORE (BTTS), FIRST_HALF_GOALS_05 (Over0.5 HT).

Auth: reusa o .env local (BETFAIR_USERNAME/PASSWORD/APP_KEY/CERT_*) — as chaves ficam so
no seu ambiente. Endpoints Brasil (betfair.bet.br).

Uso:  python coletar_betfair_direto.py --once        # 1 ciclo (teste)
      python coletar_betfair_direto.py --intervalo 60  # loop a cada 60s (VPS)
Saida: betfair_live_odds.csv (append, timestamp + min ate o apito)
"""
from __future__ import annotations
import os, sys, time, argparse, datetime
import pandas as pd
import betfairlightweight as bl
from betfairlightweight import filters

ROOT = os.path.dirname(os.path.abspath(__file__))
ARQ = os.path.join(ROOT, "betfair_live_odds.csv")
MAPA = os.path.join(ROOT, "market_map.csv")   # mapa leve market_id->jogo p/ o alerta montar link direto
MKT_TYPES = ["MATCH_ODDS", "CORRECT_SCORE", "OVER_UNDER_05", "OVER_UNDER_15",
             "OVER_UNDER_25", "OVER_UNDER_35", "OVER_UNDER_45", "BOTH_TEAMS_TO_SCORE", "FIRST_HALF_GOALS_05"]
# OVER_UNDER_15/35 add p/ o metodo "Under-no-limite" (linha de under logo acima do placar):
# 1 gol -> Under 1.5; 3 gols -> Under 3.5. Ver PREREGISTRO_under_limite_inplay.md.


def _env():
    e = {}
    p = os.path.join(ROOT, ".env")
    if os.path.exists(p):
        for line in open(p, encoding="utf-8"):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1); e[k.strip()] = v.strip()
    for k in ["BETFAIR_USERNAME", "BETFAIR_PASSWORD", "BETFAIR_APP_KEY",
              "BETFAIR_CERT_CRT_PATH", "BETFAIR_CERT_PEM_PATH", "BETFAIR_PROXY"]:
        if os.getenv(k):
            e[k] = os.getenv(k)
    return e


def login():
    e = _env()
    for k in ["BETFAIR_USERNAME", "BETFAIR_PASSWORD", "BETFAIR_APP_KEY"]:
        if not e.get(k):
            print(f"ERRO: {k} ausente no .env"); sys.exit(1)
    
    proxy_url = e.get("BETFAIR_PROXY")
    session = None
    if proxy_url:
        import requests
        session = requests.Session()
        session.proxies = {"http": proxy_url, "https": proxy_url}

    t = bl.APIClient(e["BETFAIR_USERNAME"], e["BETFAIR_PASSWORD"], app_key=e["BETFAIR_APP_KEY"],
                     cert_files=(e["BETFAIR_CERT_CRT_PATH"], e["BETFAIR_CERT_PEM_PATH"]),
                     session=session)
    t.api_uri = "https://api.betfair.com/exchange/"
    t.identity_uri = "https://identitysso.betfair.com/api/"
    t.identity_cert_uri = "https://identitysso-cert.betfair.bet.br/api/"
    t.login()
    return t


def _best(lst):
    return float(lst[0].price) if lst else None


def _best_sz(lst):
    # tamanho (R$) disponivel no melhor preco = profundidade/liquidez daquele lado
    return float(lst[0].size) if lst else None


def salvar_mapa(meta):
    # mapa leve market_id -> jogo (o alerta usa p/ montar o link direto do mercado). Sobrescreve.
    try:
        rows = [dict(market_id=mid, market_type=m.get("market_type", ""), home=m.get("home", ""),
                     away=m.get("away", ""), ko=m.get("ko", "")) for mid, m in meta.items()]
        pd.DataFrame(rows).to_csv(MAPA, index=False)
    except Exception as e:
        print(f"  [mapa] erro: {str(e)[:60]}")


def _puxar_books(trading, meta, ids):
    """Puxa os books (odds top do book) de uma lista de market_ids -> lista de linhas."""
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    pp = filters.price_projection(price_data=["EX_BEST_OFFERS"],
                                  ex_best_offers_overrides=filters.ex_best_offers_overrides(best_prices_depth=1))
    rows = []
    for i in range(0, len(ids), 25):
        chunk = ids[i:i + 25]
        try:
            books = trading.betting.list_market_book(market_ids=chunk, price_projection=pp)
        except Exception as e:
            print(f"  [book] erro no lote: {str(e)[:60]}"); continue
        for b in books:
            md = meta.get(b.market_id, {})
            mtk = round((md["ko_dt"] - datetime.datetime.now(datetime.timezone.utc)).total_seconds() / 60.0, 1) if md.get("ko_dt") else None
            for r in (b.runners or []):
                atb = getattr(r.ex, "available_to_back", None) or []
                atl = getattr(r.ex, "available_to_lay", None) or []
                rows.append(dict(
                    capture_ts=ts, market_type=md.get("market_type", ""),
                    competition=md.get("competition", ""), home=md.get("home", ""),
                    away=md.get("away", ""), ko=md.get("ko", ""), min_to_ko=mtk,
                    runner=md.get("runners", {}).get(r.selection_id, str(r.selection_id)),
                    selection_id=r.selection_id,
                    back=_best(atb), back_size=_best_sz(atb),
                    lay=_best(atl), lay_size=_best_sz(atl),
                    ltp=r.last_price_traded, matched=r.total_matched))
        time.sleep(0.5)  # educado com a API
    return rows


def passar_ft(trading, meta, mtk_lo=-120.0, mtk_hi=-95.0):
    """Passagem RAPIDA: re-puxa SO os jogos na janela de fim (min ~80-105) p/ pegar gol tardio
    antes da Betfair fechar o mercado. Reusa o meta do ciclo cheio (nao refaz catalogo)."""
    agora = datetime.datetime.now(datetime.timezone.utc)
    ids = []
    for mid, md in meta.items():
        if not md.get("ko_dt"):
            continue
        mtk = (md["ko_dt"] - agora).total_seconds() / 60.0
        if mtk_lo <= mtk <= mtk_hi:
            ids.append(mid)
    if not ids:
        return pd.DataFrame(), 0
    return pd.DataFrame(_puxar_books(trading, meta, ids)), len(ids)


_CAT_CACHE = {"meta": {}, "ts": 0.0}


def _catalogo(trading, janela):
    """As 9 chamadas de catalogo (uma por tipo de mercado). Separado para ficar em cache."""
    meta = {}  # market_id -> dict(meta) + runners{selection_id: name}
    n_err = 0  # catalogos que falharam neste ciclo (p/ detectar sessao morta)
    # uma chamada de catalogo POR TIPO de mercado (evita MARKET_DESCRIPTION / TOO_MUCH_DATA
    # e ja sabemos o tipo de cada mercado)
    for mtype in MKT_TYPES:
        mf = filters.market_filter(event_type_ids=["1"], market_type_codes=[mtype], market_start_time=janela)
        try:
            cat = trading.betting.list_market_catalogue(
                filter=mf, max_results=500,
                market_projection=["EVENT", "COMPETITION", "MARKET_START_TIME", "RUNNER_DESCRIPTION"])
        except Exception as e:
            global ULTIMO_ERRO_CATALOGO
            ULTIMO_ERRO_CATALOGO = str(e)
            n_err += 1; print(f"  [catalogo {mtype}] erro: {str(e)[:60]}"); continue
        for m in cat:
            ev = m.event
            nome = (ev.name or "") if ev else ""
            home, away = (nome.split(" v ") + ["", ""])[:2] if " v " in nome else (nome, "")
            meta[m.market_id] = dict(
                market_type=mtype,
                competition=(m.competition.name if m.competition else ""),
                home=home.strip(), away=away.strip(),
                ko=(m.market_start_time.strftime("%Y-%m-%d %H:%M") if m.market_start_time else ""),
                ko_dt=m.market_start_time,
                runners={r.selection_id: r.runner_name for r in (m.runners or [])})
    return meta, n_err

def um_ciclo(trading, horas=6.0, horas_atras=2.5, sem_meio=False, meio_lo=-95.0, catalogo_cada=30.0):
    agora = datetime.datetime.now(datetime.timezone.utc)
    # janela para TRAS tambem: pega jogos ja em andamento (apito ate ~2.5h atras cobre
    # 90min + intervalo + acrescimos) -> min_to_ko fica NEGATIVO (minuto do jogo). E isso
    # que permite medir a odd AO VIVO nos minutos 20-30 (Lay 0x0 / Over 0.5 in-play).
    inicio = agora - datetime.timedelta(hours=horas_atras)
    fim = agora + datetime.timedelta(hours=horas)
    janela = {"from": inicio.strftime("%Y-%m-%dT%H:%M:%SZ"), "to": fim.strftime("%Y-%m-%dT%H:%M:%SZ")}
    # catalogo em CACHE: a lista de jogos nao muda a cada 5 min. Sem isso sao 9 chamadas
    # por ciclo (2.592/dia) e o custo do proxy esta no NUMERO de chamadas, nao no tamanho.
    global _CAT_CACHE
    idade = time.time() - _CAT_CACHE["ts"]
    if catalogo_cada > 0 and _CAT_CACHE["meta"] and idade < catalogo_cada * 60.0:
        meta = _CAT_CACHE["meta"]; n_err = 0; cache = True
        print("  [catalogo em cache] %d mercados | renova em %.0f min" % (
              len(meta), (catalogo_cada * 60.0 - idade) / 60.0))
    else:
        meta, n_err = _catalogo(trading, janela)
        cache = False
        if meta:
            _CAT_CACHE = {"meta": meta, "ts": time.time()}
    # com catalogo em cache, o filtro de janela tem de ser feito aqui (a API nao filtrou agora)
    ids = []
    for mid, md in meta.items():
        kd = md.get("ko_dt")
        if kd is None:
            ids.append(mid); continue
        mtk = (kd - agora).total_seconds() / 60.0
        if -horas_atras * 60.0 <= mtk <= horas * 60.0:
            ids.append(mid)
    salvar_mapa(meta)
    # sem_meio: nao puxa book no MEIO do jogo (mtk entre 0 e meio_lo). O catalogo continua
    # olhando para tras, senao a passagem de fim de jogo (passar_ft) perderia o meta dela.
    if sem_meio:
        agora2 = datetime.datetime.now(datetime.timezone.utc)
        manter = []
        for mid in ids:
            kd = meta[mid].get('ko_dt')
            if not kd:
                manter.append(mid); continue
            mtk = (kd - agora2).total_seconds() / 60.0
            if mtk >= 0 or mtk <= meio_lo:
                manter.append(mid)
        pulados = len(ids) - len(manter)
        ids = manter
        if pulados:
            print('  [sem-meio-jogo] %d mercados em andamento pulados' % pulados)
    rows = _puxar_books(trading, meta, ids)
    if cache and ids and not rows:
        # book vazio com catalogo em cache = sessao provavelmente morta. Invalida o cache e
        # sinaliza n_err para o loop refazer o login e repetir o ciclo na hora.
        _CAT_CACHE["ts"] = 0.0
        n_err = 1
    return pd.DataFrame(rows), n_err, meta


def salvar(df):
    if df.empty:
        print("  (0 linhas neste ciclo)"); return 0
    # APPEND puro: deduplica so DENTRO do ciclo (mesmo minuto) e anexa. NUNCA le o
    # arquivo inteiro — o antigo read_csv+concat de um CSV grande (128MB) expandia p/
    # ~580MB na RAM e estourava o VM de 1GB (OOM/SIGKILL, loop de crash). Memoria agora
    # e O(ciclo), nao O(arquivo). Capturas do mesmo jogo em minutos diferentes sao a
    # serie temporal desejada (nao duplicatas).
    df = df.copy()
    df["_slot"] = df["capture_ts"].astype(str).str[:16]
    df = df.drop_duplicates(["_slot", "market_type", "home", "away", "runner"], keep="last").drop(columns="_slot")
    exists = os.path.exists(ARQ) and os.path.getsize(ARQ) > 0
    if exists:
        # se o schema (cabecalho) mudou — ex.: adicionamos back_size/lay_size — anexar
        # linhas novas sob o header antigo desalinha as colunas. Arquiva e comeca CSV novo.
        with open(ARQ, encoding="utf-8") as f:
            old_header = f.readline().strip()
        if old_header != ",".join(df.columns):
            bak = ARQ.replace(".csv", "_schema_%s.csv" % datetime.datetime.now().strftime("%Y%m%d"))
            os.rename(ARQ, bak)
            print("  schema mudou -> arquivado %s, iniciando CSV novo" % os.path.basename(bak))
            exists = False
    df.to_csv(ARQ, mode="a", header=not exists, index=False)
    return len(df)



# --------------------------------------------------------------- DISJUNTOR DE BLOQUEIO
# Desde 07/10/2026 a Betfair responde uma pagina HTML (bloqueio da MP 1.394/26) a IPs brasileiros.
# Sem isto o loop refaz login a cada ciclo, e login falhando em repeticao pode travar a conta.
BLOQUEIO_MARCAS = ("<!doctype html", "<html", "brasilsembets", "medida provisoria", "acesso bloqueado")
ULTIMO_ERRO_CATALOGO = ""


def _parece_bloqueio(msg):
    m = str(msg).lower()
    return any(x in m for x in BLOQUEIO_MARCAS)


# O proxy residencial e pre-pago: quando o saldo acaba ele recusa o CONNECT e o sintoma fica
# IGUAL ao do bloqueio (0 cotacoes, erro na API). Estas marcas separam os dois casos.
PROXY_MARCAS = ("proxyerror", "proxy authentication", "tunnel connection failed",
                "cannot connect to proxy", "unable to connect to proxy", "407 proxy",
                "forbidden by proxy", "proxy_auth")
_AVISO_PROXY = {
    "ts": 0.0,
    "falhas_consecutivas": 0,
    "min_falhas_alerta": 3
}


def _parece_proxy(msg):
    m = str(msg).lower()
    return any(x in m for x in PROXY_MARCAS)


def avisar_proxy(msg):
    """Avisa no Telegram SOMENTE se o proxy falhar 3 vezes consecutivas (~15 min sem dados).
    Evita falsos alarmes por oscilacao momentanea de rotacao de IP residencial."""
    _AVISO_PROXY["falhas_consecutivas"] += 1
    falhas = _AVISO_PROXY["falhas_consecutivas"]
    agora = time.time()

    if falhas < _AVISO_PROXY["min_falhas_alerta"]:
        print(f"  [PROXY] Falha transitoria detectada ({falhas}/{_AVISO_PROXY['min_falhas_alerta']}). "
              f"Aguardando autorrecuperacao sem alarmar Telegram...", flush=True)
        return

    if agora - _AVISO_PROXY["ts"] < 6 * 3600:
        return
    _AVISO_PROXY["ts"] = agora
    texto = ("🚨 ARKAD: PROXY instavel ou sem saldo." + chr(10) +
             f"Falha confirmada por {falhas} tentativas consecutivas (~15 min sem dados)." + chr(10) +
             "Causa provavel: acabou o saldo de trafego do iProyal." + chr(10) +
             "Erro: " + str(msg)[:160])
    print(f"  [PROXY] {falhas} falhas seguidas confirmadas -> avisando no Telegram", flush=True)
    try:
        import urllib.parse, urllib.request
        tok = chat = None
        env = os.path.join(ROOT, "alerta.env")
        if os.path.exists(env):
            for ln in open(env, encoding="utf-8"):
                if ln.startswith("TG_TOKEN="): tok = ln.split("=", 1)[1].strip()
                elif ln.startswith("TG_CHAT="): chat = ln.split("=", 1)[1].strip()
        if not (tok and chat):
            print("  [PROXY] Telegram nao configurado em alerta.env", flush=True); return
        dados = urllib.parse.urlencode({"chat_id": chat, "text": texto}).encode()
        req = urllib.request.Request("https://api.telegram.org/bot" + tok + "/sendMessage", data=dados)
        urllib.request.urlopen(req, timeout=15)
    except Exception as e:
        print("  [PROXY] aviso no Telegram falhou: " + str(e)[:80], flush=True)


class Disjuntor(object):
    """espera 5min, 15, 30, 60, 120 (teto) entre tentativas enquanto a API responder bloqueio."""
    ESPERAS = (300, 900, 1800, 3600, 7200)

    def __init__(self):
        self.falhas = 0

    def abrir(self):
        espera = self.ESPERAS[min(self.falhas, len(self.ESPERAS) - 1)]
        self.falhas += 1
        print("  [DISJUNTOR] API respondendo bloqueio (nao e sessao expirada). Sem re-login. "
              "Nova tentativa em %d min. Falhas seguidas: %d" % (espera // 60, self.falhas), flush=True)
        return espera

    def fechar(self):
        if self.falhas:
            print("  [DISJUNTOR] API respondeu normalmente de novo — contador zerado.", flush=True)
        self.falhas = 0
        if _AVISO_PROXY["falhas_consecutivas"] > 0:
            print("  [PROXY] Conexao reestabelecida com sucesso! Falhas consecutivas zeradas.", flush=True)
            _AVISO_PROXY["falhas_consecutivas"] = 0


def login_resiliente(disj):
    """Login que nunca derruba o processo: qualquer falha espera pelo disjuntor (5 a 120 min).
    Sem isso, falha no login de partida matava o processo e o systemd tentava de novo a cada 30 s:
    com proxy sem saldo era so log inchando; com a Betfair recusando o login, seria login em
    repeticao -- o padrao que pode travar a conta."""
    while True:
        try:
            t = login()
            disj.fechar()
            return t
        except Exception as e:
            print("  login falhou: " + str(e)[:160], flush=True)
            if _parece_proxy(e):
                avisar_proxy(e)
            time.sleep(disj.abrir())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="1 ciclo so (teste)")
    ap.add_argument("--intervalo", type=int, default=60, help="segundos entre ciclos (loop)")
    ap.add_argument("--horas", type=float, default=6.0, help="so jogos que comecam nas proximas N horas (leve/barato)")
    ap.add_argument("--horas-atras", type=float, default=2.5, help="tambem captura jogos que ja comecaram ate N horas atras (in-play) — 0 desliga")
    ap.add_argument("--ft-intervalo", type=int, default=45, help="segundos entre passagens RAPIDAS na janela de fim de jogo (min ~80-105), p/ pegar gol tardio; 0 desliga")
    ap.add_argument("--sem-meio-jogo", action="store_true",
                    help="nao captura o MEIO do jogo (min 0-80); mantem pre-jogo e fim de jogo")
    ap.add_argument("--catalogo-cada", type=float, default=30.0,
                    help="minutos entre buscas da LISTA de jogos (catalogo); 0 = a cada ciclo")
    args = ap.parse_args()

    print("Autenticando na Betfair (cert)...")
    disj = Disjuntor()
    trading = login_resiliente(disj)
    print("OK — login realizado (SOMENTE DADOS, nenhuma ordem sera enviada).")

    ciclo = 0
    while True:
        ciclo += 1
        t0 = time.time()
        meta = {}
        try:
            df, n_err, meta = um_ciclo(trading, args.horas, args.horas_atras, args.sem_meio_jogo, catalogo_cada=args.catalogo_cada)
            n = salvar(df)
            mkts = df[["home", "away"]].drop_duplicates().shape[0] if not df.empty else 0
            print(f"[ciclo {ciclo}] {n} cotacoes | {mkts} jogos | {df['market_type'].nunique() if not df.empty else 0} mercados | {time.time()-t0:.0f}s")
            # todos os catalogos falharam e nada veio -> sessao provavelmente morta: refaz login
            if n_err and df.empty:
                if _parece_proxy(ULTIMO_ERRO_CATALOGO):
                    avisar_proxy(ULTIMO_ERRO_CATALOGO)
                    time.sleep(disj.abrir())
                    continue
                if _parece_bloqueio(ULTIMO_ERRO_CATALOGO):
                    time.sleep(disj.abrir())
                    continue
                print("  catalogo falhou em todos os mercados -> refazendo login (sessao pode ter expirado)")
                try:
                    trading = login()
                    # repete o ciclo JA: sem isso o buraco e de um intervalo inteiro (5 min).
                    # A sessao morre a cada troca de IP do proxy (sticky lifetime-30m).
                    df, n_err, meta = um_ciclo(trading, args.horas, args.horas_atras, args.sem_meio_jogo, catalogo_cada=args.catalogo_cada)
                    n = salvar(df)
                    mkts = df[["home", "away"]].drop_duplicates().shape[0] if not df.empty else 0
                    nmk = df["market_type"].nunique() if not df.empty else 0
                    print(f"[ciclo {ciclo}r] {n} cotacoes | {mkts} jogos | {nmk} mercados "
                          f"| {time.time()-t0:.0f}s (repetido na hora apos re-login)")
                    if n:
                        disj.fechar()
                except Exception as e2:
                    print(f"  re-login falhou: {str(e2)[:80]}")
            elif n:
                disj.fechar()
        except Exception as e:
            print(f"[ciclo {ciclo}] ERRO: {str(e)[:100]}")
            if _parece_proxy(e):
                avisar_proxy(e)
                time.sleep(disj.abrir())
                continue
            if _parece_bloqueio(e):
                time.sleep(disj.abrir())
                continue
            if "INVALID_SESSION" in str(e).upper() or "NO_SESSION" in str(e).upper():
                print("  sessao expirou -> re-login"); trading = login_resiliente(disj)
        if args.once:
            break
        # passagens RAPIDAS na janela de fim de jogo ate o proximo ciclo cheio (pega gol tardio)
        prox = t0 + max(5, args.intervalo)
        if args.ft_intervalo and meta:
            while time.time() + args.ft_intervalo < prox:
                time.sleep(args.ft_intervalo)
                try:
                    dff, nft = passar_ft(trading, meta)
                    if not dff.empty:
                        nn = salvar(dff)
                        print(f"  [ft-rapido] {nn} cotacoes | {nft} jogos no fim")
                except Exception as e:
                    print(f"  [ft-rapido] erro: {str(e)[:80]}")
        resto = prox - time.time()
        if resto > 0:
            time.sleep(resto)
    print(f"Saida: {os.path.basename(ARQ)}")


if __name__ == "__main__":
    main()
