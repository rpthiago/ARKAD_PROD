#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tracker_favorito_dominante_inplay.py — Rastreador In-Play de Back Favorito Dominante.

MÉTODO PRÉ-REGISTRADO: PRE_REGISTRO_FAVORITO_DOMINANTE.md
STATUS: OBSERVACAO_STAKE_ZERO (stake: 0.0)

GATILHO:
- Minuto entre 30' e 70'.
- Favorito pré-jogo perdendo por 1 gol (0x1, 1x2) OU empatando (0x0, 1x1 após min 40).
- Dominância estatística Opta ao vivo:
  * xG_fav >= 1.00
  * xG_fav >= 2.5 * xG_zebra
  * Chutes no alvo_fav >= 3
  * Toques na área_fav >= 15
- Liquidação pós-jogo via agenda do dia (get-matches-by-date).
"""

import os, sys, re, csv, json, time, argparse, subprocess, unicodedata
from datetime import datetime, timezone, timedelta
import urllib.request, urllib.parse, urllib.error

AQUI = os.path.dirname(os.path.abspath(__file__))
LOG_CSV = os.path.join(AQUI, "favorito_dominante_log.csv")
HOST = "free-api-live-football-data.p.rapidapi.com"

COLS = [
    "data_utc", "match_id", "league_id", "home", "away",
    "fav_side", "fav_team", "minuto", "placar_entrada",
    "odd_pre_fav", "odd_inplay_real", "odd_inplay_est", "chave", "xg_fav", "xg_und", "razao_xg",
    "sot_fav", "sot_und", "tbox_fav", "tbox_und", "bigch_fav",
    "poss_fav", "tipo_gatilho", "status", "placar_ft", "resultado_fav", "pnl_simulado"
]

TETO_DIA = 100
_gasto = {"dia": "", "n": 0}

def load_key():
    k = os.environ.get("RAPIDAPI_KEY")
    if k:
        return k
    for p in (
        os.path.join(AQUI, ".rapidapi_key"),
        os.path.expanduser("~/.rapidapi_key"),
        os.path.join(AQUI, "alerta.env")
    ):
        if os.path.exists(p):
            for line in open(p, encoding="utf-8", errors="ignore"):
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    k_name, val = line.split("=", 1)
                    if k_name.strip() in ("RAPIDAPI_KEY", "FOTMOB_API_KEY", "RAPID_KEY"):
                        return val.strip().strip("'\"")
                elif len(line) > 20 and not line.startswith("["):
                    return line
    return None

def _pode_gastar():
    hoje = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if _gasto["dia"] != hoje:
        _gasto.update(dia=hoje, n=0)
    return _gasto["n"] < TETO_DIA

def api_get(path, key, retries=2):
    if not _pode_gastar():
        return None
    url = f"https://{HOST}/{path}"
    req = urllib.request.Request(url, headers={"x-rapidapi-host": HOST, "x-rapidapi-key": key})
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=25) as resp:
                _gasto["n"] += 1
                rest = resp.headers.get("X-RateLimit-Requests-Remaining")
                if rest and int(rest) <= 0:
                    print("  [COTA ZERADA] pausando o dia (nada e tratado como liga sem dado).")
                    _gasto["n"] = TETO_DIA
                    return None
                if rest and int(rest) < 800:
                    print(f"  [PARADA SEGURA] Cota mensal baixa ({rest}) — pausando.")
                    _gasto["n"] = TETO_DIA
                    return None
                return json.loads(resp.read().decode())
        except Exception:
            if attempt < retries - 1:
                time.sleep(2)
                continue
            return None

def extrair_minuto(live_time_obj):
    if not live_time_obj:
        return None
    s = live_time_obj.get("short", "") or live_time_obj.get("long", "")
    m = re.search(r"(\d+)", str(s))
    return int(m.group(1)) if m else None

def parse_num(val):
    if val is None:
        return 0.0
    s = str(val).split("(")[0].strip().replace("%", "")
    try:
        return float(s)
    except:
        return 0.0

def extrair_stats(eventid, key):
    res = api_get(f"football-get-match-all-stats?eventid={eventid}", key)
    if not res or res.get("status") != "success":
        return None
    out = {}
    for grupo in (res.get("response", {}) or {}).get("stats", []) or []:
        for st in grupo.get("stats", []) or []:
            k, v = st.get("key"), st.get("stats")
            if k and v and v[0] is not None and k not in out:
                out[k] = v
    return out if out else None

def carregar_log():
    if not os.path.exists(LOG_CSV):
        return {}
    registros = {}
    try:
        with open(LOG_CSV, mode="r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for r in reader:
                registros[str(r["match_id"])] = r
    except Exception as e:
        print(f"Erro lendo {LOG_CSV}: {e}")
    return registros

def salvar_log(registros):
    with open(LOG_CSV, mode="w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLS)
        writer.writeheader()
        for r in registros.values():
            writer.writerow(r)

def liquidar_pendentes(registros, key):
    pendentes = [r for r in registros.values() if r["status"] == "PENDENTE"]
    if not pendentes:
        return
    datas = sorted({str(p.get("data_utc", ""))[:10].replace("-", "") for p in pendentes})
    datas = [d for d in datas if len(d) == 8][-3:]
    placares = {}
    for dia in datas:
        ag = api_get(f"football-get-matches-by-date?date={dia}", key)
        if not ag:
            continue
        for m in (ag.get("response", {}) or {}).get("matches", []) or []:
            st = m.get("status", {}) or {}
            if st.get("finished"):
                gh = (m.get("home") or {}).get("score")
                ga = (m.get("away") or {}).get("score")
                if gh is not None and ga is not None:
                    placares[str(m.get("id"))] = (int(gh), int(ga))
    
    atualizou = False
    for pr in pendentes:
        par = placares.get(str(pr["match_id"]))
        if not par:
            continue
        gh_ft, ga_ft = par
        fav_side = pr["fav_side"]
        pr["placar_ft"] = f"{gh_ft}-{ga_ft}"
        
        # Venceu se o lado do favorito marcou mais gols FT
        if fav_side == "HOME":
            fav_ganhou = gh_ft > ga_ft
        else:
            fav_ganhou = ga_ft > gh_ft
            
        pr["resultado_fav"] = "VENCEU" if fav_ganhou else "NAO_VENCEU"
        odd_est = float(pr.get("odd_inplay_real") or pr.get("odd_inplay_est") or 1.90)
        
        # P&L com 5% de comissão no Green:
        if fav_ganhou:
            pr["pnl_simulado"] = round((odd_est - 1.0) * 0.95, 3)
            res_txt = f"GREEN (+{pr['pnl_simulado']}u)"
        else:
            pr["pnl_simulado"] = -1.000
            res_txt = "RED (-1.0u)"
            
        pr["status"] = "LIQUIDADO"
        atualizou = True
        print(f"  [LIQUIDAÇÃO FAVORITO] {pr['fav_team']} ({pr['fav_side']}) | FT: {pr['placar_ft']} | {res_txt}")
        
    if atualizou:
        salvar_log(registros)

# ----------------------------------------------------------------------------------
# DETECCAO PELO COLETOR BETFAIR LOCAL (custo ZERO de API) + ODD REAL DE MERCADO.
# Motivo: `football-current-live` a cada 180s custa ~480 req/dia e so ha ~254/dia de cota
# no mes; e a odd in-play estava CHUTADA no codigo (2,30/1,85), o que viola a linha do
# Hall of Shame "assumir odd in-play sem medir". O coletor ja tem tudo, de graca.
# ----------------------------------------------------------------------------------
COLL = "/home/ubuntu/betfair-collector/betfair_live_odds.csv"
PREJOGO = os.path.join(AQUI, "favdom_prejogo.csv")     # cache da odd pre-jogo por jogo
AGENDA_DIR = os.path.join(AQUI, "hist_agenda")
MTK_LO, MTK_HI = -85, -45                              # minuto 30 a 70
FAV_MAX_PRE = 1.65                                     # <-- REGRA DO PRE-REGISTRO, faltava no codigo
RE_CS = re.compile(r"^\s*(\d+)\s*-\s*(\d+)\s*$")


def _cn(x):
    x = unicodedata.normalize("NFKD", str(x)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", x)


def _f(x, d=None):
    try:
        return float(x) if x not in (None, "") else d
    except Exception:
        return d


def _load_prejogo():
    c = {}
    if os.path.exists(PREJOGO):
        for r in csv.DictReader(open(PREJOGO, encoding="utf-8")):
            c[(r["ko"], r["home"], r["away"])] = (float(r["mtk"]), float(r["odd_h"]), float(r["odd_a"]))
    return c


def _save_prejogo(c):
    corte = (datetime.now(timezone.utc) - timedelta(days=3)).strftime("%Y-%m-%d")
    with open(PREJOGO, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f); w.writerow(["ko", "home", "away", "mtk", "odd_h", "odd_a"])
        for (ko, h, a), (mtk, oh, oa) in c.items():
            if ko[:10] >= corte:
                w.writerow([ko, h, a, mtk, oh, oa])


def _agenda(dia, key):
    """eventid de todas as partidas da data. 1 requisicao, cacheada em disco."""
    os.makedirs(AGENDA_DIR, exist_ok=True)
    fp = os.path.join(AGENDA_DIR, "%s.json" % dia)
    if os.path.exists(fp):
        return json.load(open(fp, encoding="utf-8"))
    d = api_get("football-get-matches-by-date?date=%s" % dia, key)
    if not d:
        return []
    out = []
    for m in (d.get("response", {}) or {}).get("matches", []) or []:
        st = m.get("status", {}) or {}
        out.append(dict(id=str(m.get("id")), home=(m.get("home") or {}).get("name", ""),
                        away=(m.get("away") or {}).get("name", ""),
                        ko=(st.get("utcTime") or "")[:16]))
    json.dump(out, open(fp, "w", encoding="utf-8"))
    return out


def _nomes_batem(a, b):
    x, y = _cn(a), _cn(b)
    if not x or not y:
        return False
    return x == y or (len(x) >= 5 and len(y) >= 5 and (x in y or y in x))


def candidatos_do_coletor(cache_pre):
    """Le o coletor Betfair: jogos no minuto 30-70, placar, odd de back AO VIVO de cada lado,
    e a odd PRE-JOGO (do cache). Zero requisicao a API."""
    if not os.path.exists(COLL):
        print("  [coletor ausente] %s" % COLL, flush=True)
        return []
    cutoff = (datetime.now() - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
    awk = (r'BEGIN{FS=","} $1>="%s" {'
           r'if ($2=="MATCH_ODDS" && $10!="") {'
           r'  if (($7+0)>0 && ($7+0)<=240) print "P|"$4"|"$5"|"$6"|"$7"|"$8"|"$10; '
           r'  else if (($7+0)<=%d && ($7+0)>=%d) print "L|"$4"|"$5"|"$6"|"$7"|"$8"|"$10 } '
           r'else if ($2=="CORRECT_SCORE" && $12!="" && ($7+0)<=%d && ($7+0)>=%d) '
           r'  print "C|"$4"|"$5"|"$6"|"$7"|"$8"|"$12}') % (cutoff, MTK_HI, MTK_LO, MTK_HI, MTK_LO)
    try:
        out = subprocess.run("tail -80000 %s | awk '%s'" % (COLL, awk),
                             shell=True, capture_output=True, text=True, timeout=60)
    except Exception as e:
        print("  [coletor erro]", str(e)[:60], flush=True)
        return []
    live, cs = {}, {}
    for ln in out.stdout.splitlines():
        pr = ln.split("|")
        if len(pr) != 7:
            continue
        tag, h, a, ko, mtk, runner, val = pr
        mtk, val = _f(mtk), _f(val)
        if mtk is None or val is None:
            continue
        k = (ko[:16], h, a)
        if tag == "P":
            cur = cache_pre.get(k)
            # [11/09] '<=' e nao '<': Home e Away chegam em linhas separadas com o MESMO mtk;
            # com '<' estrito o segundo runner era descartado e odd_a ficava 0.0 para sempre
            # (413 de 413 linhas do cache sem odd_a -> favorito visitante nunca detectado).
            if cur is None or mtk <= cur[0]:
                oh = val if runner == h else (cur[1] if cur else 0.0)
                oa = val if runner == a else (cur[2] if cur else 0.0)
                cache_pre[k] = (mtk, oh, oa)
        elif tag == "L":
            g = live.setdefault(k, {"mtk": mtk})
            if abs(mtk + 65) <= abs(g["mtk"] + 65):
                g["mtk"] = mtk
            if runner == h: g["back_h"] = val
            elif runner == a: g["back_a"] = val
        elif tag == "C":
            if RE_CS.match(runner) and 1.0 < val < 900:
                cur = cs.setdefault(k, {}).get(runner)
                if cur is None or abs(mtk + 65) < abs(cur[0] + 65):
                    cs[k][runner] = (mtk, val)
    cands = []
    for k, g in live.items():
        runners = cs.get(k)
        if not runners:
            continue
        mrec = min(v[0] for v in runners.values())
        cand = [(lay, r) for r, (m, lay) in runners.items() if m == mrec]
        if not cand:
            continue
        mm = RE_CS.match(min(cand)[1])
        gh, ga = int(mm.group(1)), int(mm.group(2))
        pre = cache_pre.get(k)
        if not pre:
            continue
        _, opre_h, opre_a = pre
        ko, h, a = k
        cands.append(dict(ko=ko, home=h, away=a, gh=gh, ga=ga,
                          minuto=int(-g["mtk"] - 15),
                          back_h=g.get("back_h"), back_a=g.get("back_a"),
                          pre_h=opre_h, pre_a=opre_a))
    return cands


def ciclo_monitoramento(key):
    registros = carregar_log()
    liquidar_pendentes(registros, key)

    cache_pre = _load_prejogo()
    cands = candidatos_do_coletor(cache_pre)
    _save_prejogo(cache_pre)
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    novos = 0

    fun = dict(vivos=len(cands), placar=0, fav=0, nao_vencendo=0, odd_ok=0, eid=0, stats=0, dominante=0)
    for c in cands:
        gh, ga, minuto = c["gh"], c["ga"], c["minuto"]

        # --- situacao de placar (regra do pre-registro) ---
        diff = abs(gh - ga)
        if diff > 1:
            continue
        if diff == 0 and not (40 <= minuto <= 65):
            continue
        fun["placar"] += 1

        # --- UNIVERSO DE FAVORITISMO: odd PRE-JOGO <= 1.65 -------------------------
        # Esta regra esta no pre-registro (secao 2.1) mas NAO estava implementada: o codigo
        # anterior chamava de "favorito" quem estivesse dominando AO VIVO, o que e outro metodo.
        if c["pre_h"] and c["pre_h"] <= FAV_MAX_PRE:
            fav_side, fav_team = "HOME", c["home"]
        elif c["pre_a"] and c["pre_a"] <= FAV_MAX_PRE:
            fav_side, fav_team = "AWAY", c["away"]
        else:
            continue
        fun["fav"] += 1

        # o favorito precisa estar PERDENDO por 1 ou EMPATANDO
        is_trailing = (gh < ga) if fav_side == "HOME" else (ga < gh)
        is_drawn = (gh == ga)
        if not (is_trailing or is_drawn):
            continue
        fun["nao_vencendo"] += 1

        # --- ODD REAL de back do favorito, medida no coletor (Lei no 1 / Hall of Shame) ---
        odd_real = c["back_h"] if fav_side == "HOME" else c["back_a"]
        if not odd_real or odd_real < 1.70:
            continue                      # regra 5 do pre-registro, agora com odd MEDIDA
        fun["odd_ok"] += 1

        chave = "%s|%s|%s" % (c["ko"], c["home"], c["away"])
        if any(r.get("chave") == chave for r in registros.values()):
            continue

        # --- resolve o eventid do FotMob pela agenda do dia (1 req/dia, cacheada) ---
        eid = ""
        try:
            dia = datetime.strptime(c["ko"][:10], "%Y-%m-%d")
        except Exception:
            continue
        for delta in (0, 1, -1):
            for m in _agenda((dia + timedelta(days=delta)).strftime("%Y%m%d"), key):
                if _nomes_batem(c["home"], m["home"]) and _nomes_batem(c["away"], m["away"]):
                    eid = m["id"]; break
            if eid:
                break
        if not eid:
            print("  [sem eventid] %s x %s" % (c["home"], c["away"]), flush=True)
            continue
        fun["eid"] += 1

        stats = extrair_stats(eid, key)
        if not stats:
            print("  [sem stats/cobertura] %s x %s" % (c["home"], c["away"]), flush=True)
            continue                      # 6a Lei: sem cobertura Opta, o jogo nao entra
        fun["stats"] += 1

        xg_h = parse_num((stats.get("expected_goals") or [0, 0])[0])
        xg_a = parse_num((stats.get("expected_goals") or [0, 0])[1])
        sot_h = parse_num((stats.get("ShotsOnTarget") or [0, 0])[0])
        sot_a = parse_num((stats.get("ShotsOnTarget") or [0, 0])[1])
        tbox_h = parse_num((stats.get("touches_opp_box") or [0, 0])[0])
        tbox_a = parse_num((stats.get("touches_opp_box") or [0, 0])[1])
        bigch_h = parse_num((stats.get("big_chance") or [0, 0])[0])
        bigch_a = parse_num((stats.get("big_chance") or [0, 0])[1])
        poss_h = parse_num((stats.get("BallPossesion") or [50, 50])[0])
        poss_a = parse_num((stats.get("BallPossesion") or [50, 50])[1])

        if fav_side == "HOME":
            xg_fav, xg_und = xg_h, xg_a
            sot_fav, sot_und = sot_h, sot_a
            tbox_fav, tbox_und = tbox_h, tbox_a
            bigch_fav, poss_fav = bigch_h, poss_h
        else:
            xg_fav, xg_und = xg_a, xg_h
            sot_fav, sot_und = sot_a, sot_h
            tbox_fav, tbox_und = tbox_a, tbox_h
            bigch_fav, poss_fav = bigch_a, poss_a

        # --- dominancia (thresholds do pre-registro, INALTERADOS) ---
        if not (xg_fav >= 1.0 and xg_fav >= 2.5 * max(xg_und, 0.1)
                and sot_fav >= 3 and tbox_fav >= 15):
            print("  [nao dominante] %s x %s %d-%d min%d | xG %.2f vs %.2f SoT %d tbox %d"
                  % (c["home"], c["away"], gh, ga, minuto, xg_fav, xg_und, sot_fav, tbox_fav), flush=True)
            continue
        fun["dominante"] += 1

        tipo_gatilho = "PERDENDO_POR_1" if is_trailing else "EMPATE_TARDIO"
        razao_xg = round(xg_fav / max(xg_und, 0.05), 2)
        odd_est = 2.30 if is_trailing else 1.85      # so p/ comparar com a odd REAL

        print("\n[GATILHO FAVORITO DOMINANTE - %s]" % tipo_gatilho)
        print("  Jogo: %s x %s (%d') | Placar: %d-%d" % (c["home"], c["away"], minuto, gh, ga))
        print("  Favorito PRE-JOGO: %s (%s) @ %.2f pre | odd de back AO VIVO MEDIDA: %.2f (estimada era %.2f)"
              % (fav_team, fav_side, c["pre_h"] if fav_side == "HOME" else c["pre_a"], odd_real, odd_est))
        print("  xG %.2f vs %.2f (razao %.2fx) | SoT %d vs %d | toques na area %d vs %d"
              % (xg_fav, xg_und, razao_xg, sot_fav, sot_und, tbox_fav, tbox_und))

        registros[eid] = {
            "data_utc": now_str, "match_id": eid, "chave": chave, "league_id": "",
            "home": c["home"], "away": c["away"], "fav_side": fav_side, "fav_team": fav_team,
            "minuto": minuto, "placar_entrada": "%d-%d" % (gh, ga),
            "odd_pre_fav": round(c["pre_h"] if fav_side == "HOME" else c["pre_a"], 2),
            "odd_inplay_real": round(odd_real, 2),
            "odd_inplay_est": odd_est,
            "xg_fav": xg_fav, "xg_und": xg_und, "razao_xg": razao_xg,
            "sot_fav": sot_fav, "sot_und": sot_und, "tbox_fav": tbox_fav, "tbox_und": tbox_und,
            "bigch_fav": bigch_fav, "poss_fav": poss_fav, "tipo_gatilho": tipo_gatilho,
            "status": "PENDENTE", "placar_ft": "", "resultado_fav": "", "pnl_simulado": "",
        }
        novos += 1

    if novos > 0:
        salvar_log(registros)
        print("  [+] %d novo(s) sinal(is) em %s" % (novos, LOG_CSV))
    if fun["vivos"]:
        print("  [funil %s] vivos=%d placar=%d fav<=1.65=%d nao_vencendo=%d back>=1.70=%d eid=%d stats=%d dominante=%d novos=%d"
              % ((now_str[11:16],) + tuple(fun.values()) + (novos,)), flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--intervalo", type=int, default=180)
    args = parser.parse_args()
    
    key = load_key()
    if not key:
        print("[ERRO] Chave RapidAPI não encontrada.")
        sys.exit(1)
        
    print("=" * 70)
    print("RASTREADOR IN-PLAY — BACK FAVORITO DOMINANTE (STAKE ZERO)")
    print(f"Log: {LOG_CSV}")
    print(f"Intervalo: {args.intervalo}s | Teto: {TETO_DIA} req/dia")
    print("=" * 70)
    
    if args.once:
        ciclo_monitoramento(key)
        return
        
    try:
        while True:
            ciclo_monitoramento(key)
            time.sleep(args.intervalo)
    except KeyboardInterrupt:
        print("\n[Encerrado pelo usuário]")

if __name__ == "__main__":
    main()
