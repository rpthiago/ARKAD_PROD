# -*- coding: utf-8 -*-
"""
xg_backfill.py — preenche as estatisticas que faltam na base (xG, xGOT, chutes, posse, escanteios,
grandes chances, defesas) usando a API que voce JA paga (free-api-live-football-data no RapidAPI).
Roda na VPS, so com a biblioteca padrao (o python3 do sistema nao tem pandas).

  python3 xg_backfill.py --max 500 [--reserva 3000] [--alvos alvos_xg.csv]

Como funciona: para cada dia dos alvos faz 1 chamada de "matches-by-date" (415 jogos numa unica
resposta), casa os times por nome (exato -> semelhanca 0.60/0.80, o mesmo criterio do relatorio das
06:00) e depois 1 chamada de "all-stats" por jogo. Grava xg_ft_backfill.csv e marca o jogo em
xg_ft_seen.txt, entao pode ser interrompido e retomado a vontade.

CUIDADO COM A COTA: o logger xg-ht ao vivo usa a MESMA chave. O script le o cabecalho
x-ratelimit-requests-remaining a cada chamada e PARA sozinho quando sobra menos que --reserva,
para nunca deixar o servico ao vivo sem cota.
"""
import os, sys, csv, json, time, difflib, argparse, urllib.request, urllib.error, unicodedata, re
from datetime import datetime, timedelta

BASE = os.path.dirname(os.path.abspath(__file__))
HOST = "free-api-live-football-data.p.rapidapi.com"
ALVOS = os.path.join(BASE, "alvos_xg.csv")
SAIDA = os.path.join(BASE, "xg_ft_backfill.csv")
SEEN = os.path.join(BASE, "xg_ft_seen.txt")
LIGAS = os.path.join(BASE, "xg_ft_ligas.csv")   # liga -> tentativas/sucessos (aprende a pular)
DIAS = os.path.join(BASE, "fotmob_dias")          # cache das listas por dia (1 chamada por dia, reusada)

CAMPOS = [("expected_goals", "xg"), ("expected_goals_on_target", "xgot"),
          ("expected_goals_open_play", "xg_open"), ("expected_goals_set_play", "xg_set"),
          ("BallPossesion", "poss"), ("total_shots", "shots"), ("ShotsOnTarget", "sot"),
          ("ShotsOffTarget", "soff"), ("blocked_shots", "sblock"), ("shots_inside_box", "sbox"),
          ("big_chance", "bigch"), ("big_chance_missed_title", "bigch_perd"),
          ("touches_opp_box", "touch_box"), ("corners", "corners"), ("keeper_saves", "saves"),
          ("yellow_cards", "yellow"), ("red_cards", "red"), ("fouls", "fouls")]
COLS = ["Data", "Home", "Away", "League", "casou", "eventid", "liga_api", "gh", "ga"] + \
       [n + lado for _, n in CAMPOS for lado in ("_h", "_a")] + ["coletado_em"]


def cn(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)


def parecido(a, b):
    sh = difflib.SequenceMatcher(None, a[0], b[0]).ratio()
    if sh < 0.60: return 0.0
    sa = difflib.SequenceMatcher(None, a[1], b[1]).ratio()
    if sa < 0.60: return 0.0
    return (sh + sa) / 2


class Cota(object):
    """acompanha o que a API diz que sobrou; e quem manda no 'parar'."""
    def __init__(self, reserva): self.resta = None; self.limite = None; self.reserva = reserva; self.usadas = 0

    def ler(self, headers):
        h = {k.lower(): v for k, v in headers.items()}
        for k, v in h.items():
            if "ratelimit-requests-remaining" in k:
                try: self.resta = int(v)
                except Exception: pass
            elif "ratelimit-requests-limit" in k:
                try: self.limite = int(v)
                except Exception: pass

    def acabou(self):
        return self.resta is not None and self.resta <= self.reserva


def get(path, key, cota, tentativas=3):
    req = urllib.request.Request("https://%s/%s" % (HOST, path),
                                 headers={"x-rapidapi-host": HOST, "x-rapidapi-key": key})
    for i in range(tentativas):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                cota.ler(r.headers); cota.usadas += 1
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 404: return None
            if e.code in (429, 503) and i < tentativas - 1: time.sleep(5 * (i + 1)); continue
            print("   [http %s] %s" % (e.code, path[:60])); return None
        except Exception as e:
            if i < tentativas - 1: time.sleep(3); continue
            print("   [erro] %s: %s" % (path[:50], str(e)[:70])); return None
    return None


def jogos_do_dia(dia, key, cota):
    """lista de jogos do dia, com cache em disco: 1 chamada por dia, nunca repetida."""
    if not os.path.isdir(DIAS):
        try: os.makedirs(DIAS)
        except Exception: pass
    p = os.path.join(DIAS, "%s.json" % dia)
    if os.path.exists(p):
        try:
            with open(p, encoding="utf-8") as f: return json.load(f)
        except Exception: pass
    d = get("football-get-matches-by-date?date=%s" % dia.replace("-", ""), key, cota)
    ms = ((d or {}).get("response", {}) or {}).get("matches", []) or []
    saida = [{"id": m.get("id"), "h": (m.get("home") or {}).get("name", ""), "a": (m.get("away") or {}).get("name", ""),
              "gh": (m.get("home") or {}).get("score"), "ga": (m.get("away") or {}).get("score"),
              "lid": m.get("leagueId")} for m in ms if m.get("id")]
    try:
        with open(p, "w", encoding="utf-8") as f: json.dump(saida, f)
    except Exception: pass
    return saida


def achar_evento(jogos, home, away, gh=None, ga=None):
    """exato -> semelhanca 0.80 -> semelhanca 0.70 SE o placar bater (o placar vem da base e confirma)."""
    alvo = (cn(home), cn(away))
    for j in jogos:
        if (cn(j["h"]), cn(j["a"])) == alvo: return j, "exato"
    melhor, nota = None, 0.0
    melhor_p, nota_p = None, 0.0
    for j in jogos:
        s = parecido(alvo, (cn(j["h"]), cn(j["a"])))
        if s > nota: nota, melhor = s, j
        if gh not in (None, "") and str(j.get("gh")) == str(gh) and str(j.get("ga")) == str(ga) and s > nota_p:
            nota_p, melhor_p = s, j
    if melhor and nota >= 0.80: return melhor, "fuzzy%.2f" % nota
    if melhor_p and nota_p >= 0.70: return melhor_p, "fuzzy%.2f+placar" % nota_p
    return None, None


def stats(eventid, key, cota):
    d = get("football-get-match-all-stats?eventid=%s" % eventid, key, cota)
    if not isinstance(d, dict) or d.get("status") != "success": return None
    achado = {}
    def anda(o):
        if isinstance(o, dict):
            k, v = o.get("key"), o.get("stats")
            if k and isinstance(v, list) and len(v) == 2 and v[0] is not None and k not in achado:
                achado[k] = v
            for x in o.values(): anda(x)
        elif isinstance(o, list):
            for x in o: anda(x)
    anda((d.get("response", {}) or {}).get("stats", []))
    if not achado: return None
    out = {}
    for chave, nome in CAMPOS:
        v = achado.get(chave)
        for i, lado in enumerate(("_h", "_a")):
            x = v[i] if v else ""
            if isinstance(x, str) and "(" in x: x = x.split("(")[0].strip()   # "380 (84%)" -> 380
            out[nome + lado] = x if x is not None else ""
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max", type=int, default=300, help="maximo de JOGOS a preencher nesta rodada")
    ap.add_argument("--reserva", type=int, default=3000, help="chamadas que ficam guardadas para o xg-ht ao vivo")
    ap.add_argument("--alvos", default=ALVOS)
    ap.add_argument("--pausa", type=float, default=0.4, help="segundos entre chamadas")
    a = ap.parse_args()
    key = os.environ.get("RAPIDAPI_KEY", "")
    if not key:
        print("RAPIDAPI_KEY nao esta no ambiente. Rode:  set -a; . ./alerta.env; set +a"); sys.exit(1)
    if not os.path.exists(a.alvos):
        print("nao achei %s (gere no PC com xg_backfill_alvos.py e copie para ca)" % a.alvos); sys.exit(1)

    feitos = set()
    if os.path.exists(SEEN):
        with open(SEEN, encoding="utf-8") as f:
            feitos = set(l.strip() for l in f if l.strip())
    alvos = []
    with open(a.alvos, encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            ch = "%s|%s|%s" % (r["Data"], cn(r["Home"]), cn(r["Away"]))
            if ch not in feitos: alvos.append((r, ch))
    alvos.sort(key=lambda x: x[0]["Data"])
    print("alvos pendentes: %d (ja feitos: %d) | limite desta rodada: %d | reserva de cota: %d"
          % (len(alvos), len(feitos), a.max, a.reserva))

    novo = not os.path.exists(SAIDA)
    fh = open(SAIDA, "a", newline="", encoding="utf-8")
    w = csv.DictWriter(fh, fieldnames=COLS)
    if novo: w.writeheader()
    fseen = open(SEEN, "a", encoding="utf-8")
    cota = Cota(a.reserva)
    # LIGA SEM ESTATISTICA: dois casos, ambos inuteis e caros. (a) o jogo nao existe no matches-by-date
    # (FA Cup de fase preliminar, Scotland 3, times nao-liga); (b) o jogo existe mas o all-stats vem
    # vazio — medido em JAPAN 2, PORTUGAL 2, ENGLAND 5 e ARGENTINA 2: a API tem a partida e o placar,
    # mas nao tem xG nem chutes dessas divisoes. Cada tentativa gasta uma chamada e nunca devolve nada. Aqui a liga e aprendida: apos MIN_TENT tentativas sem NENHUM
    # sucesso, ela passa a ser pulada sem gastar chamada. Mesma ideia do fotmob_ligas.csv do xg-ht.
    MIN_TENT = 8
    ligas = {}
    if os.path.exists(LIGAS):
        with open(LIGAS, encoding="utf-8-sig") as f:
            for r in csv.DictReader(f):
                ligas[r["League"]] = [int(r["tentativas"]), int(r["sucessos"])]

    def pular(lg):
        t, s = ligas.get(lg, [0, 0])
        return t >= MIN_TENT and s == 0

    def grava_ligas():
        with open(LIGAS, "w", newline="", encoding="utf-8") as f:
            w2 = csv.writer(f); w2.writerow(["League", "tentativas", "sucessos"])
            for k, (t, s) in sorted(ligas.items()): w2.writerow([k, t, s])

    ok = semdado = nao_casou = puladas = 0
    dia_atual, jogos = None, []
    for r, ch in alvos:
        if ok + semdado >= a.max:
            print("  limite da rodada atingido."); break
        if cota.acabou():
            print("  COTA baixa (restam %s, reserva %d) — parando para nao afetar o xg-ht." % (cota.resta, a.reserva)); break
        _lg = r.get("League", "")
        if pular(_lg):
            puladas += 1; fseen.write(ch + "\n"); continue
        if r["Data"] != dia_atual:
            dia_atual = r["Data"]; jogos = jogos_do_dia(dia_atual, key, cota)
            print("  %s: %d jogos na API" % (dia_atual, len(jogos)))
            time.sleep(a.pausa)
        _gh, _ga = r.get("gh"), r.get("ga")
        j, tipo = achar_evento(jogos, r["Home"], r["Away"], _gh, _ga)
        if not j:
            # o dia da API e UTC: jogo da noite pode cair no dia seguinte (ou no anterior)
            for desloc in (1, -1):
                outro = (datetime.strptime(r["Data"], "%Y-%m-%d") + timedelta(days=desloc)).strftime("%Y-%m-%d")
                j, tipo = achar_evento(jogos_do_dia(outro, key, cota), r["Home"], r["Away"], _gh, _ga)
                if j: break
        ligas.setdefault(_lg, [0, 0])[0] += 1          # tentativa nesta liga
        if not j:
            nao_casou += 1; fseen.write(ch + "\n"); continue
        st = stats(j["id"], key, cota); time.sleep(a.pausa)
        if not st:
            semdado += 1; fseen.write(ch + "\n"); continue
        ligas[_lg][1] += 1                             # sucesso: a liga existe na API
        linha = dict(Data=r["Data"], Home=r["Home"], Away=r["Away"], League=r.get("League", ""), casou=tipo,
                     eventid=j["id"], liga_api=j.get("lid", ""), gh=j.get("gh", ""), ga=j.get("ga", ""),
                     coletado_em=datetime.now().strftime("%Y-%m-%d %H:%M"))
        linha.update(st)
        w.writerow(linha); fh.flush(); fseen.write(ch + "\n"); fseen.flush(); ok += 1
        if ok % 25 == 0:
            print("   %d preenchidos | cota restante: %s" % (ok, cota.resta))
    fh.close(); fseen.close(); grava_ligas()
    print("\npreenchidos: %d | sem estatistica na API: %d | nome nao casou: %d | pulados (liga sem estatistica): %d"
          % (ok, semdado, nao_casou, puladas))
    ruins = [k for k, (t, s2) in sorted(ligas.items()) if t >= MIN_TENT and s2 == 0]
    if ruins: print("ligas SEM ESTATISTICA na API (o jogo existe, mas nao ha xG/chutes; nao serao mais tentadas): %s" % ", ".join(ruins[:14]))
    print("chamadas usadas nesta rodada: %d | cota restante: %s de %s" % (cota.usadas, cota.resta, cota.limite))
    print("-> %s" % os.path.basename(SAIDA))


if __name__ == "__main__":
    main()
