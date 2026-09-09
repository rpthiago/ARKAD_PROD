#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
hist_xg_ht.py — coletor HISTORICO de estatisticas LIMPAS do 1o tempo (FotMob via RapidAPI).

POR QUE EXISTE: a base Bet365 tem 68% dos jogos com as stats de HT gravadas como 0 (nao vazio),
o que transformou o "filtro de pressao" do Radar HT num filtro de COBERTURA DE DADOS — o gap
caiu de +12,24pp para +3,27pp quando comparado so entre jogos que tinham estatistica.
Este script substitui essa coluna suja por dado limpo do FotMob, para dar um veredito honesto.

ARQUITETURA (cada fonte no que ela e boa):
  base Bet365 -> placar do HT, placar FT, odds pre-jogo   (confiaveis)
  FotMob      -> estatisticas do 1o tempo                  (limpas, sem zero-fill)

CUSTO: 1 requisicao por DATA (agenda do dia inteiro, ~167 partidas) + 1 por JOGO (stats).
180 dias com fav<=1.60 = ~150 + ~1.207 = ~1.360 requisicoes de 20.000/mes.

RETOMAVEL: tudo em cache no disco. Rodar de novo so busca o que falta.

  python hist_xg_ht.py --dias 180 --teto-odd 1.60 --max-req 2000
  python hist_xg_ht.py --analisar            # so refaz a analise do que ja esta em cache
"""
import os, re, csv, sys, json, time, argparse, unicodedata, difflib
import urllib.request, urllib.parse
from datetime import datetime, timedelta

AQUI = os.path.dirname(os.path.abspath(__file__))
BASE_CSV = os.path.join(AQUI, "Bases_de_Dados_API_FutPythonTrader_Bet365.csv")
DIR_AGENDA = os.path.join(AQUI, "hist_agenda")
DIR_STATS = os.path.join(AQUI, "hist_stats")
SAIDA = os.path.join(AQUI, "hist_ht_stats.csv")
HOST = "free-api-live-football-data.p.rapidapi.com"
KO_TOL_MIN = 240          # a base tem so a DATA; a agenda tem KO em UTC -> tolerancia larga
SIM_MIN = 0.82

req = [0]


def cn(s):
    s = re.sub(r"\([a-zA-Z]{2,3}\)", " ", str(s))       # 'PAOK (GRE)' -> 'PAOK'
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)


RUIDO = {"fc", "cf", "sc", "ac", "afc", "cd", "ad", "club", "clube", "the", "de", "do",
         "da", "el", "la", "ii", "b", "united", "utd", "city", "town", "cfc", "ec", "sk",
         "fk", "nk", "hk", "if", "ik", "bk", "ca", "cs", "as", "us", "sv", "tsv", "vfb"}


def tokens(s):
    """Tokens uteis do nome, ja sem codigo de pais entre parenteses e sem ruido de sigla."""
    s = re.sub(r"\([a-zA-Z]{2,3}\)", " ", str(s))          # 'Twente (NED)' -> 'Twente'
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
    ts = [t for t in re.split(r"[^a-z0-9]+", s) if t]
    uteis = [t for t in ts if len(t) >= 4 and t not in RUIDO]
    return uteis or [t for t in ts if t]      # se sobrou nada, usa tudo


def nomes_batem(a, b):
    """Betfair/Bet365 abreviam ('Atl. Madrid', 'O. Ljubljana', 'Twente (NED)'). Casa por
    TOKEN significativo alem de igualdade/contencao/similaridade."""
    x, y = cn(a), cn(b)
    if not x or not y:
        return False
    if x == y:
        return True
    if len(x) >= 5 and len(y) >= 5 and (x in y or y in x):
        return True
    if difflib.SequenceMatcher(None, x, y).ratio() >= SIM_MIN:
        return True
    ta, tb = tokens(a), tokens(b)
    if not ta or not tb:
        return False
    # o token mais longo de um dos lados aparece inteiro no outro nome normalizado
    for t in (max(ta, key=len), max(tb, key=len)):
        if len(t) >= 5 and (t in x and t in y):
            return True
    return False


def load_key():
    k = os.environ.get("RAPIDAPI_KEY")
    if k:
        return k
    for p in (os.path.join(AQUI, ".rapidapi_key"), os.path.expanduser("~/.rapidapi_key")):
        if os.path.exists(p):
            return open(p).read().strip()
    return None


def get(path, key, teto):
    if req[0] >= teto:
        raise RuntimeError("teto de requisicoes (%d) atingido" % teto)
    r = urllib.request.Request("https://%s/%s" % (HOST, path),
                               headers={"x-rapidapi-host": HOST, "x-rapidapi-key": key})
    with urllib.request.urlopen(r, timeout=40) as resp:
        req[0] += 1
        rest = resp.headers.get("X-RateLimit-Requests-Remaining")
        if rest is not None and int(rest) < 300:
            print("  [AVISO] cota mensal baixa: %s" % rest, flush=True)
        return json.loads(resp.read().decode())


def agenda(dia, key, teto):
    """Todas as partidas de uma data. 1 requisicao, cacheada em disco para sempre."""
    os.makedirs(DIR_AGENDA, exist_ok=True)
    p = os.path.join(DIR_AGENDA, "%s.json" % dia)
    if os.path.exists(p):
        return json.load(open(p, encoding="utf-8"))
    try:
        d = get("football-get-matches-by-date?date=%s" % dia, key, teto)
    except RuntimeError:
        raise
    except Exception as e:
        print("  [agenda %s] %s" % (dia, str(e)[:50]), flush=True)
        return []
    out = []
    for m in (d.get("response", {}) or {}).get("matches", []) or []:
        st = m.get("status", {}) or {}
        out.append(dict(id=str(m.get("id")), league_id=str(m.get("leagueId", "")),
                        home=(m.get("home") or {}).get("name", ""),
                        away=(m.get("away") or {}).get("name", ""),
                        ko=(st.get("utcTime") or "")[:16]))
    json.dump(out, open(p, "w", encoding="utf-8"))
    return out


def stats(eventid, key, teto):
    """Stats do 1o tempo de um jogo. 1 requisicao, cacheada. {} = liga sem cobertura."""
    os.makedirs(DIR_STATS, exist_ok=True)
    p = os.path.join(DIR_STATS, "%s.json" % eventid)
    if os.path.exists(p):
        return json.load(open(p, encoding="utf-8"))
    try:
        d = get("football-get-match-firstHalf-stats?eventid=%s" % eventid, key, teto)
    except RuntimeError:
        raise
    except Exception as e:
        print("  [stats %s] %s" % (eventid, str(e)[:50]), flush=True)
        return None
    out = {}
    if d.get("status") == "success":
        for grupo in (d.get("response", {}) or {}).get("stats", []) or []:
            for st in grupo.get("stats", []) or []:
                k, v = st.get("key"), st.get("stats")
                if k and v and v[0] is not None and k not in out:
                    out[k] = v
    json.dump(out, open(p, "w", encoding="utf-8"))
    return out


def alvos(dias, teto_odd):
    """Da base Bet365: jogos 0-0 no intervalo com mandante favorito. Placar e odds sao confiaveis."""
    import pandas as pd
    cols = ["Date", "League", "Home", "Away", "Goals_H_HT", "Goals_A_HT", "Goals_H_FT", "Goals_A_FT",
            "Odd_H_FT", "Odd_D_FT", "Odd_A_FT", "Odd_Over25_FT",
            "Shots_On_Target_H_HT", "Corners_H_HT", "Total_Shots_H_HT", "Possession_H_HT"]
    df = pd.read_csv(BASE_CSV, usecols=cols, low_memory=False)
    df["Date"] = pd.to_datetime(df.Date, errors="coerce")
    for c in cols[4:]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    lim = df.Date.max() - pd.Timedelta(days=dias)
    r = df[(df.Date >= lim) & (df.Goals_H_HT == 0) & (df.Goals_A_HT == 0) & (df.Odd_H_FT <= teto_odd)]
    return r.sort_values("Date").reset_index(drop=True)


def par(d, k, i):
    v = (d or {}).get(k)
    if not v or len(v) <= i or v[i] is None:
        return ""
    return v[i]


COLS = ["data", "liga_bf", "home", "away", "odd_h", "odd_d", "odd_a", "odd_over25",
        "gols_h_ft", "gols_a_ft", "venceu_mandante",
        "base_sot_h", "base_corners_h", "base_shots_h", "base_tem_stats",
        "eventid", "league_id", "fm_tem_stats", "fm_tem_xg",
        "xg_h", "xg_a", "sot_h", "sot_a", "shots_h", "shots_a", "corners_h", "corners_a",
        "bigch_h", "bigch_a", "touches_box_h", "touches_box_a", "poss_h"]


def coletar(args, key):
    alvo = alvos(args.dias, args.teto_odd)
    print("alvos da base: %d jogos (0-0 no HT, fav<=%.2f, ultimos %d dias)"
          % (len(alvo), args.teto_odd, args.dias))
    datas = sorted(set(alvo.Date.dt.strftime("%Y%m%d")))
    print("datas distintas: %d\n" % len(datas))

    linhas = []
    casados = naocasou = semstat = 0
    for i, row in alvo.iterrows():
        dia = row.Date.strftime("%Y%m%d")
        try:
            cands = agenda(dia, key, args.max_req)
            for d in (1, -1):     # KO perto da virada do dia UTC
                cands = cands + agenda((row.Date + timedelta(days=d)).strftime("%Y%m%d"), key, args.max_req)
        except RuntimeError as e:
            print("\n[parou] %s" % e); break
        eid = lid = ""
        for m in cands:
            if nomes_batem(row.Home, m["home"]) and nomes_batem(row.Away, m["away"]):
                try:
                    fko = datetime.strptime(m["ko"], "%Y-%m-%dT%H:%M")
                except Exception:
                    continue
                if abs((fko - row.Date.to_pydatetime()).total_seconds()) <= KO_TOL_MIN * 60 + 86400:
                    eid, lid = m["id"], m["league_id"]; break
        st = None
        if eid:
            casados += 1
            try:
                st = stats(eid, key, args.max_req)
            except RuntimeError as e:
                print("\n[parou] %s" % e); break
        else:
            naocasou += 1
        if eid and not st:
            semstat += 1
        base_tem = not (row.Total_Shots_H_HT == 0 and row.Corners_H_HT == 0 and row.Possession_H_HT == 0)
        linhas.append([row.Date.strftime("%Y-%m-%d"), row.League, row.Home, row.Away,
                       row.Odd_H_FT, row.Odd_D_FT, row.Odd_A_FT, row.Odd_Over25_FT,
                       row.Goals_H_FT, row.Goals_A_FT, int(row.Goals_H_FT > row.Goals_A_FT),
                       row.Shots_On_Target_H_HT, row.Corners_H_HT, row.Total_Shots_H_HT, int(base_tem),
                       eid, lid, int(bool(st)), int("expected_goals" in (st or {})),
                       par(st, "expected_goals", 0), par(st, "expected_goals", 1),
                       par(st, "ShotsOnTarget", 0), par(st, "ShotsOnTarget", 1),
                       par(st, "total_shots", 0), par(st, "total_shots", 1),
                       par(st, "corners", 0), par(st, "corners", 1),
                       par(st, "big_chance", 0), par(st, "big_chance", 1),
                       par(st, "touches_opp_box", 0), par(st, "touches_opp_box", 1),
                       par(st, "BallPossesion", 0)])
        if (i + 1) % 50 == 0:
            print("  %4d/%d | casados %d | sem stats %d | nao casou %d | req %d"
                  % (i + 1, len(alvo), casados, semstat, naocasou, req[0]), flush=True)
    with open(SAIDA, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f); w.writerow(COLS); w.writerows(linhas)
    print("\ngravado %s (%d linhas) | requisicoes gastas: %d" % (SAIDA, len(linhas), req[0]))
    print("casados %d | com stats FotMob %d | nao casou %d"
          % (casados, casados - semstat, naocasou))


def analisar():
    import pandas as pd
    d = pd.read_csv(SAIDA)
    print("=== ANALISE: filtro de pressao com DADO LIMPO (FotMob) ===\n")
    print("linhas: %d | casadas no FotMob: %d | com stats: %d | com xG: %d"
          % (len(d), (d.eventid.notna()).sum(), d.fm_tem_stats.sum(), d.fm_tem_xg.sum()))
    lim = d[d.fm_tem_stats == 1].copy()
    if len(lim) < 30:
        print("\namostra insuficiente ainda."); return
    for c in ("sot_h", "corners_h", "xg_h"):
        lim[c] = pd.to_numeric(lim[c], errors="coerce")
    lim["press"] = (lim.sot_h >= 3) | (lim.corners_h >= 4)
    print("\n--- COM dado limpo (so jogos que o FotMob cobre) ---")
    print("  cego        N=%4d  WR=%.2f%%" % (len(lim), 100 * lim.venceu_mandante.mean()))
    print("  com pressao N=%4d  WR=%.2f%%" % (lim.press.sum(), 100 * lim[lim.press].venceu_mandante.mean()))
    print("  sem pressao N=%4d  WR=%.2f%%" % ((~lim.press).sum(), 100 * lim[~lim.press].venceu_mandante.mean()))
    gap = 100 * (lim[lim.press].venceu_mandante.mean() - lim.venceu_mandante.mean())
    print("  >>> GAP do filtro: %+.2f pp" % gap)
    b = d[d.base_tem_stats == 1].copy()
    if len(b) > 30:
        b["pressb"] = (pd.to_numeric(b.base_sot_h, errors="coerce") >= 3) | \
                      (pd.to_numeric(b.base_corners_h, errors="coerce") >= 4)
        print("\n--- comparacao: mesma conta usando a coluna SUJA da base ---")
        print("  cego(base c/stats) N=%4d WR=%.2f%% | com pressao N=%4d WR=%.2f%%"
              % (len(b), 100 * b.venceu_mandante.mean(), b.pressb.sum(),
                 100 * b[b.pressb].venceu_mandante.mean()))
    xg = lim[pd.to_numeric(lim.xg_h, errors="coerce").notna()].copy()
    if len(xg) > 50:
        xg["xg_h"] = pd.to_numeric(xg.xg_h, errors="coerce")
        print("\n--- WR por xG do mandante no 1o tempo (dado que so o FotMob tem) ---")
        for lo, hi in [(0, .3), (.3, .6), (.6, 1.0), (1.0, 9)]:
            s = xg[(xg.xg_h >= lo) & (xg.xg_h < hi)]
            if len(s) >= 15:
                print("    xG %.1f-%.1f: N=%3d WR=%.1f%%" % (lo, hi, len(s), 100 * s.venceu_mandante.mean()))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dias", type=int, default=180)
    ap.add_argument("--teto-odd", type=float, default=1.60)
    ap.add_argument("--max-req", type=int, default=2000)
    ap.add_argument("--analisar", action="store_true")
    args = ap.parse_args()
    if args.analisar:
        analisar(); return
    key = load_key()
    if not key:
        print("[erro] sem RAPIDAPI_KEY (env ou arquivo .rapidapi_key ao lado do script)"); sys.exit(1)
    coletar(args, key)
    print()
    analisar()


if __name__ == "__main__":
    main()
