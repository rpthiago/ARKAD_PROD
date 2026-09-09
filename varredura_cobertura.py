# -*- coding: utf-8 -*-
"""Varredura de cobertura: para cada liga do ARKAD, testa se o FotMob/RapidAPI tem
stats do 1o tempo (e xG). 2 requisicoes por liga. Para sozinho se a cota acabar."""
import json, sys, time, urllib.request

KEY = "25f457d6d6msh7957c28b6440142p1c07fdjsn1b00fe338d7b"
HOST = "free-api-live-football-data.p.rapidapi.com"
SAIDA = "cobertura_ligas.csv"

# (sinais, %, liga Betfair, time representativo que jogou recentemente)
LIGAS = [
    (9, "Venezuelan Primera Division", "Deportivo La Guaira"),
    (9, "Colombian Primera A", "Llaneros FC"),
    (9, "English Sky Bet League 1", "Bromley"),
    (7, "Swiss Super League", "Servette"),
    (7, "French Ligue 2", "St Etienne"),
    (7, "Argentinian Primera Nacional", "San Martin De San Juan"),
    (7, "Czech 1 Liga", "Zlin"),
    (6, "Ecuadorian Serie A", "Mushuc Runa"),
    (6, "Chilean Primera Division", "Palestino"),
    (6, "Turkish 1 Lig", "Manisa FK"),
    (6, "Danish Superliga", "Midtjylland"),
    (6, "Czech 2 Liga", "Zizkov"),
    (6, "Portuguese Primeira Liga", "Santa Clara"),
    (5, "Egyptian 2nd Division", "Proxy Work Club"),
    (5, "Polish Ekstraklasa", "Pogon Szczecin"),
    (5, "German 3 Liga", "Wurzburger Kickers"),
    (5, "English National League", "Southend"),
    (5, "Argentinian Primera Division", "Union Santa Fe"),
    (5, "Romanian Liga II", "CSM Satu Mare"),
    (5, "Greek Super League", "Asteras Tripolis"),
    (5, "Japanese J League", "Nagoya"),
    (4, "Brazilian Serie A", "Corinthians"),
    (4, "Belgian Pro League", "Cercle Brugge"),
    (4, "Ecuadorian Serie B", "El Nacional"),
    (4, "Colombian Primera B", "Union Magdalena"),
    (4, "Indonesian Super League", "Persib Bandung"),
    (4, "Danish 1st Division", "Aarhus Fremad"),
    (4, "German Bundesliga 2", "Hertha Berlin"),
    (4, "Swedish Superettan", "Oddevold"),
    (4, "Brazilian Serie B", "Londrina"),
    (4, "Chinese Super League", "Tianjin Jinmen Tiger FC"),
    (4, "Slovakian Super League", "Ruzomberok"),
]

restante = [None]


def get(path):
    req = urllib.request.Request("https://%s/%s" % (HOST, path),
                                 headers={"x-rapidapi-host": HOST, "x-rapidapi-key": KEY})
    with urllib.request.urlopen(req, timeout=30) as r:
        rem = r.headers.get("X-RateLimit-Requests-Remaining")
        if rem is not None:
            restante[0] = int(rem)
        return json.loads(r.read().decode())


def achar_jogo(time_nome):
    d = get("football-matches-search?search=%s" % urllib.parse.quote(time_nome))
    for s in d.get("response", {}).get("suggestions", []):
        if s.get("type") == "match" and s.get("status", {}).get("finished"):
            return s["id"], s.get("leagueName", "?"), s.get("matchDate", "")[:10]
    return None, None, None


def classificar(eventid):
    try:
        d = get("football-get-match-firstHalf-stats?eventid=%s" % eventid)
    except Exception as e:
        return "ERRO", str(e)[:40], {}
    if d.get("status") != "success":
        return "DEGRAU_3_sem_dado", d.get("message", "")[:40], {}
    achado = {}
    for grupo in d.get("response", {}).get("stats", []):
        for st in grupo.get("stats", []):
            k = st.get("key"); v = st.get("stats")
            if k and v and v[0] is not None and k not in achado:
                achado[k] = v
    tem_xg = "expected_goals" in achado
    tem_chute = "ShotsOnTarget" in achado
    tem_esc = "corners" in achado
    if tem_xg and tem_chute:
        return "DEGRAU_1_xG", "", achado
    if tem_chute or tem_esc:
        return "DEGRAU_2_chutes", "", achado
    return "DEGRAU_3_sem_dado", "resposta vazia", achado


def main():
    import urllib.parse  # noqa
    linhas = []
    print("%-30s %-6s %-20s %s" % ("LIGA (Betfair)", "SINAIS", "DEGRAU", "detalhe"))
    print("-" * 96)
    for n, liga, time_nome in LIGAS:
        if restante[0] is not None and restante[0] < 3:
            print("\n[parou: cota esgotada, restam %s]" % restante[0]); break
        try:
            eid, lg_fm, data = achar_jogo(time_nome)
        except Exception as e:
            print("%-30s %-6d %-20s %s" % (liga[:30], n, "ERRO_BUSCA", str(e)[:30])); continue
        if not eid:
            linhas.append((n, liga, "SEM_JOGO", "", ""));
            print("%-30s %-6d %-20s %s" % (liga[:30], n, "SEM_JOGO_ENCERRADO", time_nome)); continue
        deg, msg, ach = classificar(eid)
        det = []
        for k, lbl in (("ShotsOnTarget", "SoT"), ("corners", "esc"), ("total_shots", "chutes"),
                       ("expected_goals", "xG"), ("big_chance", "bigch")):
            if k in ach:
                det.append("%s=%s" % (lbl, "/".join(str(x) for x in ach[k])))
        linhas.append((n, liga, deg, lg_fm or "", "; ".join(det) or msg))
        print("%-30s %-6d %-20s %s" % (liga[:30], n, deg, ("; ".join(det) or msg)[:44]))
        time.sleep(0.3)
    with open(SAIDA, "w", encoding="utf-8") as f:
        f.write("sinais,liga_betfair,degrau,liga_fotmob,detalhe\n")
        for r in linhas:
            f.write(",".join('"%s"' % str(x).replace('"', "'") for x in r) + "\n")
    print("\ncota restante: %s | salvo em %s" % (restante[0], SAIDA))
    # resumo por degrau, ponderado por sinais
    tot = sum(n for n, *_ in LIGAS)
    agg = {}
    for n, liga, deg, *_ in linhas:
        agg[deg] = agg.get(deg, 0) + n
    print("\n=== RESUMO (peso = nº de sinais do under-limite) ===")
    for k in sorted(agg, key=lambda x: -agg[x]):
        print("  %-22s %3d sinais (%.0f%% do testado)" % (k, agg[k], 100 * agg[k] / max(1, sum(agg.values()))))


if __name__ == "__main__":
    import urllib.parse
    main()
