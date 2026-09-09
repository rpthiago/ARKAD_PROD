#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
baixar_expansao_ligas_xg.py — Expansão de Data Harvesting de xG Limpo (FotMob/Opta).

Aproveita a assinatura paga de 20.000 requisições (com ~12.000 restantes) para baixar
e congelar no disco um banco de dados proprietário permanente de xG e estatísticas profundas.

- Alvo: Próximas 25 ligas profissionais (Europa, Américas, Ásia).
- Retomável: Se interrompido, continua de onde parou sem gastar requisições repetidas.
- Seguro: Monitora o header de cota e para automaticamente se restar < 500 reqs.
- Saída: hist_time_stats_expandido.csv + cache permanente em hist_stats_ft/{id}.json.

Uso:
  python baixar_expansao_ligas_xg.py --max-req 7500
"""

import os, csv, sys, json, argparse, time
import urllib.request
from datetime import timedelta
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import hist_xg_ht as H

AQUI = os.path.dirname(os.path.abspath(__file__))
DIR_FT = os.path.join(AQUI, "hist_stats_ft")
SAIDA = os.path.join(AQUI, "hist_time_stats_expandido.csv")
ORIGINAL = os.path.join(AQUI, "hist_time_stats.csv")

CAMPOS = [("expected_goals", "xg"), ("expected_goals_open_play", "xg_open"),
          ("expected_goals_set_play", "xg_set"), ("expected_goals_non_penalty", "xg_np"),
          ("expected_goals_on_target", "xgot"), ("total_shots", "shots"),
          ("ShotsOnTarget", "sot"), ("big_chance", "bigch"),
          ("touches_opp_box", "tbox"), ("keeper_saves", "saves"),
          ("corners", "corners"), ("BallPossesion", "poss")]

COLS = (["data", "liga", "home", "away", "gols_h", "gols_a", "odd_h", "odd_d", "odd_a",
         "odd_over25", "odd_cs_0x0", "eventid", "league_id", "tem_stats", "tem_xg"]
        + ["%s_h" % s for _, s in CAMPOS] + ["%s_a" % s for _, s in CAMPOS])

# Lista selecionada das 20 melhores ligas profissionais ainda não baixadas (todas com cobertura Opta/FotMob)
LIGAS_ALVO = [
    'BELGIUM 1', 'GERMANY 2', 'TURKEY 1', 'TURKEY 2', 'POLAND 1', 'JAPAN 1',
    'NETHERLANDS 2', 'EUROPA CONFERENCE LEAGUE', 'EUROPA CHAMPIONS LEAGUE',
    'SWITZERLAND 1', 'GREECE 1', 'DENMARK 1', 'CHILE 1', 'PORTUGAL 2',
    'SERBIA 1', 'BULGARIA 1', 'PERU 1', 'SAUDI ARABIA 1', 'USA 2', 'EGYPT 1'
]

rest_quota = [None]


def api_get(path, key, teto, retries=3):
    if H.req[0] >= teto:
        raise RuntimeError(f"Teto de requisições configurado ({teto}) atingido")
    r = urllib.request.Request("https://%s/%s" % (H.HOST, path),
                               headers={"x-rapidapi-host": H.HOST, "x-rapidapi-key": key})
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(r, timeout=30) as resp:
                H.req[0] += 1
                rest = resp.headers.get("X-RateLimit-Requests-Remaining")
                if rest is not None:
                    rest_quota[0] = int(rest)
                    if rest_quota[0] < 500:
                        raise RuntimeError(f"PARADA DE SEGURANÇA: Restam apenas {rest_quota[0]} requisições na sua assinatura RapidAPI! Parando para evitar qualquer custo extra.")
                return json.loads(resp.read().decode())
        except RuntimeError:
            raise
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2 * (attempt + 1))
                continue
            raise RuntimeError(f"Falha de conexão com a API após {retries} tentativas: {e}")


def stats_ft(eventid, key, teto):
    """Obtém stats de jogo completo. Se em cache, 0 requisições."""
    os.makedirs(DIR_FT, exist_ok=True)
    p = os.path.join(DIR_FT, "%s.json" % eventid)
    if os.path.exists(p):
        try:
            return json.load(open(p, encoding="utf-8"))
        except Exception:
            pass
    # Se api_get falhar por rede, levanta RuntimeError e main() faz parada segura sem corromper
    d = api_get("football-get-match-all-stats?eventid=%s" % eventid, key, teto)
    out = {}
    if d and d.get("status") == "success":
        for grupo in (d.get("response", {}) or {}).get("stats", []) or []:
            for st in grupo.get("stats", []) or []:
                k, v = st.get("key"), st.get("stats")
                if k and v and v[0] is not None and k not in out:
                    out[k] = v
    # Salva cache (dado real ou {} se a API legitimamente não tiver stats para esse jogo)
    json.dump(out, open(p, "w", encoding="utf-8"))
    return out


def carregar_alvos(dias=300):
    cols = ["Date", "League", "Home", "Away", "Goals_H_FT", "Goals_A_FT",
            "Odd_H_FT", "Odd_D_FT", "Odd_A_FT", "Odd_Over25_FT", "Odd_CS_0x0"]
    df = pd.read_csv(H.BASE_CSV, usecols=cols, low_memory=False)
    df["Date"] = pd.to_datetime(df.Date, errors="coerce")
    for c in cols[4:]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    lim = df.Date.max() - pd.Timedelta(days=dias)
    r = df[(df.Date >= lim) & (df.League.isin(LIGAS_ALVO)) & df.Goals_H_FT.notna()]
    return r.sort_values("Date").reset_index(drop=True)


def num(v):
    if v is None:
        return ""
    s = str(v).split("(")[0].strip().replace("%", "")
    try:
        return float(s)
    except Exception:
        return ""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dias", type=int, default=300)
    parser.add_argument("--max-req", type=int, default=7500)
    args = parser.parse_args()

    key = H.load_key()
    if not key:
        print("[ERRO] Chave RAPIDAPI_KEY não encontrada em .rapidapi_key nem no ambiente.")
        sys.exit(1)

    # 1. Carregar registros existentes para não duplicar
    ja_processados = set()
    linhas = []
    if os.path.exists(SAIDA):
        print(f"[*] Carregando base expandida existente: {SAIDA}")
        df_existente = pd.read_csv(SAIDA, dtype={"eventid": str})
        for _, row in df_existente.iterrows():
            chave = (str(row.data), str(row.home), str(row.away))
            ja_processados.add(chave)
            linhas.append(list(row.values))
    elif os.path.exists(ORIGINAL):
        print(f"[*] Inicializando a partir da base original: {ORIGINAL}")
        df_orig = pd.read_csv(ORIGINAL, dtype={"eventid": str})
        for _, row in df_orig.iterrows():
            chave = (str(row.data), str(row.home), str(row.away))
            ja_processados.add(chave)
            linhas.append(list(row.values))

    print(f"[*] Partidas já catalogadas no CSV: {len(ja_processados)}")

    alvo = carregar_alvos(args.dias)
    print(f"[*] Partidas elegíveis nas 25 novas ligas: {len(alvo)}")
    print(f"[*] Teto máximo de requisições: {args.max_req}\n")

    casou = comstats = comxg = 0
    novos_adicionados = 0

    for i, row in alvo.iterrows():
        data_str = row.Date.strftime("%Y-%m-%d")
        chave = (data_str, str(row.Home), str(row.Away))
        if chave in ja_processados:
            continue

        cands = []
        try:
            for d in (0, 1, -1):
                dia_str = (row.Date + timedelta(days=d)).strftime("%Y%m%d")
                cands += H.agenda(dia_str, key, args.max_req)
        except RuntimeError as e:
            print(f"\n[PARADA SEGURA] {e}")
            break

        eid = lid = ""
        for m in cands:
            if H.nomes_batem(row.Home, m["home"]) and H.nomes_batem(row.Away, m["away"]):
                eid, lid = m["id"], m["league_id"]
                break

        st = None
        if eid:
            casou += 1
            try:
                st = stats_ft(eid, key, args.max_req)
            except RuntimeError as e:
                print(f"\n[PARADA SEGURA] {e}")
                break

        if st:
            comstats += 1
            if "expected_goals" in st:
                comxg += 1

        base_row = [data_str, row.League, row.Home, row.Away,
                    row.Goals_H_FT, row.Goals_A_FT, row.Odd_H_FT, row.Odd_D_FT, row.Odd_A_FT,
                    row.Odd_Over25_FT, row.Odd_CS_0x0, eid, lid,
                    int(bool(st)), int("expected_goals" in (st or {}))]
        h_stats = [num((st or {}).get(k, [None, None])[0]) if (st or {}).get(k) else "" for k, _ in CAMPOS]
        a_stats = [num((st or {}).get(k, [None, None])[1]) if (st or {}).get(k) else "" for k, _ in CAMPOS]

        linhas.append(base_row + h_stats + a_stats)
        ja_processados.add(chave)
        novos_adicionados += 1

        if novos_adicionados % 50 == 0:
            print(f"  Progresso: {i+1}/{len(alvo)} | Novos: {novos_adicionados} | Casou: {casou} | Stats: {comstats} | xG: {comxg} | Reqs: {H.req[0]} | Cota Restante: {rest_quota[0]}", flush=True)
            with open(SAIDA, "w", encoding="utf-8-sig", newline="") as f:
                w = csv.writer(f)
                w.writerow(COLS)
                w.writerows(linhas)

    with open(SAIDA, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(COLS)
        w.writerows(linhas)

    print("\n" + "=" * 70)
    print(f"[CONCLUÍDO] Gravado com sucesso em: {SAIDA}")
    print(f"Total consolidado no CSV: {len(linhas)} partidas")
    print(f"Novas partidas adicionadas: {novos_adicionados}")
    print(f"Total com estatísticas: {comstats} | Total com xG: {comxg}")
    print(f"Requisições consumidas nesta execução: {H.req[0]}")
    print("=" * 70)


if __name__ == "__main__":
    main()
