#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
reliquidar_under_oficial.py — RE-liquida (audita) alertas ja marcados como Finalizado, usando o
STATUS OFICIAL da Betfair (list_market_book por market_id, que responde mesmo com o mercado CLOSED:
runner WINNER -> GREEN, LOSER -> RED).

Por que existe: o `settle_betfair.py` so toca em linhas "Pendente". Os 637 alertas do under-limite ja
estao "Finalizado" porque foram liquidados pelo COLETOR — e o Hall of Shame registra que liquidar metodo
UNDER pelo coletor grosso infla o resultado de forma sistematica (varredura de 5 min perde o gol do
minuto 88-95 antes de a Betfair fechar o mercado; ja mostrou +26% onde o real era -18%). Este script
mede a divergencia jogo a jogo.

  python3 reliquidar_under_oficial.py [--log arquivo.csv] [--aplicar] [--max N]

Sem --aplicar e so relatorio (nao escreve nada). Com --aplicar, reescreve o log guardando
`resultado_coletor` e `pnl_coletor` ao lado do oficial, para a auditoria nunca perder o original.
"""
import os, sys, csv, argparse
sys.path.insert(0, "/home/ubuntu/betfair-collector")

LOG_PADRAO = "/home/ubuntu/betfair-collector/under_alertas_log.csv"
COMM = 0.05          # convencao da casa (o settle_betfair.py antigo usava 0.045)


def pnl_back(odd, green, comm=COMM):
    return (odd - 1) * (1 - comm) if green else -1.0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", default=LOG_PADRAO)
    ap.add_argument("--aplicar", action="store_true", help="reescreve o log com o resultado oficial")
    ap.add_argument("--max", type=int, default=0, help="limita quantos mercados consultar (teste)")
    a = ap.parse_args()

    if not os.path.exists(a.log):
        print("nao achei %s" % a.log); sys.exit(1)
    with open(a.log, encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    if not rows or "market_id" not in rows[0]:
        print("log sem market_id — nada a auditar"); sys.exit(1)

    alvo = [r for r in rows if r.get("market_id") and r.get("selection_id") and r.get("odd")]
    print("linhas no log: %d | com market_id + selection_id + odd: %d" % (len(rows), len(alvo)))
    mids = sorted({r["market_id"] for r in alvo})
    if a.max: mids = mids[:a.max]
    print("mercados distintos a consultar: %d" % len(mids))

    import coletar_betfair_direto as C
    from betfairlightweight import filters
    t = C.login()
    pp = filters.price_projection(price_data=["EX_BEST_OFFERS"])
    st = {}
    for i in range(0, len(mids), 25):
        chunk = mids[i:i + 25]
        try:
            books = t.betting.list_market_book(market_ids=chunk, price_projection=pp)
        except Exception as e:
            print("  [book erro] %s" % str(e)[:80]); continue
        for b in books:
            for r in b.runners:
                st[(b.market_id, str(r.selection_id))] = (b.status, r.status)
        if (i // 25) % 5 == 0:
            print("  consultados %d/%d mercados..." % (min(i + 25, len(mids)), len(mids)), flush=True)

    n_of = n_ig = n_dif = 0
    gc = rc = 0            # green->red e red->green
    roi_ant, roi_of, vist = 0.0, 0.0, 0
    for r in alvo:
        k = (r["market_id"], str(r["selection_id"]))
        if k not in st: continue
        mst, rst = st[k]
        if mst != "CLOSED" or rst not in ("WINNER", "LOSER"):
            n_ig += 1; continue
        n_of += 1
        odd = float(r["odd"])
        green_of = (rst == "WINNER")
        green_ant = (r.get("resultado") == "GREEN")
        roi_ant += pnl_back(odd, green_ant); roi_of += pnl_back(odd, green_of); vist += 1
        if green_of != green_ant:
            n_dif += 1
            if green_ant and not green_of: gc += 1
            else: rc += 1
            print("    divergencia: %s %s x %s | odd %.2f | coletor %s -> Betfair %s"
                  % (r.get("data", "")[:10], str(r.get("home", ""))[:18], str(r.get("away", ""))[:18],
                     odd, r.get("resultado"), "GREEN" if green_of else "RED"))
        if a.aplicar:
            r.setdefault("resultado_coletor", r.get("resultado", ""))
            r.setdefault("pnl_coletor", r.get("pnl", ""))
            r["resultado"] = "GREEN" if green_of else "RED"
            r["pnl"] = "%.4f" % pnl_back(odd, green_of)
            r["ft_total"] = "BF:%s" % rst

    print("\n=== RESULTADO DA AUDITORIA ===")
    print("  com status oficial da Betfair: %d | sem resposta / mercado nao fechado: %d" % (n_of, n_ig))
    print("  divergencias: %d (%.1f%%) | falsos GREEN (coletor dizia GREEN, Betfair diz RED): %d | falsos RED: %d"
          % (n_dif, 100.0 * n_dif / max(n_of, 1), gc, rc))
    if vist:
        print("  ROI medio (comissao %.0f%%): coletor %+.2f%% -> oficial %+.2f%% | diferenca %+.2f pp"
              % (100 * COMM, 100 * roi_ant / vist, 100 * roi_of / vist, 100 * (roi_of - roi_ant) / vist))
    if a.aplicar:
        campos = list(rows[0].keys())
        for c in ("resultado_coletor", "pnl_coletor"):
            if c not in campos: campos.append(c)
        with open(a.log, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=campos); w.writeheader()
            for r in rows: w.writerow({c: r.get(c, "") for c in campos})
        print("  log reescrito com o resultado oficial (o do coletor ficou em resultado_coletor/pnl_coletor)")
    else:
        print("  (dry-run: nada foi escrito; use --aplicar para gravar)")


if __name__ == "__main__":
    main()
