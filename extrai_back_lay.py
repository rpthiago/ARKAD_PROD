# -*- coding: utf-8 -*-
"""Extrai do acervo Betfair as capturas PRE-JOGO de dois runners, para calibrar back->lay.

Streaming do .gz: memoria O(1). Saida: um CSV compacto.
  MATCH_ODDS    / "The Draw"  -> para o Lay Draw (existe bfe_d gratis no football-data)
  CORRECT_SCORE / "0 - 0"     -> para o Lay 0x0 (caracteriza o mercado)

Uso: python extrai_back_lay.py <entrada.gz> <saida.csv>
"""
import csv, gzip, sys

ENTRADA, SAIDA = sys.argv[1], sys.argv[2]

ALVOS = {("MATCH_ODDS", "The Draw"): "DRAW",
         ("CORRECT_SCORE", "0 - 0"): "CS00"}

# indices do schema de 15 campos
I_TS, I_MT, I_COMP, I_H, I_A, I_KO, I_MTK, I_RUN = 0, 1, 2, 3, 4, 5, 6, 7
I_BACK, I_BSZ, I_LAY, I_LSZ, I_LTP = 9, 10, 11, 12, 13

lidas = escritas = ruins = 0
with gzip.open(ENTRADA, "rt", encoding="utf-8", errors="replace", newline="") as fin, \
     open(SAIDA, "w", encoding="utf-8", newline="") as fout:
    r = csv.reader(fin)
    w = csv.writer(fout)
    w.writerow(["alvo", "capture_ts", "competition", "home", "away", "ko",
                "min_to_ko", "back", "back_size", "lay", "lay_size", "ltp"])
    next(r, None)  # cabecalho
    for row in r:
        lidas += 1
        if len(row) < 14:
            ruins += 1
            continue
        alvo = ALVOS.get((row[I_MT], row[I_RUN]))
        if alvo is None:
            continue
        try:
            mtk = float(row[I_MTK])
        except ValueError:
            ruins += 1
            continue
        # SO pre-jogo: min_to_ko POSITIVO = minutos ANTES do apito.
        # (negativo = jogo em andamento; ver a convencao do projeto)
        if not (0.0 < mtk <= 40.0):
            continue
        w.writerow([alvo, row[I_TS], row[I_COMP], row[I_H], row[I_A], row[I_KO],
                    row[I_MTK], row[I_BACK], row[I_BSZ], row[I_LAY], row[I_LSZ], row[I_LTP]])
        escritas += 1
        if lidas % 5_000_000 == 0:
            print("  %d linhas lidas, %d escritas" % (lidas, escritas), flush=True)

print("FIM: %d linhas lidas | %d escritas | %d malformadas" % (lidas, escritas, ruins))
