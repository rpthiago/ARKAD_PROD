# -*- coding: utf-8 -*-
"""Extrai MATCH_ODDS com os TRES runners, pre-jogo, para aplicar Fav = min(Odd_H, Odd_A) <= 1,40.
O runner de casa/visitante e o proprio nome do time (campos 3 e 4 da linha)."""
import csv, gzip, sys

ENT, SAI = sys.argv[1], sys.argv[2]
I_TS, I_MT, I_COMP, I_H, I_A, I_KO, I_MTK, I_RUN = 0, 1, 2, 3, 4, 5, 6, 7
I_BACK, I_BSZ, I_LAY, I_LSZ = 9, 10, 11, 12

lidas = esc = 0
with gzip.open(ENT, "rt", encoding="utf-8", errors="replace", newline="") as fi, \
     open(SAI, "w", encoding="utf-8", newline="") as fo:
    r, w = csv.reader(fi), csv.writer(fo)
    w.writerow(["capture_ts", "competition", "home", "away", "ko", "min_to_ko",
                "lado", "back", "back_size", "lay", "lay_size"])
    next(r, None)
    for row in r:
        lidas += 1
        if len(row) < 13 or row[I_MT] != "MATCH_ODDS":
            continue
        try:
            mtk = float(row[I_MTK])
        except ValueError:
            continue
        if not (0.0 < mtk <= 40.0):   # pre-jogo: min_to_ko POSITIVO
            continue
        run = row[I_RUN]
        if run == "The Draw":
            lado = "D"
        elif run == row[I_H]:
            lado = "H"
        elif run == row[I_A]:
            lado = "A"
        else:
            continue
        w.writerow([row[I_TS], row[I_COMP], row[I_H], row[I_A], row[I_KO], row[I_MTK],
                    lado, row[I_BACK], row[I_BSZ], row[I_LAY], row[I_LSZ]])
        esc += 1
print("FIM: %d lidas | %d escritas" % (lidas, esc))
