import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np

import coleta_lay_cs_aovivo

hist = coleta_lay_cs_aovivo._hist_df()
print(f"[+] Histórico carregado para Lay 0x1: {len(hist):,} jogos", flush=True)

cfg = coleta_lay_cs_aovivo.MERCADOS["0x1"]

total_0x1 = 0
for day in range(1, 21):
    d_str = f"2026-08-{day:02d}"
    sinais = coleta_lay_cs_aovivo.sinais_do_dia(d_str, cfg)
    if sinais:
        print(f"[{d_str}] {len(sinais)} jogos brutos encontrados no Lay 0x1", flush=True)
        for s in sinais:
            print(f"   * {s.get('Mandante')} x {s.get('Visitante')} | Odd: {s.get('Odd_lay_entrada')} | Prob: {s.get('Prob')} | Método: {s.get('Metodo')}", flush=True)
        total_0x1 += len(sinais)

print(f"\n[+] TOTAL BRUTO DE SINAIS LAY 0X3 EM AGOSTO: {total_0x1}", flush=True)
