import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np

df = pd.read_excel("Backtest_Lay2x2_Lay0x3_Agosto_2026_Completo.xlsx")
df_0x3 = df[df["Metodo"] == "Lay 0x3 Visitante"].copy()

print("=== SINAIS APROVADOS NO LAY 0X3 VISITANTE EM AGOSTO DE 2026 (01-20/08) ===", flush=True)
print(f"[+] Total de jogos aprovados: {len(df_0x3)}", flush=True)

df_disp = df_0x3[["d_str", "Home", "Away", "League", "Odd_Exec", "gh", "ga", "Resultado", "PnL_R$"]].copy()
df_disp.columns = ["Data", "Mandante", "Visitante", "Liga", "Odd Lay 0x3", "Gols H", "Gols A", "Resultado", "Lucro R$"]

print(df_disp.to_string(index=False), flush=True)
