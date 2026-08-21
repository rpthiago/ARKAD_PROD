import pandas as pd, numpy as np
import b365_data_utils
from futpythontrader_client import get_daily_dataframe

date_str = "2026-08-20"
df_today = get_daily_dataframe("betfair", date_str)

print("=== INSPEÇÃO DETALHADA DOS JOGOS DE HOJE (2026-08-20) ===", flush=True)
print(f"[+] Total de partidas na API Betfair para hoje: {len(df_today)}", flush=True)

if not df_today.empty:
    print("\n--- AMOSTRA DAS PRIMEIRAS 15 PARTIDAS DE HOJE ---", flush=True)
    show_cols = [c for c in ["Time", "League", "Home", "Away", "Odd_CS_0x0_Lay", "Odd_CS_0x1_Lay", "Odd_CS_1x0_Lay", "Odd_CS_2x0_Lay", "Odd_CS_2x2_Lay"] if c in df_today.columns]
    print(df_today[show_cols].head(15).to_string(), flush=True)

    print("\n--- ANÁLISE DE ODDS DE HOJE ---", flush=True)
    if "Odd_CS_2x0_Lay" in df_today.columns:
        o20 = pd.to_numeric(df_today["Odd_CS_2x0_Lay"], errors="coerce")
        print(f"Lay 2x0 - Validos (>0): {o20.notna().sum()}, Na faixa (6-12): {((o20>=6.0)&(o20<=12.0)).sum()}", flush=True)
    if "Odd_CS_0x1_Lay" in df_today.columns:
        o01 = pd.to_numeric(df_today["Odd_CS_0x1_Lay"], errors="coerce")
        print(f"Lay 0x1 - Validos (>0): {o01.notna().sum()}, Na faixa (6-12): {((o01>=6.0)&(o01<=12.0)).sum()}", flush=True)
    if "Odd_CS_0x0_Lay" in df_today.columns:
        o00 = pd.to_numeric(df_today["Odd_CS_0x0_Lay"], errors="coerce")
        print(f"Lay 0x0 - Validos (>0): {o00.notna().sum()}, Na faixa (8-16): {((o00>=8.0)&(o00<=16.0)).sum()}", flush=True)
    if "Odd_CS_2x2_Lay" in df_today.columns:
        o22 = pd.to_numeric(df_today["Odd_CS_2x2_Lay"], errors="coerce")
        print(f"Lay 2x2 - Validos (>0): {o22.notna().sum()}, Na faixa (8-14): {((o22>=8.0)&(o22<=14.0)).sum()}", flush=True)
