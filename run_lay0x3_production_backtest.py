import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np
from lay_goleada_quant_strategy import aplicar_lay_goleada

print("=== BACKTEST OFICIAL DO MÓDULO LAY_GOLEADA_QUANT_STRATEGY (LAY 0X3) ===", flush=True)

df_raw = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Bet365.csv", low_memory=False)
print(f"[+] Total de jogos na base histórica: {len(df_raw):,}", flush=True)

df_raw["Date"] = pd.to_datetime(df_raw.get("Date"), errors="coerce")
df_2026 = df_raw[df_raw["Date"].dt.year == 2026].copy()
print(f"[+] Total de jogos em 2026: {len(df_2026):,}", flush=True)

df_res = aplicar_lay_goleada(df_2026)

if df_res.empty:
    print("Nenhum sinal gerado.", flush=True)
else:
    tot = len(df_res)
    grn = (df_res["resultado"] == "GREEN").sum()
    red = (df_res["resultado"] == "RED").sum()
    wr = (grn / tot) * 100.0 if tot > 0 else 0.0
    pnl = df_res["pnl_dolar"].sum() if "pnl_dolar" in df_res.columns else 0.0
    
    print(f"\n=======================================================", flush=True)
    print(f"=== RESULTADO GERAL LAY 0X3 EM 2026 ===", flush=True)
    print(f"=======================================================", flush=True)
    print(f"Total de Entradas Aprovadas : {tot}", flush=True)
    print(f"Greens                       : {grn} ({wr:.2f}%)", flush=True)
    print(f"Reds                         : {red}", flush=True)
    print(f"Lucro Líquido (R$)           : R$ {pnl:,.2f}", flush=True)
    
    df_res["mes"] = pd.to_datetime(df_res["data"]).dt.strftime("%Y-%m")
    pvt = pd.pivot_table(df_res, values="pnl_dolar", index="mes", columns="resultado", aggfunc="count", fill_value=0)
    print("\n=== DESEMPENHO MÊS A MÊS EM 2026 ===", flush=True)
    print(pvt.to_string(), flush=True)
    
    print("\n=== TODOS OS JOGOS APROVADOS EM 2026 ===", flush=True)
    print(df_res[["data", "liga", "jogo", "odd_execucao", "resultado", "pnl_dolar"]].to_string(index=False), flush=True)
    
    df_res.to_excel("Backtest_Lay0x3_Producao_2026.xlsx", index=False)
