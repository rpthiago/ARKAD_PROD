import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np, os

from _backtest_saldo_menor import load_historical_datasets, run_saldo_menor_backtest

print("=== INICIANDO BACKTEST COMPLETO DO MÉTODO SALDO MENOR (2026) ===", flush=True)

df_raw = load_historical_datasets()

# Executar o backtest
df_ops, summary = run_saldo_menor_backtest(df_raw, stake_fixa=100.0)

if 'Date' in df_ops.columns:
    df_ops['Date'] = pd.to_datetime(df_ops['Date'], errors='coerce')
    df_ops['Ano'] = df_ops['Date'].dt.year
    df_ops['Mes'] = df_ops['Date'].dt.strftime('%Y-%m')

df_2026 = df_ops[df_ops['Ano'] == 2026].copy()

print(f"\n=======================================================", flush=True)
print("=== RESUMO GERAL DO SALDO MENOR EM 2026 ===", flush=True)
print("=======================================================", flush=True)

tot_2026 = len(df_2026)
grn_2026 = int((df_2026['Resultado_Str'] == 'GREEN').sum())
red_2026 = tot_2026 - grn_2026
wr_2026 = (grn_2026 / tot_2026 * 100.0) if tot_2026 > 0 else 0.0
pnl_2026 = df_2026['Lucro_Operacao'].sum() if tot_2026 > 0 else 0.0

print(f"Total de Operações (2026) : {tot_2026:,}", flush=True)
print(f"Greens                      : {grn_2026:,} ({wr_2026:.2f}%)", flush=True)
print(f"Reds                        : {red_2026:,}", flush=True)
print(f"Lucro Líquido (R$)          : R$ {pnl_2026:,.2f}", flush=True)

monthly = []
for mes, df_m in df_2026.groupby('Mes'):
    tot_m = len(df_m)
    grn_m = int((df_m['Resultado_Str'] == 'GREEN').sum())
    red_m = tot_m - grn_m
    wr_m = (grn_m / tot_m * 100.0) if tot_m > 0 else 0.0
    pnl_m = df_m['Lucro_Operacao'].sum()
    monthly.append({
        'Mês': mes,
        'Total Entradas': tot_m,
        'Greens': grn_m,
        'Reds': red_m,
        'Win Rate': f"{wr_m:.2f}%",
        'P&L Acumulado (R$)': f"R$ {pnl_m:,.2f}"
    })

df_mon = pd.DataFrame(monthly)
print("\n=== DESEMPENHO MÊS A MÊS EM 2026 ===", flush=True)
print(df_mon.to_string(index=False), flush=True)

df_mon.to_excel("Backtest_Saldo_Menor_2026_Mensal.xlsx", index=False)
