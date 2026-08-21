import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np, os

from lay_goleada_quant_strategy import aplicar_lay_goleada

print("=== INICIANDO BACKTEST COMPLETO E OFICIAL DO LAY 0X3 (JANEIRO A AGOSTO 2026) ===", flush=True)

df_raw = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Bet365.csv", low_memory=False)
print(f"[+] Base histórica carregada: {len(df_raw):,} jogos", flush=True)

# Normalizar colunas de Odds e Placares
df_raw["Date"] = pd.to_datetime(df_raw["Date"], errors="coerce")
df_2026 = df_raw[df_raw["Date"].dt.year == 2026].copy()
print(f"[+] Total de jogos em 2026 na base primária: {len(df_2026):,}", flush=True)

# Adicionar resolvedores de odds se necessário
if 'Odd_CS_0x3_Lay' not in df_2026.columns and 'Odd_CS_0x3' in df_2026.columns:
    df_2026['Odd_CS_0x3_Lay'] = df_2026['Odd_CS_0x3']

df_ops = aplicar_lay_goleada(df_2026)

if df_ops.empty:
    print("Nenhuma operação gerada no backtest de 2026.", flush=True)
else:
    df_ops['data'] = pd.to_datetime(df_ops['data'], errors='coerce')
    df_ops['Ano'] = df_ops['data'].dt.year
    df_ops['Mes'] = df_ops['data'].dt.strftime('%Y-%m')

    df_2026_ops = df_ops[df_ops['Ano'] == 2026].copy()

    tot_2026 = len(df_2026_ops)
    grn_2026 = int((df_2026_ops['resultado'] == 'GREEN').sum())
    red_2026 = int((df_2026_ops['resultado'] == 'RED').sum())
    wr_2026 = (grn_2026 / tot_2026 * 100.0) if tot_2026 > 0 else 0.0
    pnl_2026 = df_2026_ops['pnl_dolar'].sum() if tot_2026 > 0 else 0.0

    print(f"\n=======================================================", flush=True)
    print("=== RESUMO OFICIAL DO LAY 0X3 EM 2026 (JAN A AGO) ===", flush=True)
    print("=======================================================", flush=True)
    print(f"Total de Operações Aprovadas : {tot_2026:,}", flush=True)
    print(f"Greens                       : {grn_2026:,} ({wr_2026:.2f}%)", flush=True)
    print(f"Reds                         : {red_2026:,}", flush=True)
    print(f"Lucro Líquido Acumulado (R$) : R$ {pnl_2026:,.2f}", flush=True)

    monthly = []
    for mes, df_m in df_2026_ops.groupby('Mes'):
        tot_m = len(df_m)
        grn_m = int((df_m['resultado'] == 'GREEN').sum())
        red_m = int((df_m['resultado'] == 'RED').sum())
        wr_m = (grn_m / tot_m * 100.0) if tot_m > 0 else 0.0
        pnl_m = df_m['pnl_dolar'].sum()
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

    # Salvar planilha completa
    with pd.ExcelWriter("Backtest_Lay0x3_Oficial_2026_Janeiro_a_Agosto.xlsx") as writer:
        df_mon.to_excel(writer, sheet_name="Resumo_Mensal", index=False)
        df_2026_ops.to_excel(writer, sheet_name="Operacoes_Detalhadas", index=False)

    print("\n[+] Planilha oficial gravada: Backtest_Lay0x3_Oficial_2026_Janeiro_a_Agosto.xlsx", flush=True)
