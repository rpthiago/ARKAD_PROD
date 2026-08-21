import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np, os

print("=== INICIANDO BACKTEST EXATO E EMPÍRICO 2026 — LAY 0X3 (COM TRAVA DE VISITANTE FAVORITO) ===", flush=True)

df_raw = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Bet365.csv", low_memory=False)
df_raw["Date"] = pd.to_datetime(df_raw["Date"], errors="coerce")
df_2026 = df_raw[df_raw["Date"].dt.year == 2026].copy()
print(f"[+] Total de jogos na base histórica de 2026: {len(df_2026):,}", flush=True)

sinais = []

for idx, row in df_2026.iterrows():
    date_str = str(row.get('Date'))[:10]
    league = str(row.get('League') or row.get('Liga') or 'Geral')
    home = str(row.get('Home_Team') or row.get('Home') or 'Home')
    away = str(row.get('Away_Team') or row.get('Away') or 'Away')
    
    odd_h = float(row.get('Odd_H_FT') or row.get('Odd_H') or 0.0)
    odd_a = float(row.get('Odd_A_FT') or row.get('Odd_A') or 0.0)
    odd_u25 = float(row.get('Odd_Under25_FT') or row.get('Odd_Under25_FT_Back') or row.get('Odd_Under25') or 0.0)
    odd_0x3 = float(row.get('Odd_CS_0x3_Lay') or row.get('Odd_CS_0x3') or 0.0)
    
    xg_a_val = row.get('A_xGF_r5') or row.get('Media_Gols_Pro_Visitante') or row.get('xG_A_FT') or row.get('xg_a')
    xg_a = float(xg_a_val) if pd.notna(xg_a_val) else 1.0
    
    gh = row.get('Goals_H_FT')
    ga = row.get('Goals_A_FT')
    if pd.isna(gh) or pd.isna(ga): continue
    gh = int(gh); ga = int(ga)
    
    # ---------------------------------------------------------------------
    # CRITÉRIOS ESTÁVEIS DE PRODUÇÃO COM TRAVA DE SEGURANÇA CONTRA VISITANTE FAVORITO:
    # 1. Odd Under 2.5 FT <= 2.10
    # 2. 14.00 <= Odd Lay 0x3 <= 35.00
    # 3. Odd Visitante >= 1.85 (IMPEDE ENTRADA QUANDO O VISITANTE É SUPER FAVORITO <= 1.80)
    # 4. xG Visitante <= 1.10
    # ---------------------------------------------------------------------
    cond_u25 = (0.0 < odd_u25 <= 2.10)
    cond_0x3 = (14.0 <= odd_0x3 <= 35.0)
    cond_away_not_super_fav = (odd_a >= 1.85 or odd_a == 0.0)
    cond_xg = (xg_a <= 1.10)
    
    if cond_u25 and cond_0x3 and cond_away_not_super_fav and cond_xg:
        is_0x3 = (gh == 0 and ga == 3)
        res = "GREEN" if not is_0x3 else "RED"
        pnl = 95.0 if not is_0x3 else -(odd_0x3 - 1.0) * 100.0
        
        sinais.append({
            "data": date_str,
            "liga": league,
            "jogo": f"{home} x {away}",
            "odd_h": odd_h,
            "odd_a": odd_a,
            "odd_u25": odd_u25,
            "odd_0x3": odd_0x3,
            "placar": f"{gh}x{ga}",
            "resultado": res,
            "pnl": pnl
        })

df_res = pd.DataFrame(sinais)

if df_res.empty:
    print("Nenhuma operação gerada com esse filtro.", flush=True)
else:
    df_res['data'] = pd.to_datetime(df_res['data'], errors='coerce')
    df_res['Ano'] = df_res['data'].dt.year
    df_res['Mes'] = df_res['data'].dt.strftime('%Y-%m')
    
    tot = len(df_res)
    grn = int((df_res['resultado'] == 'GREEN').sum())
    red = int((df_res['resultado'] == 'RED').sum())
    wr = (grn / tot * 100.0) if tot > 0 else 0.0
    pnl = df_res['pnl'].sum()
    
    print("\n" + "="*80, flush=True)
    print("=== RESUMO EXECUTIVO 2026 — LAY 0X3 (COM TRAVA DE SEGURANÇA ODD_A >= 1.85) ===", flush=True)
    print("="*80, flush=True)
    print(f"Total de Entradas Aprovadas : {tot:,}", flush=True)
    print(f"Greens                       : {grn:,} ({wr:.2f}%)", flush=True)
    print(f"Reds                         : {red:,}", flush=True)
    print(f"Lucro Líquido Acumulado (R$) : R$ {pnl:,.2f}", flush=True)
    
    monthly = []
    for mes, df_m in df_res.groupby('Mes'):
        tot_m = len(df_m)
        grn_m = int((df_m['resultado'] == 'GREEN').sum())
        red_m = int((df_m['resultado'] == 'RED').sum())
        wr_m = (grn_m / tot_m * 100.0) if tot_m > 0 else 0.0
        pnl_m = df_m['pnl'].sum()
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
    
    with pd.ExcelWriter("Backtest_Lay0x3_Com_Trava_Seguranca_2026.xlsx") as writer:
        df_mon.to_excel(writer, sheet_name="Resumo_Mensal", index=False)
        df_res.to_excel(writer, sheet_name="Operacoes", index=False)
        
    print("\n[+] Planilha gravada com sucesso: Backtest_Lay0x3_Com_Trava_Seguranca_2026.xlsx", flush=True)
