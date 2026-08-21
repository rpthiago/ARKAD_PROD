import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np
from futpythontrader_client import get_daily_dataframe
import coleta_lay_cs_aovivo

print("=== BACKTEST RÁPIDO DO LAY 0X1 EM AGOSTO (01 A 20/08) ===", flush=True)

hist = coleta_lay_cs_aovivo._hist_df()
cfg_0x1 = coleta_lay_cs_aovivo.MERCADOS["0x1"]

ops_0x1 = []

for day in range(1, 21):
    d_str = f"2026-08-{day:02d}"
    df_day = get_daily_dataframe("betfair", d_str)
    if df_day is None or df_day.empty: continue
    
    sinais = coleta_lay_cs_aovivo.sinais_do_dia(d_str, cfg_0x1)
    if sinais:
        for s in sinais:
            home = str(s.get("Mandante", ""))
            away = str(s.get("Visitante", ""))
            odd_lay = float(pd.to_numeric(s.get("Odd_lay_entrada", 0.0), errors="coerce") or 0.0)
            
            match_row = df_day[(df_day["Home"].astype(str).str.contains(home[:5], case=False, na=False)) & (df_day["Away"].astype(str).str.contains(away[:5], case=False, na=False))]
            if not match_row.empty:
                r = match_row.iloc[0]
                gh = r.get("Goals_H_FT") or r.get("Home_Score") or r.get("gols_mandante")
                ga = r.get("Goals_A_FT") or r.get("Away_Score") or r.get("gols_visitante")
                if gh is not None and ga is not None and pd.notna(gh) and pd.notna(ga):
                    gh_i = int(float(gh)); ga_i = int(float(ga))
                    is_0x1 = (gh_i == 0 and ga_i == 1)
                    res = "GREEN" if not is_0x1 else "RED"
                    pnl = 95.0 if not is_0x1 else -(odd_lay - 1.0) * 100.0
                    ops_0x1.append({
                        "Data": d_str, "Confronto": f"{home} x {away}", "Odd_Lay_0x1": odd_lay,
                        "Placar": f"{gh_i}x{ga_i}", "Resultado": res, "PnL_R$": pnl
                    })

df_0x1 = pd.DataFrame(ops_0x1)

print("\n" + "="*80, flush=True)
print("=== RESUMO DO LAY 0X1 (IA TRADER & RF V2) EM AGOSTO (01 A 20/08) ===", flush=True)
print("="*80, flush=True)

if not df_0x1.empty:
    tot = len(df_0x1)
    grn = (df_0x1["Resultado"] == "GREEN").sum()
    red = (df_0x1["Resultado"] == "RED").sum()
    wr = (grn / tot * 100.0) if tot > 0 else 0.0
    pnl = df_0x1["PnL_R$"].sum()
    
    print(f"Total de Operações Finalizadas : {tot}", flush=True)
    print(f"Greens                          : {grn} ({wr:.2f}%)", flush=True)
    print(f"Reds                            : {red}", flush=True)
    print(f"Lucro Líquido Acumulado         : R$ {pnl:,.2f}", flush=True)
    print("\n=== PRIMEIRAS 25 OPERAÇÕES DE AGOSTO ===", flush=True)
    print(df_0x1.head(25).to_string(index=False), flush=True)

df_0x1.to_excel("Backtest_Lay0x1_Agosto_Finalizado.xlsx", index=False)
