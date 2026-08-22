import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np

import coleta_lay_cs_aovivo

print("=== INICIANDO BACKTEST 2026 COMPLETO DOS MÉTODOS RF V2 (LAY 2X0, 0X2, 0X0, 1X0) ===", flush=True)

df_hist = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Bet365.csv", low_memory=False)
df_hist["d_str"] = pd.to_datetime(df_hist["Date"], errors='coerce').dt.strftime("%Y-%m-%d")

df_2026 = df_hist[(df_hist["d_str"] >= "2026-01-01") & (df_hist["d_str"] <= "2026-08-20")].copy()
print(f"[+] Total de jogos na base histórica de 2026: {len(df_2026):,} partidas", flush=True)

hist_base = coleta_lay_cs_aovivo._hist_df()

strategies = [
    ("Lay 2x0 RF v2", "lay_2x0_rf_v2_strategy", 2, 0),
    ("Lay 0x2 RF v2", "lay_0x2_rf_v2_strategy", 0, 2),
    ("Lay 0x0 RF v2", "lay_0x0_rf_v2_strategy", 0, 0),
    ("Lay 1x0 RF v2", "lay_1x0_rf_v2_strategy", 1, 0)
]

summary_report = []
detailed_dfs = {}

for name, mod_name, target_h, target_a in strategies:
    print(f"\n[+] Executando backtest em 2026 para: {name} ({mod_name})...", flush=True)
    try:
        mod = __import__(mod_name, fromlist=["predict_and_evaluate_live"])
        payload = df_2026.to_dict("records")
        res = mod.predict_and_evaluate_live(payload, hist_base)
        
        aprovados = [g for g in (res or []) if g.get("Decision") == "APOSTA"]
        
        ops = []
        for g in aprovados:
            h = g.get("Home"); a = g.get("Away"); date_v = g.get("Date")
            odd = float(g.get("Odd") or g.get("Odd_CS_2x0_Lay") or g.get("Odd_CS_0x2_Lay") or g.get("Odd_CS_0x0_Lay") or g.get("Odd_CS_1x0_Lay") or 10.0)
            
            row = df_2026[(df_2026["Home"] == h) & (df_2026["Away"] == a)]
            if not row.empty:
                r = row.iloc[0]
                gh = r.get("Goals_H_FT") if pd.notna(r.get("Goals_H_FT")) else r.get("Home_Score")
                ga = r.get("Goals_A_FT") if pd.notna(r.get("Goals_A_FT")) else r.get("Away_Score")
                if pd.notna(gh) and pd.notna(ga):
                    gh_i = int(float(gh)); ga_i = int(float(ga))
                    is_hit = (gh_i == target_h and ga_i == target_a)
                    res_str = "GREEN" if not is_hit else "RED"
                    pnl = 95.0 if not is_hit else -(odd - 1.0) * 100.0
                    ops.append({
                        "Date": str(date_v)[:10], "Home": h, "Away": a, "Odd_Lay": odd,
                        "Placar_Real": f"{gh_i}x{ga_i}", "Resultado": res_str, "PnL": pnl
                    })
                    
        df_ops = pd.DataFrame(ops)
        detailed_dfs[name] = df_ops
        
        if not df_ops.empty:
            tot = len(df_ops)
            grn = (df_ops["Resultado"] == "GREEN").sum()
            red = (df_ops["Resultado"] == "RED").sum()
            wr = (grn / tot) * 100.0
            pnl_tot = df_ops["PnL"].sum()
            summary_report.append({
                "Estratégia": name,
                "Entradas em 2026": tot,
                "Greens": grn,
                "Reds": red,
                "Win Rate %": f"{wr:.2f}%",
                "Lucro Acumulado R$": f"R$ {pnl_tot:,.2f}"
            })
        else:
            summary_report.append({"Estratégia": name, "Entradas em 2026": 0, "Greens": 0, "Reds": 0, "Win Rate %": "0.00%", "Lucro Acumulado R$": "R$ 0.00"})
    except Exception as e:
        print(f"x Erro executando {name}: {e}", flush=True)

df_summary = pd.DataFrame(summary_report)
print("\n" + "="*80, flush=True)
print("=== RESUMO DOS BACKTESTS 2026 COMPLETO (LAY 2X0, 0X2, 0X0, 1X0) ===", flush=True)
print("="*80, flush=True)
print(df_summary.to_string(index=False), flush=True)

with pd.ExcelWriter("Backtest_2026_Metodos_RF_v2_Completo.xlsx") as writer:
    df_summary.to_excel(writer, sheet_name="Resumo_Geral", index=False)
    for k, v in detailed_dfs.items():
        if not v.empty:
            v.to_excel(writer, sheet_name=k.replace(" ", "_"), index=False)

print("\n[+] Relatório detalhado salvo em: Backtest_2026_Metodos_RF_v2_Completo.xlsx", flush=True)
