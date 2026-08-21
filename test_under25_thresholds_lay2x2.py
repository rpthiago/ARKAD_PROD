import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np

print("=== TESTANDO LIMITES DA ODD UNDER 2.5 NO LAY 2X2 (2026 COMPLETO) ===", flush=True)

df_hist = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Bet365.csv", low_memory=False)
df_hist["d_str"] = pd.to_datetime(df_hist["Date"], errors='coerce').dt.strftime("%Y-%m-%d")

df_2026 = df_hist[(df_hist["d_str"] >= "2026-01-01") & (df_hist["d_str"] <= "2026-08-20")].copy()

thresholds = [1.80, 1.85, 1.90, 1.95, 2.00, 2.10]
summary = []

for u_cut in thresholds:
    ops = []
    for idx, r in df_2026.iterrows():
        o_2x2 = float(pd.to_numeric(r.get('Odd_CS_2x2_Lay') or r.get('Odd_CS_2x2'), errors='coerce') or 0.0)
        o_u25 = float(pd.to_numeric(r.get('Odd_Under25_FT_Back') or r.get('Odd_Under25_FT') or r.get('Odd_Under25'), errors='coerce') or 0.0)
        o_h = float(pd.to_numeric(r.get('Odd_H_Back') or r.get('Odd_H_FT') or r.get('Odd_H'), errors='coerce') or 0.0)
        o_a = float(pd.to_numeric(r.get('Odd_A_Back') or r.get('Odd_A_FT') or r.get('Odd_A'), errors='coerce') or 0.0)
        
        gh = r.get("Goals_H_FT") if pd.notna(r.get("Goals_H_FT")) else r.get("Home_Score")
        ga = r.get("Goals_A_FT") if pd.notna(r.get("Goals_A_FT")) else r.get("Away_Score")
        
        if pd.isna(gh) or pd.isna(ga) or o_2x2 <= 1.0: continue
        
        gh_i = int(float(gh)); ga_i = int(float(ga))
        is_2x2 = (gh_i == 2 and ga_i == 2)
        
        # Testar com Teto 20.0 + Under 2.5 <= u_cut + Favorito
        passou = (0.0 < o_u25 <= u_cut) or (o_h > 0 and o_h <= 1.75) or (o_a > 0 and o_a <= 1.75)
        if 8.0 <= o_2x2 <= 20.0 and passou:
            res = "GREEN" if not is_2x2 else "RED"
            pnl = 95.0 if not is_2x2 else -(o_2x2 - 1.0) * 100.0
            ops.append({"Resultado": res, "PnL": pnl})
            
    df_ops = pd.DataFrame(ops)
    if not df_ops.empty:
        tot = len(df_ops)
        grn = (df_ops["Resultado"] == "GREEN").sum()
        red = (df_ops["Resultado"] == "RED").sum()
        wr = (grn / tot) * 100.0
        pnl = df_ops["PnL"].sum()
        summary.append({
            "Filtro Odd Under 2.5": f"Under 2.5 <= {u_cut:.2f}",
            "Total Entradas": tot,
            "Greens": grn,
            "Reds": red,
            "Win Rate %": f"{wr:.2f}%",
            "Lucro Acumulado 2026": f"R$ {pnl:,.2f}"
        })

df_res = pd.DataFrame(summary)
print("\n" + "="*80, flush=True)
print("=== SENSIBILIDADE DO CORTE DA ODD UNDER 2.5 (2026 COMPLETO) ===", flush=True)
print("="*80, flush=True)
print(df_res.to_string(index=False), flush=True)
