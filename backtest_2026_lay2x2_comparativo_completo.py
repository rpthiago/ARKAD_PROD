import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np

print("=== INICIANDO BACKTEST EMPÍRICO 2026 COMPLETO — LAY 2X2 QUANT (SEM SUPOSIÇÕES) ===", flush=True)

df_hist = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Bet365.csv", low_memory=False)
df_hist["d_str"] = pd.to_datetime(df_hist["Date"], errors='coerce').dt.strftime("%Y-%m-%d")

df_2026 = df_hist[(df_hist["d_str"] >= "2026-01-01") & (df_hist["d_str"] <= "2026-08-20")].copy()
print(f"[+] Total de jogos na base histórica de 2026: {len(df_2026):,} partidas", flush=True)

results_A = []  # Teto 14.00 + Strict Under 2.5 <= 2.00
results_B = []  # Teto 20.00 + Strict Under 2.5 <= 2.00
results_C = []  # Teto 20.00 + Com Favorito Claro (Antigo)

for idx, r in df_2026.iterrows():
    o_2x2 = float(pd.to_numeric(r.get('Odd_CS_2x2_Lay') or r.get('Odd_CS_2x2'), errors='coerce') or 0.0)
    o_u25 = float(pd.to_numeric(r.get('Odd_Under25_FT_Back') or r.get('Odd_Under25_FT') or r.get('Odd_Under25'), errors='coerce') or 0.0)
    o_h = float(pd.to_numeric(r.get('Odd_H_Back') or r.get('Odd_H_FT') or r.get('Odd_H'), errors='coerce') or 0.0)
    o_a = float(pd.to_numeric(r.get('Odd_A_Back') or r.get('Odd_A_FT') or r.get('Odd_A'), errors='coerce') or 0.0)
    
    gh = r.get("Goals_H_FT") if pd.notna(r.get("Goals_H_FT")) else r.get("Home_Score")
    ga = r.get("Goals_A_FT") if pd.notna(r.get("Goals_A_FT")) else r.get("Away_Score")
    
    if pd.isna(gh) or pd.isna(ga) or o_2x2 <= 1.0:
        continue
        
    gh_i = int(float(gh)); ga_i = int(float(ga))
    is_2x2 = (gh_i == 2 and ga_i == 2)
    
    # -------------------------------------------------------------
    # CENÁRIO A: Teto 14.00 + Strict Under 2.5 <= 2.00
    # -------------------------------------------------------------
    if 8.0 <= o_2x2 <= 14.0 and 0.0 < o_u25 <= 2.00:
        res = "GREEN" if not is_2x2 else "RED"
        pnl = 95.0 if not is_2x2 else -(o_2x2 - 1.0) * 100.0
        results_A.append({"Date": r["d_str"], "Home": r.get("Home"), "Away": r.get("Away"), "Odd_2x2": o_2x2, "Odd_U25": o_u25, "Resultado": res, "PnL": pnl})
        
    # -------------------------------------------------------------
    # CENÁRIO B: Teto 20.00 + Strict Under 2.5 <= 2.00
    # -------------------------------------------------------------
    if 8.0 <= o_2x2 <= 20.0 and 0.0 < o_u25 <= 2.00:
        res = "GREEN" if not is_2x2 else "RED"
        pnl = 95.0 if not is_2x2 else -(o_2x2 - 1.0) * 100.0
        results_B.append({"Date": r["d_str"], "Home": r.get("Home"), "Away": r.get("Away"), "Odd_2x2": o_2x2, "Odd_U25": o_u25, "Resultado": res, "PnL": pnl})

    # -------------------------------------------------------------
    # CENÁRIO C: Teto 20.00 + Com Favorito Claro (Antigo)
    # -------------------------------------------------------------
    passou_c = (0.0 < o_u25 <= 2.00) or (o_h > 0 and o_h <= 1.75) or (o_a > 0 and o_a <= 1.75)
    if 8.0 <= o_2x2 <= 20.0 and passou_c:
        res = "GREEN" if not is_2x2 else "RED"
        pnl = 95.0 if not is_2x2 else -(o_2x2 - 1.0) * 100.0
        results_C.append({"Date": r["d_str"], "Home": r.get("Home"), "Away": r.get("Away"), "Odd_2x2": o_2x2, "Odd_U25": o_u25, "Resultado": res, "PnL": pnl})

df_A = pd.DataFrame(results_A)
df_B = pd.DataFrame(results_B)
df_C = pd.DataFrame(results_C)

print("\n" + "="*80, flush=True)
print("=== COMPARATIVO OFICIAL DO ANO 2026 COMPLETO (JANEIRO A AGOSTO) ===", flush=True)
print("="*80, flush=True)

def calc_stats(df, nome):
    if df.empty: return {}
    tot = len(df)
    grn = (df["Resultado"] == "GREEN").sum()
    red = (df["Resultado"] == "RED").sum()
    wr = (grn / tot) * 100.0
    pnl = df["PnL"].sum()
    max_odd_red = df[df["Resultado"] == "RED"]["Odd_2x2"].max() if red > 0 else 0.0
    return {
        "Cenário": nome,
        "Total Entradas": tot,
        "Greens": grn,
        "Reds": red,
        "Win Rate %": f"{wr:.2f}%",
        "Maior Odd em RED": max_odd_red,
        "Lucro Acumulado R$": f"R$ {pnl:,.2f}"
    }

summary = [
    calc_stats(df_A, "Cenário A: Teto 14.00 + Strict Under 2.5 <= 2.00"),
    calc_stats(df_B, "Cenário B: Teto 20.00 + Strict Under 2.5 <= 2.00"),
    calc_stats(df_C, "Cenário C: Teto 20.00 + Com Favorito Claro (Antigo)")
]

df_sum = pd.DataFrame(summary)
print(df_sum.to_string(index=False), flush=True)

df_A["Mes"] = df_A["Date"].str[:7]
df_B["Mes"] = df_B["Date"].str[:7]
df_C["Mes"] = df_C["Date"].str[:7]

print("\n=== DETALHAMENTO MÊS A MÊS EM 2026 — CENÁRIO A (TETO 14.00 STRICT) ===", flush=True)
monthly_A = df_A.groupby("Mes").apply(lambda g: pd.Series({
    "Entradas": len(g),
    "Greens": (g["Resultado"]=="GREEN").sum(),
    "Reds": (g["Resultado"]=="RED").sum(),
    "Win Rate": f"{(g['Resultado']=='GREEN').mean()*100:.2f}%",
    "PnL": f"R$ {g['PnL'].sum():,.2f}"
})).reset_index()
print(monthly_A.to_string(index=False), flush=True)

print("\n=== DETALHAMENTO MÊS A MÊS EM 2026 — CENÁRIO B (TETO 20.00 STRICT) ===", flush=True)
monthly_B = df_B.groupby("Mes").apply(lambda g: pd.Series({
    "Entradas": len(g),
    "Greens": (g["Resultado"]=="GREEN").sum(),
    "Reds": (g["Resultado"]=="RED").sum(),
    "Win Rate": f"{(g['Resultado']=='GREEN').mean()*100:.2f}%",
    "PnL": f"R$ {g['PnL'].sum():,.2f}"
})).reset_index()
print(monthly_B.to_string(index=False), flush=True)

print("\n=== DETALHAMENTO MÊS A MÊS EM 2026 — CENÁRIO C (TETO 20.00 COM FAVORITO) ===", flush=True)
monthly_C = df_C.groupby("Mes").apply(lambda g: pd.Series({
    "Entradas": len(g),
    "Greens": (g["Resultado"]=="GREEN").sum(),
    "Reds": (g["Resultado"]=="RED").sum(),
    "Win Rate": f"{(g['Resultado']=='GREEN').mean()*100:.2f}%",
    "PnL": f"R$ {g['PnL'].sum():,.2f}"
})).reset_index()
print(monthly_C.to_string(index=False), flush=True)

with pd.ExcelWriter("Backtest_2026_Lay2x2_Comparativo_Completo.xlsx") as writer:
    df_sum.to_excel(writer, sheet_name="Resumo_Geral", index=False)
    monthly_A.to_excel(writer, sheet_name="Mes_A_Teto14", index=False)
    monthly_B.to_excel(writer, sheet_name="Mes_B_Teto20_Strict", index=False)
    monthly_C.to_excel(writer, sheet_name="Mes_C_Teto20_Fav", index=False)

print("\n[+] Planilha salva com sucesso: Backtest_2026_Lay2x2_Comparativo_Completo.xlsx", flush=True)
