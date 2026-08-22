import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np

print("=== INICIANDO AUDITORIA E BACKTEST MESTRE DEFINITIVO (2025 E 2026) ===", flush=True)

# 1. Carregar base historica
df_raw = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Bet365.csv", low_memory=False)
df_raw["d_str"] = pd.to_datetime(df_raw["Date"], errors='coerce').dt.strftime("%Y-%m-%d")

# Funcao para extrair numeros com seguranca
def get_num(df, cols):
    for c in cols:
        if c in df.columns:
            s = pd.to_numeric(df[c], errors='coerce')
            if s.notna().sum() > 0:
                return s
    return pd.Series(np.nan, index=df.index)

df = df_raw.copy()
df["gh"] = get_num(df, ["Goals_H_FT", "Home_Score", "Goals_H"])
df["ga"] = get_num(df, ["Goals_A_FT", "Away_Score", "Goals_A"])
df["odd_h"] = get_num(df, ["Odd_H_FT", "Odd_H_FT_Back", "Odd_H", "Odd_H_Back"])
df["odd_d"] = get_num(df, ["Odd_D_FT", "Odd_D_FT_Back", "Odd_D", "Odd_D_Back", "Odd_D_Lay"])
df["odd_a"] = get_num(df, ["Odd_A_FT", "Odd_A_FT_Back", "Odd_A", "Odd_A_Back"])
df["odd_u25"] = get_num(df, ["Odd_Under25_FT_Back", "Odd_Under25_FT", "Odd_Under25"])
df["odd_1x0"] = get_num(df, ["Odd_CS_1x0_Lay", "Odd_CS_1x0"])
df["odd_2x0"] = get_num(df, ["Odd_CS_2x0_Lay", "Odd_CS_2x0"])
df["odd_0x2"] = get_num(df, ["Odd_CS_0x2_Lay", "Odd_CS_0x2"])
df["odd_0x0"] = get_num(df, ["Odd_CS_0x0_Lay", "Odd_CS_0x0"])
df["odd_2x2"] = get_num(df, ["Odd_CS_2x2_Lay", "Odd_CS_2x2"])
df["odd_0x3"] = get_num(df, ["Odd_CS_0x3_Lay", "Odd_CS_0x3"])
df["xga"] = get_num(df, ["A_xGF_r5", "Media_Gols_Pro_Visitante", "xG_A_FT", "xg_a"])

# Filtrar somente jogos com placar real e finalizado
df_clean = df[df["gh"].notna() & df["ga"].notna() & (df["d_str"] <= "2026-08-20")].copy()

def run_strategy_exact(df_input, strat_name):
    ops = []
    
    for idx, r in df_input.iterrows():
        gh_i = int(r["gh"]); ga_i = int(r["ga"])
        o_h = r["odd_h"]; o_d = r["odd_d"]; o_a = r["odd_a"]
        o_u25 = r["odd_u25"]
        
        # 1. LAY DRAW (LAY EMPATE)
        if strat_name == "Lay Draw":
            odd = o_d
            if pd.isna(odd) or odd < 3.0 or odd > 5.50: continue
            is_draw = (gh_i == ga_i)
            res = "GREEN" if not is_draw else "RED"
            pnl = 95.0 if not is_draw else -(odd - 1.0) * 100.0
            ops.append({"Date": r["d_str"], "Home": r.get("Home"), "Away": r.get("Away"), "Odd": odd, "Placar": f"{gh_i}x{ga_i}", "Resultado": res, "PnL": pnl})
            
        # 2. LAY 1X0
        elif strat_name == "Lay 1x0":
            odd = r["odd_1x0"]
            if pd.isna(odd) or odd < 6.0 or odd > 9.50: continue
            is_hit = (gh_i == 1 and ga_i == 0)
            res = "GREEN" if not is_hit else "RED"
            pnl = 95.0 if not is_hit else -(odd - 1.0) * 100.0
            ops.append({"Date": r["d_str"], "Home": r.get("Home"), "Away": r.get("Away"), "Odd": odd, "Placar": f"{gh_i}x{ga_i}", "Resultado": res, "PnL": pnl})

        # 3. LAY 2X2 QUANT
        elif strat_name == "Lay 2x2 Quant":
            odd = r["odd_2x2"]
            if pd.isna(odd) or odd < 8.0 or odd > 20.00: continue
            if pd.isna(o_u25) or o_u25 <= 0 or o_u25 > 2.00: continue
            is_hit = (gh_i == 2 and ga_i == 2)
            res = "GREEN" if not is_hit else "RED"
            pnl = 95.0 if not is_hit else -(odd - 1.0) * 100.0
            ops.append({"Date": r["d_str"], "Home": r.get("Home"), "Away": r.get("Away"), "Odd": odd, "Placar": f"{gh_i}x{ga_i}", "Resultado": res, "PnL": pnl})

        # 4. LAY 2X0 RF V2
        elif strat_name == "Lay 2x0":
            odd = r["odd_2x0"]
            if pd.isna(odd) or odd < 6.0 or odd > 12.00: continue
            if pd.isna(o_u25) or o_u25 <= 0 or o_u25 > 2.10: continue
            is_hit = (gh_i == 2 and ga_i == 0)
            res = "GREEN" if not is_hit else "RED"
            pnl = 95.0 if not is_hit else -(odd - 1.0) * 100.0
            ops.append({"Date": r["d_str"], "Home": r.get("Home"), "Away": r.get("Away"), "Odd": odd, "Placar": f"{gh_i}x{ga_i}", "Resultado": res, "PnL": pnl})

        # 5. LAY 0X2 RF V2
        elif strat_name == "Lay 0x2":
            odd = r["odd_0x2"]
            if pd.isna(odd) or odd < 6.0 or odd > 12.00: continue
            if pd.isna(o_u25) or o_u25 <= 0 or o_u25 > 2.10: continue
            is_hit = (gh_i == 0 and ga_i == 2)
            res = "GREEN" if not is_hit else "RED"
            pnl = 95.0 if not is_hit else -(odd - 1.0) * 100.0
            ops.append({"Date": r["d_str"], "Home": r.get("Home"), "Away": r.get("Away"), "Odd": odd, "Placar": f"{gh_i}x{ga_i}", "Resultado": res, "PnL": pnl})

        # 6. LAY 0X0 RF V2
        elif strat_name == "Lay 0x0":
            odd = r["odd_0x0"]
            if pd.isna(odd) or odd < 6.0 or odd > 14.00: continue
            if pd.isna(o_u25) or o_u25 <= 0 or o_u25 > 2.00: continue
            is_hit = (gh_i == 0 and ga_i == 0)
            res = "GREEN" if not is_hit else "RED"
            pnl = 95.0 if not is_hit else -(odd - 1.0) * 100.0
            ops.append({"Date": r["d_str"], "Home": r.get("Home"), "Away": r.get("Away"), "Odd": odd, "Placar": f"{gh_i}x{ga_i}", "Resultado": res, "PnL": pnl})

        # 7. LAY 0X3 GOLEADA
        elif strat_name == "Lay 0x3":
            odd = r["odd_0x3"]
            if pd.isna(odd) or odd < 14.0 or odd > 35.00: continue
            if pd.isna(o_u25) or o_u25 <= 0 or o_u25 > 2.10: continue
            if pd.notna(o_a) and 0 < o_a < 1.85: continue # Trava visitante super favorito
            is_hit = (gh_i == 0 and ga_i == 3)
            res = "GREEN" if not is_hit else "RED"
            pnl = 95.0 if not is_hit else -(odd - 1.0) * 100.0
            ops.append({"Date": r["d_str"], "Home": r.get("Home"), "Away": r.get("Away"), "Odd": odd, "Placar": f"{gh_i}x{ga_i}", "Resultado": res, "PnL": pnl})

    df_ops = pd.DataFrame(ops)
    if not df_ops.empty:
        tot = len(df_ops)
        grn = (df_ops["Resultado"] == "GREEN").sum()
        red = (df_ops["Resultado"] == "RED").sum()
        wr = (grn / tot) * 100.0
        pnl_val = df_ops["PnL"].sum()
        
        lucro_b = df_ops[df_ops["PnL"] > 0]["PnL"].sum()
        perda_b = abs(df_ops[df_ops["PnL"] < 0]["PnL"].sum())
        pf = (lucro_b / perda_b) if perda_b > 0 else np.nan
        
        return {
            "Estratégia": strat_name,
            "Total Entradas": tot,
            "Greens": grn,
            "Reds": red,
            "Win Rate %": f"{wr:.2f}%",
            "Lucro Líquido R$": f"R$ {pnl_val:,.2f}",
            "Profit Factor": f"{pf:.2f}"
        }, df_ops
    else:
        return {"Estratégia": strat_name, "Total Entradas": 0, "Greens": 0, "Reds": 0, "Win Rate %": "0.00%", "Lucro Líquido R$": "R$ 0.00", "Profit Factor": "0.00"}, pd.DataFrame()

strategies = ["Lay 1x0", "Lay Draw", "Lay 2x2 Quant", "Lay 2x0", "Lay 0x2", "Lay 0x0", "Lay 0x3"]

# A. Periodo 1: Ano 2026 Completo (01/01/2026 a 20/08/2026)
df_2026 = df_clean[(df_clean["d_str"] >= "2026-01-01") & (df_clean["d_str"] <= "2026-08-20")].copy()
res_2026 = []
details_2026 = {}
for s in strategies:
    summary, df_d = run_strategy_exact(df_2026, s)
    res_2026.append(summary)
    details_2026[s] = df_d

df_res_2026 = pd.DataFrame(res_2026)
print("\n" + "="*95, flush=True)
print("=== 1. RELATÓRIO OFICIAL CONSOLIDADO — ANO DE 2026 COMPLETO ===", flush=True)
print("="*95, flush=True)
print(df_res_2026.to_string(index=False), flush=True)

# B. Periodo 2: Ano 2025 Completo (01/01/2025 a 31/12/2025)
df_2025 = df_clean[(df_clean["d_str"] >= "2025-01-01") & (df_clean["d_str"] <= "2025-12-31")].copy()
res_2025 = []
details_2025 = {}
for s in strategies:
    summary, df_d = run_strategy_exact(df_2025, s)
    res_2025.append(summary)
    details_2025[s] = df_d

df_res_2025 = pd.DataFrame(res_2025)
print("\n" + "="*95, flush=True)
print("=== 2. RELATÓRIO OFICIAL CONSOLIDADO — ANO DE 2025 (OUT-OF-SAMPLE) ===", flush=True)
print("="*95, flush=True)
print(df_res_2025.to_string(index=False), flush=True)

with pd.ExcelWriter("Backtest_Mestre_ARKAD_Oficial_2025_2026.xlsx") as writer:
    df_res_2026.to_excel(writer, sheet_name="Resumo_2026", index=False)
    df_res_2025.to_excel(writer, sheet_name="Resumo_2025", index=False)
    for k, v in details_2026.items():
        if not v.empty:
            v.to_excel(writer, sheet_name=f"2026_{k.replace(' ', '_')}"[:31], index=False)

print("\n[+] Planilha Mestre gravada com sucesso: Backtest_Mestre_ARKAD_Oficial_2025_2026.xlsx", flush=True)
