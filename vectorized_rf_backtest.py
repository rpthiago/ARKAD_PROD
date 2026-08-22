import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np

print("=== INICIANDO BACKTEST EMPÍRICO RÁPIDO DOS MÉTODOS RF V2 EM AGOSTO E 2026 COMPLETO ===", flush=True)

df_hist = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Bet365.csv", low_memory=False)
df_hist["d_str"] = pd.to_datetime(df_hist["Date"], errors='coerce').dt.strftime("%Y-%m-%d")

# 1. Backtest em Agosto (01 a 20/08/2026)
df_august = df_hist[(df_hist["d_str"] >= "2026-08-01") & (df_hist["d_str"] <= "2026-08-20")].copy()

# A. Lay 2x0 (Odds 6.00 a 12.00, Under 2.5 <= 2.10)
df_2x0 = df_august.copy()
df_2x0["Odd_2x0"] = pd.to_numeric(df_2x0.get("Odd_CS_2x0_Lay") or df_2x0.get("Odd_CS_2x0"), errors='coerce')
df_2x0["Odd_U25"] = pd.to_numeric(df_2x0.get("Odd_Under25_FT_Back") or df_2x0.get("Odd_Under25"), errors='coerce')
df_2x0_sub = df_2x0[(df_2x0["Odd_2x0"] >= 6.0) & (df_2x0["Odd_2x0"] <= 12.0) & (df_2x0["Odd_U25"] <= 2.10)].copy()

# B. Lay 0x2 (Odds 6.00 a 12.00, Under 2.5 <= 2.10)
df_0x2 = df_august.copy()
df_0x2["Odd_0x2"] = pd.to_numeric(df_0x2.get("Odd_CS_0x2_Lay") or df_0x2.get("Odd_CS_0x2"), errors='coerce')
df_0x2["Odd_U25"] = pd.to_numeric(df_0x2.get("Odd_Under25_FT_Back") or df_0x2.get("Odd_Under25"), errors='coerce')
df_0x2_sub = df_0x2[(df_0x2["Odd_0x2"] >= 6.0) & (df_0x2["Odd_0x2"] <= 12.0) & (df_0x2["Odd_U25"] <= 2.10)].copy()

# C. Lay 0x0 (Odds 6.00 a 14.00, Under 2.5 <= 2.00)
df_0x0 = df_august.copy()
df_0x0["Odd_0x0"] = pd.to_numeric(df_0x0.get("Odd_CS_0x0_Lay") or df_0x0.get("Odd_CS_0x0"), errors='coerce')
df_0x0["Odd_U25"] = pd.to_numeric(df_0x0.get("Odd_Under25_FT_Back") or df_0x0.get("Odd_Under25"), errors='coerce')
df_0x0_sub = df_0x0[(df_0x0["Odd_0x0"] >= 6.0) & (df_0x0["Odd_0x0"] <= 14.0) & (df_0x0["Odd_U25"] <= 2.00)].copy()

# D. Lay 1x0 (Odds 6.00 a 9.50)
df_1x0 = df_august.copy()
df_1x0["Odd_1x0"] = pd.to_numeric(df_1x0.get("Odd_CS_1x0_Lay") or df_1x0.get("Odd_CS_1x0"), errors='coerce')
df_1x0["Odd_U25"] = pd.to_numeric(df_1x0.get("Odd_Under25_FT_Back") or df_1x0.get("Odd_Under25"), errors='coerce')
df_1x0_sub = df_1x0[(df_1x0["Odd_1x0"] >= 6.0) & (df_1x0["Odd_1x0"] <= 9.5)].copy()

summary = []

for name, sub, target_h, target_a, odd_col in [
    ("Lay 2x0 RF v2", df_2x0_sub, 2, 0, "Odd_2x0"),
    ("Lay 0x2 RF v2", df_0x2_sub, 0, 2, "Odd_0x2"),
    ("Lay 0x0 RF v2", df_0x0_sub, 0, 0, "Odd_0x0"),
    ("Lay 1x0 RF v2", df_1x0_sub, 1, 0, "Odd_1x0")
]:
    if not sub.empty:
        ops = []
        for idx, r in sub.iterrows():
            gh = r.get("Goals_H_FT") if pd.notna(r.get("Goals_H_FT")) else r.get("Home_Score")
            ga = r.get("Goals_A_FT") if pd.notna(r.get("Goals_A_FT")) else r.get("Away_Score")
            if pd.notna(gh) and pd.notna(ga):
                gh_i = int(float(gh)); ga_i = int(float(ga))
                odd = float(r[odd_col])
                is_hit = (gh_i == target_h and ga_i == target_a)
                res = "GREEN" if not is_hit else "RED"
                pnl = 95.0 if not is_hit else -(odd - 1.0) * 100.0
                ops.append({"Resultado": res, "PnL": pnl})
        df_ops = pd.DataFrame(ops)
        if not df_ops.empty:
            tot = len(df_ops)
            grn = (df_ops["Resultado"] == "GREEN").sum()
            red = (df_ops["Resultado"] == "RED").sum()
            wr = (grn / tot) * 100.0
            pnl_val = df_ops["PnL"].sum()
            summary.append({
                "Estratégia": name,
                "Entradas em Agosto": tot,
                "Greens": grn,
                "Reds": red,
                "Win Rate %": f"{wr:.2f}%",
                "Lucro Acumulado R$": f"R$ {pnl_val:,.2f}"
            })

df_res = pd.DataFrame(summary)
print("\n" + "="*80, flush=True)
print("=== RESUMO DOS BACKTESTS EM AGOSTO DE 2026 (01 A 20/08) ===", flush=True)
print("="*80, flush=True)
print(df_res.to_string(index=False), flush=True)
