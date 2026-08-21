import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np, os

from futpythontrader_client import get_daily_dataframe
from metodo_lay2x2_strategy import validar_entrada_lay2x2

print("=== GERANDO RELATÓRIO OFICIAL DE BACKTEST EM AGOSTO (01 A 20/08) ===", flush=True)

# 1. Carregar base de dados histórica para buscar placares reais de Agosto
df_hist = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Bet365.csv", low_memory=False)
df_hist["d_str"] = pd.to_datetime(df_hist["Date"], errors='coerce').dt.strftime("%Y-%m-%d")

def _canon(s):
    import unicodedata, re
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii","ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)

df_august_hist = df_hist[(df_hist["d_str"] >= "2026-08-01") & (df_hist["d_str"] <= "2026-08-20")].copy()

# A. BACKTEST LAY 2X2 QUANT (TETO 20.00)
lay2x2_ops = []
for idx, r in df_august_hist.iterrows():
    o_2x2 = float(pd.to_numeric(r.get('Odd_CS_2x2_Lay') or r.get('Odd_CS_2x2'), errors='coerce') or 0.0)
    o_u25 = float(pd.to_numeric(r.get('Odd_Under25_FT_Back') or r.get('Odd_Under25_FT') or r.get('Odd_Under25'), errors='coerce') or 0.0)
    o_h = float(pd.to_numeric(r.get('Odd_H_Back') or r.get('Odd_H_FT') or r.get('Odd_H'), errors='coerce') or 0.0)
    o_a = float(pd.to_numeric(r.get('Odd_A_Back') or r.get('Odd_A_FT') or r.get('Odd_A'), errors='coerce') or 0.0)
    
    ok, _ = validar_entrada_lay2x2(odd_lay_2x2=o_2x2, odd_under25=o_u25, odd_h=o_h, odd_a=o_a)
    if ok:
        gh = r.get("Goals_H_FT") if pd.notna(r.get("Goals_H_FT")) else r.get("Home_Score")
        ga = r.get("Goals_A_FT") if pd.notna(r.get("Goals_A_FT")) else r.get("Away_Score")
        if pd.notna(gh) and pd.notna(ga):
            gh_i = int(float(gh)); ga_i = int(float(ga))
            is_2x2 = (gh_i == 2 and ga_i == 2)
            res = "GREEN" if not is_2x2 else "RED"
            pnl = 95.0 if not is_2x2 else -(o_2x2 - 1.0) * 100.0
            lay2x2_ops.append({
                "Data": r.get("d_str"), "Confronto": f"{r.get('Home')} x {r.get('Away')}",
                "Odd_Lay_2x2": o_2x2, "Placar": f"{gh_i}x{ga_i}", "Resultado": res, "PnL_R$": pnl
            })

df_2x2 = pd.DataFrame(lay2x2_ops)

# B. BACKTEST LAY 0X1 IA (TRADER & RF V2)
lay0x1_ops = []
for idx, r in df_august_hist.iterrows():
    o_0x1 = float(pd.to_numeric(r.get('Odd_CS_0x1_Lay') or r.get('Odd_CS_0x1'), errors='coerce') or 0.0)
    o_u25 = float(pd.to_numeric(r.get('Odd_Under25_FT_Back') or r.get('Odd_Under25_FT') or r.get('Odd_Under25'), errors='coerce') or 0.0)
    o_h = float(pd.to_numeric(r.get('Odd_H_Back') or r.get('Odd_H_FT') or r.get('Odd_H'), errors='coerce') or 0.0)
    o_a = float(pd.to_numeric(r.get('Odd_A_Back') or r.get('Odd_A_FT') or r.get('Odd_A'), errors='coerce') or 0.0)
    
    # Faixa Trader (10-18) ou Faixa RF (6.0-9.5) com Under 2.5
    if (6.0 <= o_0x1 <= 9.5 and o_u25 <= 2.10) or (10.0 <= o_0x1 <= 18.0 and o_u25 <= 2.10):
        gh = r.get("Goals_H_FT") if pd.notna(r.get("Goals_H_FT")) else r.get("Home_Score")
        ga = r.get("Goals_A_FT") if pd.notna(r.get("Goals_A_FT")) else r.get("Away_Score")
        if pd.notna(gh) and pd.notna(ga):
            gh_i = int(float(gh)); ga_i = int(float(ga))
            is_0x1 = (gh_i == 0 and ga_i == 1)
            res = "GREEN" if not is_0x1 else "RED"
            pnl = 95.0 if not is_0x1 else -(o_0x1 - 1.0) * 100.0
            lay0x1_ops.append({
                "Data": r.get("d_str"), "Confronto": f"{r.get('Home')} x {r.get('Away')}",
                "Odd_Lay_0x1": o_0x1, "Placar": f"{gh_i}x{ga_i}", "Resultado": res, "PnL_R$": pnl
            })

df_0x1 = pd.DataFrame(lay0x1_ops)

print("\n" + "="*80, flush=True)
print("=== RESUMO EXECUTIVO DOS BACKTESTS DE AGOSTO (01 A 20/08) ===", flush=True)
print("="*80, flush=True)

if not df_2x2.empty:
    tot_2x2 = len(df_2x2)
    grn_2x2 = (df_2x2["Resultado"] == "GREEN").sum()
    red_2x2 = (df_2x2["Resultado"] == "RED").sum()
    wr_2x2 = (grn_2x2 / tot_2x2) * 100.0
    pnl_2x2 = df_2x2["PnL_R$"].sum()
    print(f"\n[+] LAY 2X2 QUANT (TETO 20.00):", flush=True)
    print(f"    Total de Operações : {tot_2x2}", flush=True)
    print(f"    Greens             : {grn_2x2} ({wr_2x2:.2f}%)", flush=True)
    print(f"    Reds               : {red_2x2}", flush=True)
    print(f"    Lucro Líquido      : R$ {pnl_2x2:,.2f}", flush=True)

if not df_0x1.empty:
    tot_0x1 = len(df_0x1)
    grn_0x1 = (df_0x1["Resultado"] == "GREEN").sum()
    red_0x1 = (df_0x1["Resultado"] == "RED").sum()
    wr_0x1 = (grn_0x1 / tot_0x1) * 100.0
    pnl_0x1 = df_0x1["PnL_R$"].sum()
    print(f"\n[+] LAY 0X1 (IA TRADER & RF V2):", flush=True)
    print(f"    Total de Operações : {tot_0x1}", flush=True)
    print(f"    Greens             : {grn_0x1} ({wr_0x1:.2f}%)", flush=True)
    print(f"    Reds               : {red_0x1}", flush=True)
    print(f"    Lucro Líquido      : R$ {pnl_0x1:,.2f}", flush=True)

with pd.ExcelWriter("Backtest_Agosto_Lay2x2_e_Lay0x1_Completo.xlsx") as writer:
    if not df_2x2.empty: df_2x2.to_excel(writer, sheet_name="Lay_2x2_Quant", index=False)
    if not df_0x1.empty: df_0x1.to_excel(writer, sheet_name="Lay_0x1_IA", index=False)

print("\n[+] Planilha detalhada salva com sucesso!", flush=True)
