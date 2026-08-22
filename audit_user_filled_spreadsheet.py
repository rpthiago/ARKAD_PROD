import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np

print("=== PROCESSANDO A PLANILHA PREENCHIDA E CORRIGIDA PELO USUÁRIO ===", flush=True)

file_path = "lay0x3/Backtest_Sinais_Agosto_2026_Lay_Draw.xlsx"
df = pd.read_excel(file_path)

print(f"[+] Total de jogos na planilha corrigida: {len(df)} jogos", flush=True)

processed_rows = []
for idx, row in df.iterrows():
    d = str(row.get("Data", ""))[:10]
    t = str(row.get("Horário") or row.get("Horrio") or "")[:5]
    lig = str(row.get("Liga", "")).strip()
    h = str(row.get("Mandante", "")).strip()
    a = str(row.get("Visitante", "")).strip()
    odd = float(row.get("Odd Lay Empate", 0))
    prob = str(row.get("Prob IA", ""))
    ev = float(row.get("EV", 0)) if pd.notna(row.get("EV")) else np.nan
    placar = str(row.get("Placar Real", "")).strip()
    
    if placar and "x" in placar:
        try:
            gh_s, ga_s = placar.lower().replace(" ", "").split("x")
            gh = int(gh_s); ga = int(ga_s)
            is_draw = (gh == ga)
            resultado = "GREEN" if not is_draw else "RED"
            pnl = 95.0 if not is_draw else -(odd - 1.0) * 100.0
        except Exception:
            resultado = "N/D"
            pnl = 0.0
    else:
        resultado = "N/D"
        pnl = 0.0
        
    row_dict = row.to_dict()
    row_dict["Data"] = d
    row_dict["Horário"] = t
    row_dict["Liga"] = lig
    row_dict["Mandante"] = h
    row_dict["Visitante"] = a
    row_dict["Odd Lay Empate"] = odd
    row_dict["Prob IA"] = prob
    row_dict["EV"] = ev
    row_dict["Placar Real"] = placar
    row_dict["Resultado"] = resultado
    row_dict["PnL (R$)"] = pnl
    processed_rows.append(row_dict)

df_calc = pd.DataFrame(processed_rows)

# 1. Resumo Diário
resumo_dias = []
cum_pnl = 0.0
peak_pnl = 0.0
max_dd = 0.0

for d, g_df in df_calc.groupby("Data"):
    tot_d = len(g_df)
    grn_d = (g_df["Resultado"] == "GREEN").sum()
    red_d = (g_df["Resultado"] == "RED").sum()
    wr_d = (grn_d / tot_d) * 100.0 if tot_d > 0 else 0.0
    pnl_d = g_df["PnL (R$)"].sum()
    
    cum_pnl += pnl_d
    if cum_pnl > peak_pnl:
        peak_pnl = cum_pnl
    dd = peak_pnl - cum_pnl
    if dd > max_dd:
        max_dd = dd
        
    resumo_dias.append({
        "Data": d,
        "Total Jogos": tot_d,
        "Greens": grn_d,
        "Reds": red_d,
        "Win Rate %": f"{wr_d:.1f}%",
        "Lucro no Dia R$": pnl_d,
        "Lucro Acumulado R$": cum_pnl
    })

df_resumo = pd.DataFrame(resumo_dias)

# 2. Métricas Consolidadas
tot = len(df_calc)
grn = (df_calc["Resultado"] == "GREEN").sum()
red = (df_calc["Resultado"] == "RED").sum()
wr = (grn / tot) * 100.0 if tot > 0 else 0.0
total_pnl = df_calc["PnL (R$)"].sum()

lucro_bruto = df_calc[df_calc["PnL (R$)"] > 0]["PnL (R$)"].sum()
perda_bruta = abs(df_calc[df_calc["PnL (R$)"] < 0]["PnL (R$)"].sum())
profit_factor = (lucro_bruto / perda_bruta) if perda_bruta > 0 else np.nan

# Salvar planilha completa atualizada
out_files = ["Backtest_Sinais_Agosto_2026_Lay_Draw.xlsx", "lay0x3/Backtest_Sinais_Agosto_2026_Lay_Draw.xlsx"]
for f in out_files:
    with pd.ExcelWriter(f) as writer:
        df_calc.to_excel(writer, sheet_name="Sinais_Agosto_Auditados", index=False)
        df_resumo.to_excel(writer, sheet_name="Resumo_Diario", index=False)

print(f"\n[+] Planilhas salvas com sucesso em: {out_files}", flush=True)

print("\n" + "="*95, flush=True)
print("=== 📊 RELATÓRIO OFICIAL CONSOLIDADO DOS SINAIS DE AGOSTO/2026 (PREENCHIDO) ===", flush=True)
print("="*95, flush=True)
print(f"Total de Entradas: {tot} jogos")
print(f"Greens: {grn} jogos")
print(f"Reds: {red} jogos")
print(f"Taxa de Acerto (Win Rate): {wr:.2f}%")
print(f"Lucro Bruto dos Greens: R$ {lucro_bruto:,.2f}")
print(f"Perda Bruta dos Reds: R$ {perda_bruta:,.2f}")
print(f"LUCRO LÍQUIDO FINAL (Stake R$ 100): R$ {total_pnl:,.2f}")
print(f"Profit Factor: {profit_factor:.2f}")
print(f"Drawdown Máximo: R$ {max_dd:,.2f}")
print("="*95, flush=True)

print("\n=== RESUMO DIA A DIA (01 A 20 DE AGOSTO): ===", flush=True)
print(df_resumo.to_string(index=False), flush=True)
