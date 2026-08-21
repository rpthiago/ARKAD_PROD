import os, sys, pandas as pd, numpy as np

print("=== RUNNING VECTORIZED 2026 MONTHLY BACKTEST ===")

# Base lean ultra rápida
cols = ["Date", "League", "Home", "Away", "Goals_H_FT", "Goals_A_FT", "Odd_CS_0x0_Lay", "Odd_CS_0x0",
        "Odd_CS_0x1_Lay", "Odd_CS_0x1", "Odd_CS_1x0_Lay", "Odd_CS_1x0", "Odd_CS_2x0_Lay", "Odd_CS_2x0",
        "Odd_CS_0x2_Lay", "Odd_CS_0x2", "Odd_CS_2x2_Lay", "Odd_CS_2x2", "Odd_D_FT", "Odd_Under25_FT",
        "Odd_H_FT", "Odd_A_FT", "total_xg", "Total_xG"]

def col_filter(c): return c in cols

df = pd.read_csv("b365_base_lean.csv", usecols=col_filter, low_memory=False)
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
df_2026 = df[df["Date"].dt.year == 2026].dropna(subset=["Date", "Goals_H_FT", "Goals_A_FT"]).sort_values("Date").reset_index(drop=True)
df_2026["Month"] = df_2026["Date"].dt.strftime("%Y-%m")

print(f"[+] Total de jogos no ano de 2026: {len(df_2026)}")
months = sorted(list(df_2026["Month"].unique()))

import metodo_lay2x2_strategy as strat_2x2
import lay_0x0_rf_v2_strategy as s_00
import lay_0x1_rf_v2_strategy as s_01
import lay_1x0_rf_v2_strategy as s_10
import lay_2x0_rf_v2_strategy as s_20
import lay_0x2_rf_v2_strategy as s_02
import lay_draw_rf_v2_strategy as s_draw
import lay_under25_rf_v2_strategy as s_u25
import lay_0x1_agressivo_strategy as s_01_ag

strategies = [
    (s_00, "Lay 0x0 RF v2", "Odd_CS_0x0_Lay", "Odd_CS_0x0"),
    (s_01, "Lay 0x1 RF v2", "Odd_CS_0x1_Lay", "Odd_CS_0x1"),
    (s_10, "Lay 1x0 RF v2", "Odd_CS_1x0_Lay", "Odd_CS_1x0"),
    (s_20, "Lay 2x0 RF v2", "Odd_CS_2x0_Lay", "Odd_CS_2x0"),
    (s_02, "Lay 0x2 RF v2", "Odd_CS_0x2_Lay", "Odd_CS_0x2"),
    (s_draw, "Lay Draw v2", "Odd_Lay_Draw", "Odd_D_FT"),
    (s_u25, "Lay Under 2.5 v2", "Odd_Lay_Under25", "Odd_Under25_FT"),
    (s_01_ag, "Lay 0x1 Agressivo", "Odd_CS_0x1_Lay", "Odd_CS_0x1")
]

all_results = []

# Processar por lote mensal para execucao hiper-rapida
for m in months:
    m_df = df_2026[df_2026["Month"] == m].copy()
    payload_m = m_df.to_dict("records")
    
    # RF v2
    for mod, label, odd_lay_col, odd_back_col in strategies:
        res = mod.predict_and_evaluate_live(payload_m, m_df)
        for g in (res or []):
            if g.get("Decision") == "APOSTA":
                gh = g.get("Goals_H_FT")
                ga = g.get("Goals_A_FT")
                odd_exec = g.get(odd_lay_col) or g.get(odd_back_col) or 0.0
                
                is_red = False
                if label == "Lay 0x0 RF v2" and gh == 0 and ga == 0: is_red = True
                elif "0x1" in label and gh == 0 and ga == 1: is_red = True
                elif label == "Lay 1x0 RF v2" and gh == 1 and ga == 0: is_red = True
                elif label == "Lay 2x0 RF v2" and gh == 2 and ga == 0: is_red = True
                elif label == "Lay 0x2 RF v2" and gh == 0 and ga == 2: is_red = True
                elif label == "Lay Draw v2" and gh == ga: is_red = True
                elif label == "Lay Under 2.5 v2" and (gh + ga) < 2.5: is_red = True

                win = not is_red
                stake_unit = 100.0
                pnl = (stake_unit * 0.95) if win else -((odd_exec - 1.0) * stake_unit)

                all_results.append({
                    "Metodo": label,
                    "Mes": m,
                    "Resultado": "GREEN" if win else "RED",
                    "PnL_R$": pnl
                })

    # Lay 2x2 Quant
    for _, r in m_df.iterrows():
        odd_2x2 = pd.to_numeric(r.get("Odd_CS_2x2_Lay") or r.get("Odd_CS_2x2"), errors="coerce")
        odd_u25 = pd.to_numeric(r.get("Odd_Under25_FT_Back") or r.get("Odd_Under25_FT"), errors="coerce")
        total_xg = pd.to_numeric(r.get("total_xg") or r.get("Total_xG"), errors="coerce")
        odd_h = pd.to_numeric(r.get("Odd_H_FT"), errors="coerce")
        odd_a = pd.to_numeric(r.get("Odd_A_FT"), errors="coerce")

        ok, motivo = strat_2x2.validar_entrada_lay2x2(odd_2x2, odd_u25, total_xg, odd_h, odd_a)
        if ok:
            gh = r.get("Goals_H_FT")
            ga = r.get("Goals_A_FT")
            is_2x2 = (gh == 2 and ga == 2)
            win = not is_2x2
            stake_unit = 100.0
            pnl = (stake_unit * 0.95) if win else -((odd_2x2 - 1.0) * stake_unit)
            all_results.append({
                "Metodo": "Lay 2x2 Quant",
                "Mes": m,
                "Resultado": "GREEN" if win else "RED",
                "PnL_R$": pnl
            })

df_res = pd.DataFrame(all_results)
print(f"[+] Total de apostas computadas: {len(df_res)}")

pivot_pnl = pd.pivot_table(df_res, values="PnL_R$", index="Mes", columns="Metodo", aggfunc="sum", fill_value=0.0)
pivot_count = pd.pivot_table(df_res, values="Resultado", index="Mes", columns="Metodo", aggfunc="count", fill_value=0)

with open("backtest_2026_mensal_relatorio.txt", "w", encoding="utf-8") as f:
    f.write("=== TABELA CONSOLIDADA DE P&L POR MÊS E MÉTODO (2026) ===\n\n")
    f.write(pivot_pnl.to_string())
    f.write("\n\n=== TOTAL DE APOSTAS POR MÊS E MÉTODO ===\n\n")
    f.write(pivot_count.to_string())
    f.write("\n\n=== RESUMO GERAL POR MÉTODO (ANO 2026) ===\n\n")
    
    summary_list = []
    for met in sorted(df_res["Metodo"].unique()):
        sub = df_res[df_res["Metodo"] == met]
        tot_ops = len(sub)
        greens = (sub["Resultado"] == "GREEN").sum()
        reds = (sub["Resultado"] == "RED").sum()
        wr = (greens / tot_ops * 100.0) if tot_ops > 0 else 0.0
        tot_pnl = sub["PnL_R$"].sum()
        summary_list.append({
            "Método": met,
            "Operações": tot_ops,
            "Greens": greens,
            "Reds": reds,
            "Win Rate (%)": f"{wr:.2f}%",
            "P&L Total (R$)": f"R$ {tot_pnl:,.2f}"
        })
    df_sum = pd.DataFrame(summary_list)
    f.write(df_sum.to_string(index=False))

print("✅ Relatório escrito com sucesso!")
