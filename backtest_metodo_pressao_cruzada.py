# -*- coding: utf-8 -*-
"""
backtest_metodo_pressao_cruzada.py — Backtest Forense do Método Pressão Cruzada
ARKAD_PROD

Valida os 2 perfis da estratégia sobre a base real da Betfair Exchange:
- Base: Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH.csv (50.964 jogos)
- Odds de Lay reais: Odd_D_Lay e Odd_H_Lay
- Comissão real: 5% (comissao Betfair)
- Cálculo estrito de Break-even WR e ROI sobre Liability (Risco) e Stake
"""

import os
import sys
import numpy as np
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from metodo_pressao_cruzada_strategy import avaliar_jogo_pressao_cruzada

BASE_CSV = ROOT / "Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH.csv"
COMMISSION = 0.05


def rodar_backtest():
    print("=" * 80)
    print("BACKTEST FORENSE — MÉTODO PRESSÃO CRUZADA (CROSS-MARKET INDEX)")
    print("=" * 80)
    
    if not BASE_CSV.exists():
        print(f"[ERRO] Base não encontrada: {BASE_CSV}")
        return
        
    print(f"[+] Lendo base histórica da Betfair Exchange: {BASE_CSV.name}...")
    cols = [
        "Date", "League", "Home", "Away",
        "Odd_H_Back", "Odd_H_Lay", "Odd_D_Back", "Odd_D_Lay", "Odd_A_Back", "Odd_A_Lay",
        "Odd_Over25_FT_Back", "Odd_BTTS_Yes_Back",
        "Goals_H_FT", "Goals_A_FT"
    ]
    df = pd.read_csv(BASE_CSV, usecols=lambda c: c in cols, low_memory=False)
    for c in cols[4:]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
            
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date", "Goals_H_FT", "Goals_A_FT"]).sort_values("Date").reset_index(drop=True)
    print(f"[i] Total de jogos avaliados: {len(df):,}")
    
    operacoes = []
    
    for idx, row in df.iterrows():
        r_dict = row.to_dict()
        aprovado, motivo, metricas = avaliar_jogo_pressao_cruzada(r_dict)
        if not aprovado:
            continue
            
        gh = int(row["Goals_H_FT"])
        ga = int(row["Goals_A_FT"])
        perfil = metricas["perfil"]
        odd_lay = metricas["odd_entrada"]
        
        # Liquidação honesta
        if perfil == "LAY_DRAW_ALTA_PRESSAO":
            is_green = (gh != ga)
        elif perfil == "LAY_HOME_FALSO_FAVORITO":
            is_green = (ga >= gh)
        else:
            continue
            
        # P&L com comissão de 5%
        pnl_stake = (1.0 - COMMISSION) if is_green else -(odd_lay - 1.0)
        liability = (odd_lay - 1.0)
        pnl_liab_pct = (pnl_stake / liability) * 100.0 if liability > 0 else 0.0
        
        operacoes.append({
            "Data": row["Date"].strftime("%Y-%m-%d"),
            "Mes": row["Date"].strftime("%Y-%m"),
            "Liga": row.get("League", ""),
            "Home": row["Home"],
            "Away": row["Away"],
            "Perfil": perfil,
            "Odd_Lay": round(odd_lay, 2),
            "Odd_Fav": round(metricas["odd_fav"], 2),
            "K_pressao": round(metricas["k_pressao"], 2),
            "K_ratio": round(metricas["k_ratio"], 2) if pd.notna(metricas.get("k_ratio")) else np.nan,
            "Placar": f"{gh}x{ga}",
            "Resultado": "GREEN" if is_green else "RED",
            "PnL_u": round(pnl_stake, 4),
            "Liability": round(liability, 2),
            "ROI_Liab_Pct": round(pnl_liab_pct, 2)
        })
        
    df_ops = pd.DataFrame(operacoes)
    
    if df_ops.empty:
        print("[!] Nenhuma operação atendeu aos critérios.")
        return
        
    print(f"\n[+] Total de Entradas Identificadas: {len(df_ops):,}")
    
    for perfil, g_perf in df_ops.groupby("Perfil"):
        print("\n" + "=" * 80)
        print(f"RELATÓRIO DE PERFORMANCE — {perfil}")
        print("=" * 80)
        
        n = len(g_perf)
        gr = (g_perf["Resultado"] == "GREEN").sum()
        rd = n - gr
        wr = (gr / n) * 100.0
        
        odd_med = g_perf["Odd_Lay"].median()
        be_wr = (odd_med - 1.0) / (odd_med - COMMISSION) * 100.0
        margem = wr - be_wr
        
        tot_pnl_u = g_perf["PnL_u"].sum()
        tot_liab = g_perf["Liability"].sum()
        roi_liab = (tot_pnl_u / tot_liab) * 100.0 if tot_liab > 0 else 0.0
        roi_stake = (tot_pnl_u / n) * 100.0
        
        print(f"Amostra (N)               : {n} apostas")
        print(f"Greens / Reds             : {gr} Greens / {rd} Reds")
        print(f"Win Rate Real             : {wr:.2f}%")
        print(f"Odd Mediana de Lay        : @ {odd_med:.2f}")
        print(f"Break-Even WR Exigido     : {be_wr:.2f}%")
        print(f"Margem Matemática (WR-BE) : {margem:+.2f} pp")
        print(f"P&L Líquido Acumulado     : {tot_pnl_u:+.2f} unidades (R$ {tot_pnl_u*100:+.0f})")
        print(f"ROI sobre Liability       : {roi_liab:+.2f}%")
        print(f"ROI sobre Stake (1u)      : {roi_stake:+.2f}%")
        
        # Desempenho Mensal
        print("\nDesempenho por Mês:")
        mensal = []
        for mes, gm in g_perf.groupby("Mes"):
            nm = len(gm)
            grm = (gm["Resultado"] == "GREEN").sum()
            wrm = (grm / nm) * 100.0
            pnlm = gm["PnL_u"].sum()
            roim = (pnlm / gm["Liability"].sum()) * 100.0 if gm["Liability"].sum() > 0 else 0.0
            mensal.append({
                "Mês": mes, "Apostas": nm, "Greens": grm, "Reds": nm - grm,
                "WR %": round(wrm, 1), "P&L (u)": round(pnlm, 2), "ROI Liab %": round(roim, 1)
            })
            print(f"  {mes} | N={nm:3d} | WR={wrm:5.1f}% | P&L={pnlm:+6.2f}u | ROI Liab={roim:+5.1f}%")
            
        df_mensal = pd.DataFrame(mensal)
        meses_pos = (df_mensal["P&L (u)"] > 0).sum()
        print(f"\nMeses no Verde: {meses_pos}/{len(df_mensal)} ({meses_pos/len(df_mensal)*100:.1f}%)")

    # Salva planilha analítica
    out_xlsx = ROOT / "Backtest_Metodo_Pressao_Cruzada_2024_2026.xlsx"
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as w:
        df_ops.to_excel(w, sheet_name="Operacoes_Detalhadas", index=False)
    print(f"\n[+] Planilha consolidada gerada: {out_xlsx.name}")


if __name__ == "__main__":
    rodar_backtest()
