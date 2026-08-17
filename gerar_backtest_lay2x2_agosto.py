"""
GERADOR DE BACKTEST DO MÉTODO LAY 2X2 - MÊS DE AGOSTO DE 2026
ARKAD_PROD

Este script compila todas as partidas do mês de Agosto (01/08 a 17/08/2026) que atenderam
aos critérios quantitativos da estratégia Lay 2x2, calcula os retornos financeiros
e gera o arquivo de Excel formatado 'Backtest_Lay_2x2_Agosto_2026.xlsx'.
"""

import os
import shutil
import difflib
from pathlib import Path
import pandas as pd
import numpy as np
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

from metodo_lay2x2_strategy import validar_entrada_lay2x2, calcular_resultado_lay2x2

print("==========================================================================")
print("     PROCESSANDO BACKTEST DO MÉTODO LAY 2X2 - MÊS DE AGOSTO DE 2026")
print("==========================================================================\n")

STAKE_UNIDADE = 100.0
RESPONSABILIDADE_FIXA = 200.0
COMISSAO_BETFAIR = 0.05

# 1. Carregar base de dados de Agosto
df_bf = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH.csv", low_memory=False)
df_b365 = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Bet365.csv", low_memory=False)

df_bf["d_str"] = pd.to_datetime(df_bf["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
df_b365["d_str"] = pd.to_datetime(df_b365["Date"], errors="coerce").dt.strftime("%Y-%m-%d")

# Seleciona registros de Agosto de 2026
df_aug_bf = df_bf[(df_bf["d_str"] >= "2026-08-01") & (df_bf["d_str"] <= "2026-08-17")].copy()

# Garante colunas numéricas
df_aug_bf["Odd_CS_2x2_Lay"] = pd.to_numeric(df_aug_bf["Odd_CS_2x2_Lay"], errors="coerce")
df_aug_bf["Odd_Under25"] = pd.to_numeric(df_aug_bf.get("Odd_Under25_FT_Back", df_aug_bf.get("Odd_Under25")), errors="coerce")
df_aug_bf["Odd_H"] = pd.to_numeric(df_aug_bf.get("Odd_H_FT_Back", df_aug_bf.get("Odd_H")), errors="coerce")
df_aug_bf["Odd_A"] = pd.to_numeric(df_aug_bf.get("Odd_A_FT_Back", df_aug_bf.get("Odd_A")), errors="coerce")
df_aug_bf["gh"] = pd.to_numeric(df_aug_bf["Goals_H_FT"], errors="coerce")
df_aug_bf["ga"] = pd.to_numeric(df_aug_bf["Goals_A_FT"], errors="coerce")

rows_out = []
greens_count = 0
reds_count = 0
pnl_stake100_total = 0.0
pnl_liab200_total = 0.0

for idx, r in df_aug_bf.iterrows():
    odd_lay_2x2 = r["Odd_CS_2x2_Lay"]
    odd_u25 = r["Odd_Under25"]
    odd_h = r["Odd_H"]
    odd_a = r["Odd_A"]
    
    # Aplica validação estrita da estratégia Lay 2x2
    ok, motivo = validar_entrada_lay2x2(
        odd_lay_2x2=odd_lay_2x2,
        odd_under25=odd_u25,
        odd_h=odd_h,
        odd_a=odd_a
    )
    
    if ok:
        dt = str(r["d_str"])
        time_str = str(r.get("Time", "15:00"))[:5] if pd.notna(r.get("Time")) else "15:00"
        liga = str(r.get("League", r.get("Div", "Desconhecida")))
        home = str(r.get("Home", r.get("Home_Team", "")))
        away = str(r.get("Away", r.get("Away_Team", "")))
        confronto = f"{home} x {away}"
        
        gh = r["gh"]
        ga = r["ga"]
        
        if pd.notna(gh) and pd.notna(ga):
            gh = int(gh)
            ga = int(ga)
            is_2x2 = (gh == 2 and ga == 2)
            win = not is_2x2
            res_str = "GREEN" if win else "RED"
            
            # P&L com Stake Fixa R$ 100
            pnl_stk = round(STAKE_UNIDADE * (1.0 - COMISSAO_BETFAIR), 2) if win else -round((odd_lay_2x2 - 1.0) * STAKE_UNIDADE, 2)
            
            # P&L com Responsabilidade Fixa R$ 200
            stake_liab = 200.0 / (odd_lay_2x2 - 1.0)
            pnl_liab = round(stake_liab * (1.0 - COMISSAO_BETFAIR), 2) if win else -200.0
            
            if win:
                greens_count += 1
            else:
                reds_count += 1
                
            pnl_stake100_total += pnl_stk
            pnl_liab200_total += pnl_liab
            
            rows_out.append({
                "Data": dt,
                "Horário": time_str,
                "Liga": liga,
                "Confronto": confronto,
                "Método": "Lay 2x2 Quant",
                "Lado": "LAY",
                "Odd Lay 2x2 Betfair": odd_lay_2x2,
                "Gols Mandante": gh,
                "Gols Visitante": ga,
                "Resultado (GREEN/RED)": res_str,
                "Lucro/Prejuízo (Stake R$100)": pnl_stk,
                "Lucro/Prejuízo (Liab R$200)": pnl_liab,
                "Critério de Validação": motivo
            })

df_out = pd.DataFrame(rows_out)

print(f"Total de jogos de Agosto analisados: {len(df_aug_bf)}")
print(f"Total de oportunidades aprovadas no Lay 2x2: {len(df_out)}")
print(f"Greens: {greens_count} | Reds: {reds_count} | Win Rate: {(greens_count/len(df_out)*100 if len(df_out)>0 else 0):.2f}%")
print(f"Lucro Acumulado (Stake Fixa R$ 100): R$ {pnl_stake100_total:,.2f}")
print(f"Lucro Acumulado (Responsabilidade Fixa R$ 200): R$ {pnl_liab200_total:,.2f}\n")

# 2. Gerar Excel formatado
out_file_root = "Backtest_Lay_2x2_Agosto_2026.xlsx"
out_file_lay = "lay0x3/Backtest_Lay_2x2_Agosto_2026.xlsx"

for path in [out_file_root, out_file_lay]:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Backtest_Lay_2x2_Agosto"

    headers = list(df_out.columns)
    ws.append(headers)

    header_fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
    header_font = Font(name="Calibri", size=11, bold=True, color="FFFFFF")

    for col_num, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col_num)
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center", vertical="center")

    green_fill = PatternFill(start_color="D1FAE5", fill_type="solid")
    red_fill = PatternFill(start_color="FEE2E2", fill_type="solid")

    thin_border = Border(
        left=Side(style="thin", color="D9D9D9"),
        right=Side(style="thin", color="D9D9D9"),
        top=Side(style="thin", color="D9D9D9"),
        bottom=Side(style="thin", color="D9D9D9")
    )

    for r_idx, row_data in enumerate(df_out.values, 2):
        ws.append(list(row_data))
        for c_idx in range(1, len(headers) + 1):
            cell = ws.cell(row=r_idx, column=c_idx)
            cell.border = thin_border
            cell.alignment = Alignment(horizontal="center" if c_idx in [1, 2, 6, 7, 8, 9, 10, 11, 12] else "left", vertical="center")
            
            res_val = str(ws.cell(row=r_idx, column=10).value)
            if c_idx in [8, 9, 10, 11, 12]:
                if res_val == "GREEN":
                    cell.fill = green_fill
                elif res_val == "RED":
                    cell.fill = red_fill

    for col in ws.columns:
        max_len = max(len(str(cell.value or "")) for cell in col)
        col_letter = get_column_letter(col[0].column)
        ws.column_dimensions[col_letter].width = max(max_len + 4, 12)

    wb.save(path)
    print(f"Planilha salva com sucesso em: {path}")

print("\nProcessamento do Backtest de Agosto para Lay 2x2 concluído!")
