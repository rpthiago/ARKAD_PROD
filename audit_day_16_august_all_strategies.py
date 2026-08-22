import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np

import b365_data_utils
import coleta_lay_cs_aovivo
from metodo_lay2x2_strategy import validar_entrada_lay2x2
import lay_1x0_rf_v2_strategy as strat_1x0
import lay_draw_rf_v2_strategy as strat_draw
import lay_2x0_rf_v2_strategy as strat_2x0
import lay_0x2_rf_v2_strategy as strat_0x2
import lay_0x0_rf_v2_strategy as strat_0x0

target_date = "2026-08-16"
print(f"=== AUDITORIA GERAL DO DIA {target_date} (TODOS OS MÉTODOS) ===", flush=True)

df_bf = b365_data_utils.fetch_betfair_daily(target_date)
print(f"[+] Total de jogos no feed Betfair em {target_date}: {len(df_bf) if df_bf is not None else 0} jogos\n", flush=True)

if df_bf is None or df_bf.empty:
    print("Erro: Nenhum jogo encontrado para esta data.")
    sys.exit(0)

payload = df_bf.to_dict("records")
hist = coleta_lay_cs_aovivo._hist_df()

# 1. LAY 1X0 RF V2
print("="*85, flush=True)
print("1. SINAIS LAY 1X0 RF V2 (16/08/2026)", flush=True)
print("="*85, flush=True)
res_1x0 = strat_1x0.predict_and_evaluate_live(payload, hist)
aprov_1x0 = [g for g in (res_1x0 or []) if g.get("Decision") == "APOSTA"]
if aprov_1x0:
    df_out_1x0 = pd.DataFrame([{
        "Horário": str(g.get("Time", ""))[:5],
        "Liga": g.get("League", ""),
        "Mandante": g.get("Home", ""),
        "Visitante": g.get("Away", ""),
        "Odd Lay 1x0": g.get("Odd_CS_1x0_Lay") or g.get("Odd_CS_1x0"),
        "Prob IA": f"{g.get('Prob_ML', 0)*100:.1f}%"
    } for g in aprov_1x0])
    print(df_out_1x0.to_string(index=False))
else:
    print("Nenhum jogo passou nos filtros do Lay 1x0.")

# 2. LAY DRAW (LAY EMPATE) RF V2
print("\n" + "="*85, flush=True)
print("2. SINAIS LAY DRAW / LAY EMPATE RF V2 (16/08/2026)", flush=True)
print("="*85, flush=True)
res_draw = strat_draw.predict_and_evaluate_live(payload, hist)
aprov_draw = [g for g in (res_draw or []) if g.get("Decision") == "APOSTA"]
if aprov_draw:
    df_out_draw = pd.DataFrame([{
        "Horário": str(g.get("Time", ""))[:5],
        "Liga": g.get("League", ""),
        "Mandante": g.get("Home", ""),
        "Visitante": g.get("Away", ""),
        "Odd Lay Empate": g.get("Odd_D_FT"),
        "Prob IA (Não-Empate)": f"{g.get('Prob_ML', 0)*100:.1f}%"
    } for g in aprov_draw])
    print(df_out_draw.to_string(index=False))
else:
    print("Nenhum jogo passou nos filtros do Lay Draw.")

# 3. LAY 2X2 QUANT
print("\n" + "="*85, flush=True)
print("3. SINAIS LAY 2X2 QUANT (16/08/2026)", flush=True)
print("="*85, flush=True)
lay2x2_list = []
for idx, r in df_bf.iterrows():
    h = str(r.get("Home", ""))
    a = str(r.get("Away", ""))
    liga = str(r.get("League", ""))
    
    odd_2x2 = pd.to_numeric(r.get("Odd_CS_2x2_Lay"), errors='coerce')
    odd_u25 = pd.to_numeric(r.get("Odd_Under25_FT_Back"), errors='coerce')
    odd_h = pd.to_numeric(r.get("Odd_H_Back"), errors='coerce')
    odd_a = pd.to_numeric(r.get("Odd_A_Back"), errors='coerce')
    
    o_2x2 = float(odd_2x2) if pd.notna(odd_2x2) else 0.0
    o_u25 = float(odd_u25) if pd.notna(odd_u25) else None
    o_h = float(odd_h) if pd.notna(odd_h) else None
    o_a = float(odd_a) if pd.notna(odd_a) else None
    
    ok, _ = validar_entrada_lay2x2(odd_lay_2x2=o_2x2, odd_under25=o_u25, odd_h=o_h, odd_a=o_a)
    if ok:
        lay2x2_list.append({
            "Horário": str(r.get("Time", ""))[:5],
            "Liga": liga,
            "Mandante": h,
            "Visitante": a,
            "Odd Lay 2x2": o_2x2,
            "Odd Under 2.5 FT": o_u25
        })

df_out_2x2 = pd.DataFrame(lay2x2_list)
if not df_out_2x2.empty:
    print(df_out_2x2.to_string(index=False))
else:
    print("Nenhum jogo passou nos critérios de Lay 2x2.")

# 4. LAY 2X0 RF V2
print("\n" + "="*85, flush=True)
print("4. SINAIS LAY 2X0 RF V2 (16/08/2026)", flush=True)
print("="*85, flush=True)
res_2x0 = strat_2x0.predict_and_evaluate_live(payload, hist)
aprov_2x0 = [g for g in (res_2x0 or []) if g.get("Decision") == "APOSTA"]
if aprov_2x0:
    df_out_2x0 = pd.DataFrame([{
        "Horário": str(g.get("Time", ""))[:5],
        "Liga": g.get("League", ""),
        "Mandante": g.get("Home", ""),
        "Visitante": g.get("Away", ""),
        "Odd Lay 2x0": g.get("Odd_CS_2x0_Lay") or g.get("Odd_CS_2x0"),
        "Prob IA": f"{g.get('Prob_ML', 0)*100:.1f}%"
    } for g in aprov_2x0])
    print(df_out_2x0.to_string(index=False))
else:
    print("Nenhum jogo passou nos filtros do Lay 2x0.")

# 5. LAY 0X2 RF V2
print("\n" + "="*85, flush=True)
print("5. SINAIS LAY 0X2 RF V2 (16/08/2026)", flush=True)
print("="*85, flush=True)
res_0x2 = strat_0x2.predict_and_evaluate_live(payload, hist)
aprov_0x2 = [g for g in (res_0x2 or []) if g.get("Decision") == "APOSTA"]
if aprov_0x2:
    df_out_0x2 = pd.DataFrame([{
        "Horário": str(g.get("Time", ""))[:5],
        "Liga": g.get("League", ""),
        "Mandante": g.get("Home", ""),
        "Visitante": g.get("Away", ""),
        "Odd Lay 0x2": g.get("Odd_CS_0x2_Lay") or g.get("Odd_CS_0x2"),
        "Prob IA": f"{g.get('Prob_ML', 0)*100:.1f}%"
    } for g in aprov_0x2])
    print(df_out_0x2.to_string(index=False))
else:
    print("Nenhum jogo passou nos filtros do Lay 0x2.")

# 6. LAY 0X0 RF V2
print("\n" + "="*85, flush=True)
print("6. SINAIS LAY 0X0 RF V2 (16/08/2026)", flush=True)
print("="*85, flush=True)
res_0x0 = strat_0x0.predict_and_evaluate_live(payload, hist)
aprov_0x0 = [g for g in (res_0x0 or []) if g.get("Decision") == "APOSTA"]
if aprov_0x0:
    df_out_0x0 = pd.DataFrame([{
        "Horário": str(g.get("Time", ""))[:5],
        "Liga": g.get("League", ""),
        "Mandante": g.get("Home", ""),
        "Visitante": g.get("Away", ""),
        "Odd Lay 0x0": g.get("Odd_CS_0x0_Lay") or g.get("Odd_CS_0x0"),
        "Prob IA": f"{g.get('Prob_ML', 0)*100:.1f}%"
    } for g in aprov_0x0])
    print(df_out_0x0.to_string(index=False))
else:
    print("Nenhum jogo passou nos filtros do Lay 0x0.")
