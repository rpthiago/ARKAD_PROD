import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np

import b365_data_utils
import coleta_lay_cs_aovivo
import lay_draw_rf_v2_strategy as strat_draw

print("=== AUDITORIA EXCLUSIVA E DEFINITIVA DO LAY DRAW PARA 16/08/2026 ===", flush=True)

target_date = "2026-08-16"

# 1. Carregar payload da Betfair
df_bf = b365_data_utils.fetch_betfair_daily(target_date)
hist = coleta_lay_cs_aovivo._hist_df()

# 2. Rodar a predicao oficial do Lay Draw
res = strat_draw.predict_and_evaluate_live(df_bf.to_dict('records'), hist)

# 3. Filtrar aprovados
aprovados = [g for g in res if g.get("Decision") == "APOSTA"]

print(f"[+] Total de jogos no feed Betfair em {target_date}: {len(df_bf)} jogos")
print(f"[+] Total de jogos APROVADOS no Lay Draw em {target_date}: {len(aprovados)} jogos\n", flush=True)

# Resultados reais conhecidos e verificados das partidas
real_scores = {
    "Arminia Bielefeld x Cottbus": "3x0",
    "Brann x Ham-Kam": "3x0",
    "Hafnarfjordur x Vikingur Reykjavik": "2x4",
    "Muglaspor x Bandirmaspor": "0x0",
    "Vasco da Gama x Santos": "0x3",
    "Colo Colo x OHiggins": "2x2",
    "Hansa Rostock x Waldhof Mannheim": "1x1",
    "Kayserispor x Sivasspor": "1x2",
    "Keciorengucu x Pendikspor": "1x1",
    "Brommapojkarna x Orgryte": "2x1",
    "Racing Santander x Villarreal": "0x2",
    "Nublense x Union La Calera": "1x1",
    "Varazdin x Rijeka": "0x0",
    "Sloga Doboj x Borac Banja Luka": "0x2",
    "River Plate x Argentinos Juniors": "1x1",
    "Barracas Central x Rosario Central": "0x0",
    "Central Cordoba (SdE) x Instituto": "1x1",
    "Deportes Iquique x Antofagasta": "2x1"
}

table_rows = []
for g in aprovados:
    h = str(g.get("Home", "")).strip()
    a = str(g.get("Away", "")).strip()
    match_key = f"{h} x {a}"
    
    # Buscar score
    placar = real_scores.get(match_key, "N/D")
    odd_lay = float(g.get("Odd_D_FT", 0))
    prob_ia = float(g.get("Prob_ML", 0)) * 100
    
    if placar != "N/D":
        gh, ga = map(int, placar.split("x"))
        is_draw = (gh == ga)
        resultado = "RED" if is_draw else "GREEN"
        pnl = 95.0 if not is_draw else -(odd_lay - 1.0) * 100.0
    else:
        resultado = "Pendente"
        pnl = 0.0
        
    table_rows.append({
        "Horário": str(g.get("Time", ""))[:5],
        "Liga": g.get("League", ""),
        "Mandante": h,
        "Visitante": a,
        "Odd Lay Empate": odd_lay,
        "Prob IA (Não-Empate)": f"{prob_ia:.1f}%",
        "Placar Real": placar,
        "Resultado": resultado,
        "PnL (R$)": f"R$ {pnl:,.2f}" if placar != "N/D" else "-"
    })

df_table = pd.DataFrame(table_rows)
print(df_table.to_string(index=False), flush=True)

if not df_table.empty and (df_table["Resultado"] != "Pendente").any():
    finalizados = df_table[df_table["Resultado"].isin(["GREEN", "RED"])]
    tot_f = len(finalizados)
    grn_f = (finalizados["Resultado"] == "GREEN").sum()
    red_f = (finalizados["Resultado"] == "RED").sum()
    wr_f = (grn_f / tot_f) * 100 if tot_f > 0 else 0
    pnl_tot = sum([float(x.replace("R$", "").replace(",", "").strip()) for x in finalizados["PnL (R$)"]])
    
    print("\n" + "="*85, flush=True)
    print(f"📊 RESUMO DO DIA 16/08/2026 NO LAY DRAW:", flush=True)
    print(f"Total de Entradas: {tot_f} | Greens: {grn_f} | Reds: {red_f} | Win Rate: {wr_f:.2f}% | P&L: R$ {pnl_tot:,.2f}", flush=True)
    print("="*85, flush=True)
