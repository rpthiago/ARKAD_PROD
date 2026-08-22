import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np

import b365_data_utils
import coleta_lay_cs_aovivo
import lay_draw_rf_v2_strategy as s

df_bf = b365_data_utils.fetch_betfair_daily('2026-08-16')
hist = coleta_lay_cs_aovivo._hist_df()

# 1. Configurar filtro Sniper Sugerido:
# Prob >= 90%, Odd <= 4.20, EV >= 0.05
s.PROB_MIN = 0.90
s.ODD_MAX = 4.20
s.EV_MIN = 0.05

res = s.predict_and_evaluate_live(df_bf.to_dict('records'), hist)
aprovados = [g for g in res if g.get('Decision') == 'APOSTA']

print("=== SUGESTÃO DE GRADE ENXUTA / SNIPER (16/08/2026) ===", flush=True)
print(f"Total de Jogos Aprovados: {len(aprovados)}\n")

df_tab = pd.DataFrame([{
    'Horário': str(g.get('Time'))[:5],
    'Liga': g.get('League'),
    'Mandante': g.get('Home'),
    'Visitante': g.get('Away'),
    'Odd Lay Empate': g.get('Odd_D_FT'),
    'Prob IA': f"{g.get('Prob_ML')*100:.1f}%",
    'EV': f"{g.get('ev_lay'):+.3f}"
} for g in aprovados])

print(df_tab.to_string(index=False), flush=True)
