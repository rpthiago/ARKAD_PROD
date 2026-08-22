import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np

import b365_data_utils
import coleta_lay_cs_aovivo
import lay_draw_rf_v2_strategy as strat_draw

df_bf = b365_data_utils.fetch_betfair_daily("2026-08-16")
hist = coleta_lay_cs_aovivo._hist_df()

res = strat_draw.predict_and_evaluate_live(df_bf.to_dict('records'), hist)

print(f"Total avaliado pelo script: {len(res)} jogos", flush=True)

# Imprimir todos que deram APOSTA
aprov = [g for g in res if g.get("Decision") == "APOSTA"]
print(f"Total APOSTA: {len(aprov)} jogos\n", flush=True)
for g in aprov:
    print(f"{g.get('Time')} | {g.get('League')} | {g.get('Home')} x {g.get('Away')} | Odd: {g.get('Odd_D_FT')} | Prob: {g.get('Prob_ML')*100:.1f}% | Reason: {g.get('Reason')}")

# Verificar especificamente os 6 jogos do usuario
print("\n--- DETALHES DOS 6 JOGOS DO USUARIO ---", flush=True)
user_teams = ["Arminia", "Brann", "Hafnarfjordur", "Muglaspor", "Vasco", "Colo Colo"]
for u in user_teams:
    matches = [g for g in res if u.lower() in str(g.get("Home")).lower() or u.lower() in str(g.get("Away")).lower()]
    if matches:
        for m in matches:
            print(f"ACHOU: {m.get('Home')} x {m.get('Away')} | Decision: {m.get('Decision')} | Odd: {m.get('Odd_D_FT')} | Prob: {m.get('Prob_ML')*100:.1f}% | Reason: {m.get('Reason')} | liga_draw_rate: {m.get('liga_draw_rate')}")
    else:
        # Verificar se estava no payload da Betfair
        in_bf = df_bf[df_bf["Home"].str.contains(u, case=False, na=False) | df_bf["Away"].str.contains(u, case=False, na=False)]
        if not in_bf.empty:
            row0 = in_bf.iloc[0]
            print(f"ESTAVA NA BETFAIR MAS NAO AVALIADO: {row0.get('Home')} x {row0.get('Away')} | Odd_D_Lay: {row0.get('Odd_D_Lay')} | Odd_D_Back: {row0.get('Odd_D_Back')}")
        else:
            print(f"NAO ENCONTRADO NA GRADE BETFAIR: {u}")
