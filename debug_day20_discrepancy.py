import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np

import b365_data_utils
import coleta_lay_cs_aovivo
import lay_draw_rf_v2_strategy as strat_draw

df_bf = b365_data_utils.fetch_betfair_daily('2026-08-20')
hist = coleta_lay_cs_aovivo._hist_df()

print("=== DEBUG DIA 20/08/2026 NA BETFAIR ===", flush=True)
print(f"Total jogos no feed Betfair em 20/08: {len(df_bf) if df_bf is not None else 0}", flush=True)

# 1. Rodar com parâmetros padrão do módulo
res_default = strat_draw.predict_and_evaluate_live(df_bf.to_dict('records'), hist)
aprov_def = [g for g in res_default if g.get("Decision") == "APOSTA"]
print(f"\nAprovados no modulo padrao (PROB_MIN={strat_draw.PROB_MIN}, ODD_MAX={strat_draw.ODD_MAX}, FAV_ODD_MAX={strat_draw.FAV_ODD_MAX}): {len(aprov_def)}")
for g in aprov_def:
    print(f"  -> {g.get('Time')} | {g.get('League')} | {g.get('Home')} x {g.get('Away')} | Odd: {g.get('Odd_D_FT')} | Prob: {g.get('Prob_ML')*100:.1f}%")

# 2. Ver por que os 4 jogos do usuario foram ou nao aprovados
user_matches = ["Drita", "Sint Truiden", "Dinamo Tirana", "LDU"]
print("\n--- STATUS DOS 4 JOGOS DO USUARIO NO FEED DE 20/08 ---", flush=True)
for u in user_matches:
    m = [g for g in res_default if u.lower() in str(g.get("Home")).lower() or u.lower() in str(g.get("Away")).lower()]
    if m:
        for g in m:
            print(f"ENCONTRADO: {g.get('League')} | {g.get('Home')} x {g.get('Away')} | Decision: {g.get('Decision')} | Reason: {g.get('Reason')} | Odd_D: {g.get('Odd_D_FT')} | Odd_H: {g.get('Odd_H_FT')} | Odd_A: {g.get('Odd_A_FT')} | Prob: {g.get('Prob_ML')*100:.1f}%")
    else:
        # Verificar se estava no df_bf
        in_bf = df_bf[df_bf["Home"].str.contains(u, case=False, na=False) | df_bf["Away"].str.contains(u, case=False, na=False)]
        if not in_bf.empty:
            r0 = in_bf.iloc[0]
            print(f"ESTAVA NO FEED MAS NAO AVALIADO: {r0.get('League')} | {r0.get('Home')} x {r0.get('Away')} | Odd_D_Lay: {r0.get('Odd_D_Lay')} | Odd_D_Back: {r0.get('Odd_D_Back')}")
        else:
            print(f"NAO ENCONTRADO NO FEED BETFAIR: {u}")
