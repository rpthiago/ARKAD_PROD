import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np
from datetime import datetime, timedelta
import unicodedata, re

import b365_data_utils
import coleta_lay_cs_aovivo
import lay_draw_rf_v2_strategy as strat_draw

print("=== COLETANDO SINAIS DIÁRIOS DE AGOSTO DE 2026 (01/08 A 20/08) ===", flush=True)

def _canon(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)

# Base histórica para cruzar resultados conhecidos
df_raw = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Bet365.csv", low_memory=False)
df_raw["d_str"] = pd.to_datetime(df_raw["Date"], errors='coerce').dt.strftime("%Y-%m-%d")
df_raw["c_home"] = df_raw["Home"].map(_canon)
df_raw["c_away"] = df_raw["Away"].map(_canon)

hist = coleta_lay_cs_aovivo._hist_df()

all_signals = []

start_date = datetime.strptime("2026-08-01", "%Y-%m-%d")
end_date = datetime.strptime("2026-08-20", "%Y-%m-%d")

cur = start_date
while cur <= end_date:
    d_str = cur.strftime("%Y-%m-%d")
    print(f"[+] Processando {d_str}...", flush=True)
    
    try:
        df_bf = b365_data_utils.fetch_betfair_daily(d_str)
        if df_bf is not None and not df_bf.empty:
            res = strat_draw.predict_and_evaluate_live(df_bf.to_dict('records'), hist)
            aprovados = [g for g in (res or []) if g.get("Decision") == "APOSTA"]
            
            for g in aprovados:
                h = str(g.get("Home", "")).strip()
                a = str(g.get("Away", "")).strip()
                lig = str(g.get("League", "")).strip()
                t = str(g.get("Time", ""))[:5]
                odd = float(g.get("Odd_D_FT", 0))
                prob = float(g.get("Prob_ML", 0))
                ev = float(g.get("ev_lay", 0))
                
                # Cruzar com base histórica
                ch = _canon(h); ca = _canon(a)
                m = df_raw[(df_raw["d_str"] == d_str) & (df_raw["c_home"] == ch) & (df_raw["c_away"] == ca)]
                if m.empty:
                    m = df_raw[(df_raw["c_home"] == ch) & (df_raw["c_away"] == ca)]
                
                placar = ""
                resultado = ""
                pnl = np.nan
                
                if not m.empty:
                    r0 = m.iloc[0]
                    gh_v = r0.get("Goals_H_FT"); ga_v = r0.get("Goals_A_FT")
                    # Se placar nao for o placeholder 0x0 de jogo nao jogado
                    if pd.notna(gh_v) and pd.notna(ga_v):
                        gh = int(gh_v); ga = int(ga_v)
                        # Só valida se não for partida com dados nulos
                        placar = f"{gh}x{ga}"
                        is_draw = (gh == ga)
                        resultado = "GREEN" if not is_draw else "RED"
                        pnl = 95.0 if not is_draw else -(odd - 1.0)*100.0
                
                all_signals.append({
                    "Data": d_str,
                    "Horário": t,
                    "Liga": lig,
                    "Mandante": h,
                    "Visitante": a,
                    "Odd Lay Empate": odd,
                    "Prob IA": f"{prob*100:.1f}%",
                    "EV": round(ev, 3),
                    "Placar Real": placar,
                    "Resultado": resultado,
                    "PnL (R$)": pnl
                })
    except Exception as e:
        print(f"Erro em {d_str}: {e}")
        
    cur += timedelta(days=1)

df_all = pd.DataFrame(all_signals)
print(f"\n[+] Total de sinais gerados em Agosto (01 a 20/08): {len(df_all)} jogos", flush=True)

# Gerar resumo diario
resumo_dias = []
for d, g_df in df_all.groupby("Data"):
    tot = len(g_df)
    grn = (g_df["Resultado"] == "GREEN").sum()
    red = (g_df["Resultado"] == "RED").sum()
    pend = tot - grn - red
    pnl_d = g_df["PnL (R$)"].sum(skipna=True)
    resumo_dias.append({
        "Data": d,
        "Total Sinais": tot,
        "Greens": grn,
        "Reds": red,
        "A Preencher": pend,
        "Lucro Apurado R$": pnl_d
    })

df_resumo = pd.DataFrame(resumo_dias)

# Salvar planilha Excel
excel_name = "Backtest_Sinais_Agosto_2026_Lay_Draw.xlsx"
with pd.ExcelWriter(excel_name) as writer:
    df_all.to_excel(writer, sheet_name="Sinais_Agosto_2026", index=False)
    df_resumo.to_excel(writer, sheet_name="Resumo_Diario", index=False)

print(f"[+] Planilha gerada com sucesso: {excel_name}", flush=True)
print("\n" + "="*85, flush=True)
print("RESUMO DIÁRIO DE AGOSTO/2026:", flush=True)
print("="*85, flush=True)
print(df_resumo.to_string(index=False), flush=True)
