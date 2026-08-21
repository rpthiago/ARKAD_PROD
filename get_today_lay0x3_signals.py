import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd
from futpythontrader_client import get_daily_dataframe

date_str = "2026-08-20"
df_day = get_daily_dataframe("betfair", date_str)

print(f"=== SINAIS DO LAY 0X3 VISITANTE HOJE ({date_str}) — {len(df_day)} JOGOS NA BETFAIR ===", flush=True)

sinais = []
reasons = {}

if df_day is not None and not df_day.empty:
    for idx, row in df_day.iterrows():
        odd_h = float(row.get('Odd_H_Back') or row.get('Odd_H_FT_Back') or row.get('Odd_H_FT') or row.get('Odd_H') or 0.0)
        odd_a = float(row.get('Odd_A_Back') or row.get('Odd_A_FT_Back') or row.get('Odd_A_FT') or row.get('Odd_A') or 0.0)
        odd_u25 = float(row.get('Odd_Under25_FT_Back') or row.get('Odd_Under25_FT') or row.get('Odd_Under25') or 0.0)
        odd_0x3 = float(row.get('Odd_CS_0x3_Lay') or row.get('Odd_CS_0x3') or 0.0)
        
        xg_a_val = row.get('A_xGF_r5') or row.get('Media_Gols_Pro_Visitante') or row.get('xG_A_FT') or row.get('xg_a')
        xg_a = float(xg_a_val) if pd.notna(xg_a_val) else 1.0
        
        home = str(row.get("Home", row.get("Home_Team", "")))
        away = str(row.get("Away", row.get("Away_Team", "")))
        liga = str(row.get("League", row.get("Div", "Liga Externa")))
        horario = str(row.get("Time", row.get("horario", "15:00")))[:5]
        
        cond_u25 = (0.0 < odd_u25 <= 2.10)
        cond_0x3 = (14.0 <= odd_0x3 <= 35.0)
        cond_away_safe = (odd_a >= 1.85 or odd_a == 0.0)
        cond_xg = (xg_a <= 1.10)
        
        if cond_u25 and cond_0x3 and cond_away_safe and cond_xg:
            sinais.append({
                "Horário": horario,
                "Liga": liga,
                "Mandante": home,
                "Visitante": away,
                "Odd Mandante": odd_h,
                "Odd Visitante": odd_a,
                "Odd Under 2.5": odd_u25,
                "Odd Lay 0x3": odd_0x3
            })
        else:
            if not cond_away_safe:
                rsn = f"Visitante Super Favorito (Odd A: {odd_a:.2f} < 1.85)"
            elif not cond_u25:
                rsn = f"Sem Tendência Under 2.5 (Odd U25: {odd_u25:.2f} > 2.10)"
            elif not cond_0x3:
                rsn = f"Odd Lay 0x3 Fora da Faixa 14.0-35.0 ({odd_0x3:.2f})"
            else:
                rsn = f"xG Visitante Alto ({xg_a:.2f} > 1.10)"
            reasons[rsn] = reasons.get(rsn, 0) + 1

df_sinais = pd.DataFrame(sinais)

if df_sinais.empty:
    print("\n[!] Nenhum jogo aprovado no Lay 0x3 hoje.", flush=True)
else:
    print(f"\n[+] TOTAL DE JOGOS APROVADOS HOJE: {len(df_sinais)}", flush=True)
    print(df_sinais.to_string(index=False), flush=True)

print(f"\n[-] RESUMO DOS MOTIVOS DE REJEIÇÃO HOJE ({len(df_day)} JOGOS):", flush=True)
for rsn, count in sorted(reasons.items(), key=lambda x: x[1], reverse=True):
    print(f"   - {rsn}: {count} jogos", flush=True)
