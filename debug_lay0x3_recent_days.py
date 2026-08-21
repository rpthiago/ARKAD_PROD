import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np
from futpythontrader_client import get_daily_dataframe

print("=== DIAGNÓSTICO DO LAY 0X3 VISITANTE NOS ÚLTIMOS DIAS (17 A 20/08/2026) ===", flush=True)

for date_str in ["2026-08-17", "2026-08-18", "2026-08-19", "2026-08-20"]:
    try:
        df_day = get_daily_dataframe("betfair", date_str)
        if df_day is None or df_day.empty:
            print(f"\n[{date_str}] Sem jogos no feed Betfair.", flush=True)
            continue
            
        print(f"\n" + "="*70, flush=True)
        print(f"=== ANÁLISE DE {date_str} ({len(df_day)} JOGOS) ===", flush=True)
        print("="*70, flush=True)
        
        aprovados = []
        reasons = {}
        
        for idx, r in df_day.iterrows():
            odd_h = float(r.get('Odd_H_FT_Back', 0.0) or r.get('Odd_H_FT', 0.0) or r.get('Odd_H', 0.0) or 0.0)
            odd_a = float(r.get('Odd_A_FT_Back', 0.0) or r.get('Odd_A_FT', 0.0) or r.get('Odd_A', 0.0) or 0.0)
            odd_u25 = float(r.get('Odd_Under25_FT_Back', 0.0) or r.get('Odd_Under25_FT', 0.0) or r.get('Odd_Under25', 0.0) or 0.0)
            odd_0x3 = float(r.get('Odd_CS_0x3_Lay', 0.0) or r.get('Odd_CS_0x3', 0.0) or 0.0)
            xg_a = float(r.get('A_xGF_r5', 0.0) or r.get('Media_Gols_Pro_Visitante', 0.0) or r.get('xG_A_FT', 1.0) or 1.0)
            
            home = str(r.get("Home", r.get("Home_Team", "")))
            away = str(r.get("Away", r.get("Away_Team", "")))
            
            # Validações estritas de Lay 0x3
            if odd_h <= 0.0 or odd_h > 2.20:
                key = f"Mandante Não Favorito Claro (Odd H > 2.20)"
            elif odd_h >= odd_a:
                key = "Visitante Favorito"
            elif odd_u25 <= 0.0 or odd_u25 > 2.10:
                key = f"Jogo com Tendência Over 2.5 (Odd U25 > 2.10)"
            elif odd_0x3 < 14.0 or odd_0x3 > 35.0:
                key = f"Odd Lay 0x3 Fora da Faixa 14.0-35.0"
            elif xg_a > 1.10:
                key = f"Ataque Visitante Perigoso (xG Visitante > 1.10)"
            else:
                key = "APROVADO"
                
            if key == "APROVADO":
                aprovados.append(f"{home} x {away} (Odd Lay 0x3: {odd_0x3:.2f})")
            else:
                reasons[key] = reasons.get(key, 0) + 1
                
        print(f"[+] SINAIS APROVADOS EM {date_str}: {len(aprovados)} JOGOS", flush=True)
        for a in aprovados:
            print(f"   * {a}", flush=True)
            
        print(f"[-] Breakdown de motivos de rejeição ({len(df_day)} jogos):", flush=True)
        for rsn, cnt in sorted(reasons.items(), key=lambda x: x[1], reverse=True):
            print(f"   - {rsn}: {cnt} jogos", flush=True)
    except Exception as e:
        print(f"Erro analisando {date_str}: {e}", flush=True)
