import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np, requests
import coleta_lay_cs_aovivo

print("=== RESOLVENDO PLACARES E P&L DO LAY 0X1 EM AGOSTO (01 A 20/08) ===", flush=True)

# 1. Carregar placares ESPN API para Agosto
db_scores = {}
for day in range(1, 21):
    date_str = f"202608{day:02d}"
    url = f"https://site.api.espn.com/apis/site/v2/sports/soccer/all/scoreboard?dates={date_str}&limit=1000"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            events = r.json().get('events', [])
            for ev in events:
                try:
                    comp = ev.get('competitions', [])[0]
                    competitors = comp.get('competitors', [])
                    h_comp = [c for c in competitors if c.get('homeAway') == 'home'][0]
                    a_comp = [c for c in competitors if c.get('homeAway') == 'away'][0]
                    h_name = h_comp.get('team', {}).get('displayName', '').lower().strip()
                    a_name = a_comp.get('team', {}).get('displayName', '').lower().strip()
                    status = comp.get('status', {}).get('type', {}).get('name', '')
                    if status in ['STATUS_FULL_TIME', 'STATUS_FINAL', 'STATUS_APPROVED', 'FULL_TIME']:
                        gh = int(h_comp.get('score', 0))
                        ga = int(a_comp.get('score', 0))
                        dt_fmt = f"2026-08-{day:02d}"
                        hk = ''.join(c for c in h_name if c.isalnum())
                        ak = ''.join(c for c in a_name if c.isalnum())
                        db_scores[(dt_fmt, hk, ak)] = (gh, ga)
                        db_scores[(dt_fmt, hk[:5], ak[:5])] = (gh, ga)
                except Exception: pass
    except Exception: pass

hist = coleta_lay_cs_aovivo._hist_df()
cfg_0x1 = coleta_lay_cs_aovivo.MERCADOS["0x1"]

ops_0x1 = []
for day in range(1, 21):
    d_str = f"2026-08-{day:02d}"
    sinais = coleta_lay_cs_aovivo.sinais_do_dia(d_str, cfg_0x1)
    if sinais:
        for s in sinais:
            home = str(s.get("Mandante", ""))
            away = str(s.get("Visitante", ""))
            odd_lay = float(pd.to_numeric(s.get("Odd_lay_entrada", 0.0), errors="coerce") or 0.0)
            prob = str(s.get("Prob", ""))
            metodo = str(s.get("Metodo", ""))
            
            gh, ga = None, None
            hk = ''.join(c for c in home.lower() if c.isalnum())
            ak = ''.join(c for c in away.lower() if c.isalnum())
            if (d_str, hk, ak) in db_scores: gh, ga = db_scores[(d_str, hk, ak)]
            elif (d_str, hk[:5], ak[:5]) in db_scores: gh, ga = db_scores[(d_str, hk[:5], ak[:5])]
            
            if gh is not None and ga is not None:
                is_0x1 = (gh == 0 and ga == 1)
                res = "GREEN" if not is_0x1 else "RED"
                pnl = 95.0 if not is_0x1 else -(odd_lay - 1.0) * 100.0
                ops_0x1.append({
                    "Data": d_str, "Confronto": f"{home} x {away}", "Odd_Lay_0x1": odd_lay,
                    "Método": metodo, "Probabilidade": prob,
                    "Placar": f"{gh}x{ga}", "Resultado": res, "PnL_R$": pnl
                })

df_0x1 = pd.DataFrame(ops_0x1)
print("\n" + "="*80, flush=True)
print("=== RESULTADO DO BACKTEST DO LAY 0X1 (IA TRADER & RF V2) EM AGOSTO ===", flush=True)
print("="*80, flush=True)

if not df_0x1.empty:
    tot = len(df_0x1)
    grn = (df_0x1["Resultado"] == "GREEN").sum()
    red = (df_0x1["Resultado"] == "RED").sum()
    wr = (grn / tot) * 100.0
    pnl = df_0x1["PnL_R$"].sum()
    
    print(f"Total de Operações Finalizadas : {tot}", flush=True)
    print(f"Greens                          : {grn} ({wr:.2f}%)", flush=True)
    print(f"Reds                            : {red}", flush=True)
    print(f"Lucro Líquido Acumulado         : R$ {pnl:,.2f}", flush=True)
    print("\n=== TODAS AS OPERAÇÕES DE AGOSTO ===", flush=True)
    print(df_0x1.to_string(index=False), flush=True)
    
    df_0x1.to_excel("Backtest_Lay0x1_Agosto_Final.xlsx", index=False)
