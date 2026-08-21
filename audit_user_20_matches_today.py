import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, requests

matches = [
    ("Kairat Almaty", "Anderlecht", 20.0),
    ("FC Inter Turku", "FC Copenhagen", 19.0),
    ("Mjallby", "Red Bull Salzburg", 17.0),
    ("Vendsyssel FF", "Hillerod Fodbold", 18.5),
    ("FC Nordsjaelland", "St Gallen", 20.0),
    ("Egnatia Rrogozhine", "Lillestrom", 20.0),
    ("Trabzonspor", "Ferencvaros", 19.0),
    ("Klaksvikar Itrottarfelag", "FK Riga", 15.5),
    ("Gornik Zabrze", "Monaco", 19.0),
    ("KF Drita", "Inter Club Escaldes", 17.0),
    ("Crvena Zvezda", "Plzen", 16.0),
    ("OFI", "CSKA Sofia", 19.0),
    ("Sion", "Ajax", 16.0),
    ("Lugano", "Maccabi Tel Aviv", 19.0),
    ("Hearts", "Rapid Vienna", 16.0),
    ("Sheff Wed", "Bradford", 17.0),
    ("Hajduk Split", "Rakow Czestochowa", 19.0),
    ("Real Santander", "Barranquilla", 18.0),
    ("Macara", "Santos", 19.0),
    ("Venados FC", "Dorados", 16.5)
]

print("=== VERIFICANDO PLACARES DOS 20 JOGOS DO USUÁRIO HOJE (20/08/2026) ===", flush=True)

url = "https://site.api.espn.com/apis/site/v2/sports/soccer/all/scoreboard?dates=20260820&limit=1000"
espn_scores = {}
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
                gh = int(h_comp.get('score', 0))
                ga = int(a_comp.get('score', 0))
                hk = ''.join(c for c in h_name if c.isalnum())
                ak = ''.join(c for c in a_name if c.isalnum())
                espn_scores[(hk, ak)] = (gh, ga)
                espn_scores[(hk[:5], ak[:5])] = (gh, ga)
            except Exception: pass
except Exception: pass

results = []
for h, a, odd_lay in matches:
    hk = ''.join(c for c in h.lower() if c.isalnum())
    ak = ''.join(c for c in a.lower() if c.isalnum())
    score = espn_scores.get((hk, ak)) or espn_scores.get((hk[:5], ak[:5]))
    
    if score:
        gh, ga = score
        placar = f"{gh}x{ga}"
        is_2x2 = (gh == 2 and ga == 2)
        res = "RED (2x2)" if is_2x2 else "GREEN"
        pnl = 95.0 if not is_2x2 else -(odd_lay - 1.0) * 100.0
    else:
        placar = "Sem Placar Na API"
        res = "Pendente/Nao Achado"
        pnl = 0.0
        
    results.append({
        "Mandante": h, "Visitante": a, "Odd Lay 2x2": odd_lay,
        "Placar Final": placar, "Resultado": res, "PnL (R$)": pnl
    })

df_res = pd.DataFrame(results)
print(df_res.to_string(index=False), flush=True)

df_fin = df_res[df_res["Resultado"].str.contains("GREEN|RED")].copy()
if not df_fin.empty:
    grn = (df_fin["Resultado"] == "GREEN").sum()
    red = (df_fin["Resultado"].str.contains("RED")).sum()
    tot = len(df_fin)
    pnl = df_fin["PnL (R$)"].sum()
    print(f"\n[+] RESUMO DA GRADE DOS 20 JOGOS DO USUÁRIO HOJE:", flush=True)
    print(f"    Total Jogos Localizados  : {tot} de 20", flush=True)
    print(f"    Greens                   : {grn}", flush=True)
    print(f"    Reds (Placar 2x2)        : {red}", flush=True)
    print(f"    P&L Acumulado (Stake 100): R$ {pnl:,.2f}", flush=True)
