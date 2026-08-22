import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np

# Lista exata dos 37 jogos informados pelo usuario
user_signals_text = """
2026-08-16	07:15	NETHERLANDS 1	ADO Den Haag	FC Groningen	3.7	85.8%
2026-08-16	07:30	SOUTH KOREA 2	Daegu Fc	Chungnam Asan	4.1	92.5%
2026-08-16	08:30	GERMANY 2	Dynamo Dresden	SV Darmstadt	4.1	97.3%
2026-08-16	09:00	DENMARK 2	Vendsyssel FF	AB	4.1	94.1%
2026-08-16	09:00	DENMARK 1	OB	AC Horsens	3.9	92.2%
2026-08-16	09:30	NORWAY 1	Aalesunds	Valerenga	4.1	92.5%
2026-08-16	09:30	UKRAINE 1	FC Kharkiv	Shakhtar	3.95	93.5%
2026-08-16	09:45	POLAND 1	LKP Motor Lublin	GKS Katowice	3.8	90.1%
2026-08-16	11:00	ESTONIA 1	Trans Narva	Paide Linnameeskond	4.2	100.0%
2026-08-16	11:30	GERMANY 3	Jahn Regensburg	Saarbrucken	4.1	92.3%
2026-08-16	11:30	SWEDEN 1	Kalmar FF	Hammarby	4.4	96.2%
2026-08-16	12:00	BOSNIA 1	FK BSK Banja Luka	Fk Velez Mostar	3.75	91.7%
2026-08-16	12:00	NORWAY 1	Sarpsborg	Sandefjord	4.4	96.0%
2026-08-16	12:00	UKRAINE 1	Dynamo Kiev	Kolos Kovalyovka	4.3	100.0%
2026-08-16	12:30	ROMANIA 1	Otelul Galati	Universitatea Craiova	3.75	88.4%
2026-08-16	12:30	SLOVENIA 1	NK Radomlje	NK Aluminij	3.8	100.0%
2026-08-16	13:00	DENMARK 1	Randers	FC Copenhagen	4.4	95.1%
2026-08-16	13:30	BELGIUM 1	Yellow-Red Mechelen	Standard	3.45	88.6%
2026-08-16	14:00	AUSTRIA 1	Rapid Vienna	Grazer AK	4.4	100.0%
2026-08-16	14:00	BOSNIA 1	Sloga Doboj	Borac Banja Luka	4.2	97.7%
2026-08-16	14:00	PORTUGAL 2	Porto B	Farense	3.45	95.6%
2026-08-16	14:00	SLOVAKIA 1	KFC Komarno	Dunajska Streda	3.55	97.7%
2026-08-16	14:15	BELGIUM 1	Lommel	Charleroi	4.2	92.4%
2026-08-16	14:30	POLAND 2	Unia Skierniewice	Miedz Legnica	3.85	100.0%
2026-08-16	15:00	CZECH 1	Slovan Liberec	Slavia Prague	4.1	90.6%
2026-08-16	15:00	ICELAND 1	IBV	IA Akranes	4.3	100.0%
2026-08-16	15:00	MEXICO 1	Pumas UNAM	Queretaro	3.9	86.6%
2026-08-16	15:15	BULGARIA 1	Lokomotiv Plovdiv	Dunav Ruse	3.8	89.2%
2026-08-16	15:15	POLAND 1	Gornik Zabrze	Wisla Krakow	3.9	89.8%
2026-08-16	16:00	CROATIA 1	HNK Gorica	Hajduk Split	4.3	92.4%
2026-08-16	16:00	ECUADOR 1	Orense Sporting Club	Deportivo Cuenca	3.6	97.7%
2026-08-16	18:15	ECUADOR 1	Emelec	Tecnico Universitario	3.7	88.3%
2026-08-16	18:30	PARAGUAY 1	Olimpia	Club Sportivo Ameliano	3.75	89.9%
2026-08-16	19:30	VENEZUELA 1	Deportivo La Guaira	Zamora FC	3.9	93.5%
2026-08-16	20:00	VENEZUELA 1	Carabobo FC	Metropolitanos	4.4	97.3%
2026-08-16	20:30	BOLIVIA 1	Club Independiente Petr	CD Gualberto Villarroel	4.4	100.0%
2026-08-16	21:00	ECUADOR 1	Aucas	LDU	3.4	100.0%
"""

lines = [l.strip() for l in user_signals_text.strip().split("\n") if l.strip()]
print(f"=== AUDITORIA COMPLETA DOS {len(lines)} SINAIS DO DIA 16/08/2026 ===", flush=True)

# Buscar placares reais na base
df_raw = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Bet365.csv", low_memory=False)
df_raw["d_str"] = pd.to_datetime(df_raw["Date"], errors='coerce').dt.strftime("%Y-%m-%d")

import unicodedata, re
def _canon(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)

df_raw["c_home"] = df_raw["Home"].map(_canon)
df_raw["c_away"] = df_raw["Away"].map(_canon)

results = []
for line in lines:
    parts = line.split("\t")
    if len(parts) >= 7:
        d, t, lig, h, a, odd_s, prob_s = parts[:7]
        odd = float(odd_s)
        
        # Buscar na base se tiver placar
        ch = _canon(h); ca = _canon(a)
        m = df_raw[(df_raw["d_str"] == d) & (df_raw["c_home"] == ch) & (df_raw["c_away"] == ca)]
        if m.empty:
            m = df_raw[(df_raw["c_home"] == ch) & (df_raw["c_away"] == ca)]
            
        placar = "N/D"
        gh = np.nan; ga = np.nan
        if not m.empty:
            r0 = m.iloc[0]
            gh_v = r0.get("Goals_H_FT"); ga_v = r0.get("Goals_A_FT")
            if pd.notna(gh_v) and pd.notna(ga_v):
                gh = int(gh_v); ga = int(ga_v)
                placar = f"{gh}x{ga}"
                
        is_draw = (gh == ga) if pd.notna(gh) else None
        res_str = "GREEN" if is_draw is False else ("RED" if is_draw is True else "Verificando")
        pnl = 95.0 if res_str == "GREEN" else (-(odd - 1.0)*100.0 if res_str == "RED" else 0.0)
        
        results.append({
            "Horário": t,
            "Liga": lig,
            "Mandante": h,
            "Visitante": a,
            "Odd Lay": odd,
            "Prob IA": prob_s,
            "Placar Real": placar,
            "Resultado": res_str,
            "PnL (R$)": f"R$ {pnl:,.2f}" if res_str != "Verificando" else "-"
        })

df_res = pd.DataFrame(results)
print(df_res.to_string(index=False), flush=True)
