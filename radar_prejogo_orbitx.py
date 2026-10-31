# -*- coding: utf-8 -*-
"""
radar_prejogo_orbitx.py — Radar de Candidatos Pré-Jogo (Football-Data -> Verificação OrbitX)

Objetivo:
Gerar a grade de jogos candidatos para a Tríade Oficial e métodos do ARKAD usando
fontes internacionais abertas e resilientes (football-data.co.uk fixtures), permitindo
ao operador conferir a odd de Lay na OrbitX (ou BFB247) antes de confirmar a aposta.

Tetos Homologados para Entrada na OrbitX:
- Lay Draw Super Fav: Lay Draw <= 7.00 | Spread <= 1.15
- Lay Home Fav Visitante: Lay Home <= 8.00 | Spread <= 1.15
- Lay Over 4.5 FT (Under Pesado): Lay Over 4.5 <= 20.00 | Spread <= 1.25
- Lay Away Fortaleza 1X: Lay Away <= 15.00 | Spread <= 1.20
"""

import io
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
import pandas as pd
import requests

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

ROOT = Path(__file__).resolve().parent
CACHE_FILE = ROOT / "metodos_aprovados" / ".cache_fixtures_football_data.csv"

DIV_NAMES = {
    "E0": "Premier League (Inglaterra)",
    "E1": "Championship (Inglaterra)",
    "E2": "League 1 (Inglaterra)",
    "E3": "League 2 (Inglaterra)",
    "EC": "National League (Inglaterra)",
    "SC0": "Premiership (Escócia)",
    "SC1": "Championship (Escócia)",
    "SC2": "League 1 (Escócia)",
    "SC3": "League 2 (Escócia)",
    "D1": "Bundesliga (Alemanha)",
    "D2": "2. Bundesliga (Alemanha)",
    "I1": "Serie A (Itália)",
    "I2": "Serie B (Itália)",
    "SP1": "La Liga (Espanha)",
    "SP2": "Segunda Division (Espanha)",
    "F1": "Ligue 1 (França)",
    "F2": "Ligue 2 (França)",
    "N1": "Eredivisie (Holanda)",
    "B1": "Jupiler Pro League (Bélgica)",
    "P1": "Primeira Liga (Portugal)",
    "T1": "Super Lig (Turquia)",
    "G1": "Super League (Grécia)",
    "BRA": "Brasileirão Série A",
    "ARG": "Liga Profesional (Argentina)",
    "USA": "MLS (Estados Unidos)",
    "MEX": "Liga MX (México)",
}

RE_COPA = re.compile(r"cup|taça|copa|pokal|coppa|trophy|shield|fa cup|league cup", re.IGNORECASE)

def _utc_para_brt(tm_str: str) -> str:
    """Converte horário UTC (GMT) de football-data para Horário de Brasília (UTC-3)."""
    if not tm_str or pd.isna(tm_str) or ":" not in str(tm_str):
        return "15:00"
    try:
        parts = str(tm_str).strip().split(":")
        h = int(parts[0])
        m = int(parts[1])
        h_brt = (h - 3) % 24
        return f"{h_brt:02d}:{m:02d}"
    except Exception:
        return str(tm_str)

def baixar_fixtures_football_data(ttl_segundos: int = 300) -> pd.DataFrame:
    """Baixa fixtures.csv de football-data.co.uk com cache local."""
    agora = time.time()
    
    # Se cache local for recente, lê direto
    if CACHE_FILE.exists():
        idade = agora - CACHE_FILE.stat().st_mtime
        if idade < ttl_segundos:
            try:
                return pd.read_csv(CACHE_FILE, low_memory=False, encoding="utf-8-sig")
            except Exception:
                pass

    url = "https://www.football-data.co.uk/fixtures.csv"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    
    try:
        r = requests.get(url, headers=headers, timeout=20)
        if r.status_code == 200 and r.text.strip():
            content = r.content.decode("utf-8-sig", errors="replace")
            df = pd.read_csv(io.StringIO(content), low_memory=False)
            
            # Limpa nomes de colunas
            df.columns = [str(c).strip().replace("\ufeff", "") for c in df.columns]
            
            # Salva no cache
            CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(CACHE_FILE, index=False, encoding="utf-8-sig")
            return df
    except Exception as e:
        print(f"[!] Erro ao baixar fixtures.csv: {e}")

    # Fallback para cache antigo se a web falhar
    if CACHE_FILE.exists():
        try:
            return pd.read_csv(CACHE_FILE, low_memory=False, encoding="utf-8-sig")
        except Exception:
            pass

    return pd.DataFrame()

def escanear_jogos_candidatos_orbitx(data_iso: str = None) -> pd.DataFrame:
    """Escaneia os jogos da data especificada (YYYY-MM-DD) e gera os candidatos com tetos da OrbitX."""
    if not data_iso:
        data_iso = datetime.now().strftime("%Y-%m-%d")

    df_raw = baixar_fixtures_football_data()
    if df_raw.empty or "Date" not in df_raw.columns:
        return pd.DataFrame()

    def _parse_date(d_str):
        if not d_str or pd.isna(d_str):
            return ""
        s = str(d_str).strip()
        parts = s.split("/")
        if len(parts) == 3:
            day, month, year = parts
            if len(year) == 2:
                year = "20" + year
            return f"{year}-{int(month):02d}-{int(day):02d}"
        return s

    df_raw["Data_ISO"] = df_raw["Date"].apply(_parse_date)
    df_day = df_raw[df_raw["Data_ISO"] == data_iso].copy()
    if df_day.empty:
        return pd.DataFrame()

    candidatos = []

    for _, row in df_day.iterrows():
        div_code = str(row.get("Div", "")).strip()
        liga = DIV_NAMES.get(div_code, div_code if div_code else "Outra Liga")
        home = str(row.get("HomeTeam", "")).strip()
        away = str(row.get("AwayTeam", "")).strip()
        tm_utc = str(row.get("Time", "")).strip()
        hora_brt = _utc_para_brt(tm_utc)

        oh = pd.to_numeric(row.get("B365H") or row.get("AvgH") or row.get("PPH") or row.get("BFEH"), errors="coerce")
        od = pd.to_numeric(row.get("B365D") or row.get("AvgD") or row.get("PPD") or row.get("BFED"), errors="coerce")
        oa = pd.to_numeric(row.get("B365A") or row.get("AvgA") or row.get("PPA") or row.get("BFEA"), errors="coerce")

        ou25 = pd.to_numeric(row.get("B365<2.5") or row.get("Avg<2.5") or row.get("BFE<2.5"), errors="coerce")
        o25 = pd.to_numeric(row.get("B365>2.5") or row.get("Avg>2.5") or row.get("BFE>2.5"), errors="coerce")

        if pd.isna(oh) or pd.isna(oa):
            continue

        fav = min(oh, oa)
        eh_copa = bool(RE_COPA.search(liga) or RE_COPA.search(div_code))

        # 1. Lay Draw Super Fav (min(Fav) <= 1.40 | Copas só Mandante <= 1.40)
        draw_qualifica = False
        if not eh_copa and fav <= 1.40:
            draw_qualifica = True
        elif eh_copa and oh <= 1.40:
            draw_qualifica = True

        # Trava Fortaleza: se mandante <= 1.40 e Over 2.5 >= 1.75 -> Prioriza Lay Away Fortaleza
        eh_away_fortaleza = (oh <= 1.40 and pd.notna(o25) and o25 >= 1.75)

        if draw_qualifica and not eh_away_fortaleza:
            lado_fav = "CASA" if oh <= oa else "FORA"
            odd_ent = round(float(od), 2) if pd.notna(od) and od > 1.0 else 6.00
            candidatos.append({
                "Data": data_iso,
                "Hora": hora_brt,
                "Hora_UTC": tm_utc,
                "Liga": liga,
                "Jogo": f"{home} x {away}",
                "Home": home,
                "Away": away,
                "Método": "Lay Draw (Fav <= 1.40)",
                "Mercado": "The Draw (Empate)",
                "Lado": "LAY",
                "Odd_Entrada": odd_ent,
                "Odd_Fav": round(float(fav), 2),
                "Odd_Ref_b365": round(float(fav), 2),
                "Odd_D_b365": round(float(od), 2) if pd.notna(od) else 0.0,
                "Teto_OrbitX": "<= 7.00",
                "Spread_Max": "<= 1.15",
                "Expectativa_WR": "87.7%",
                "EV_Estimado": "+2.87%",
                "Resultado": "PENDENTE",
                "Status": "⏳ PENDENTE",
                "Placar": "vs",
                "Origem": "Football-Data (Radar OrbitX)",
                "Instrucao_OrbitX": f"Abrir OrbitX -> Seleção 'The Draw' -> Entrar em LAY se odd <= 7.00 (Fav {lado_fav} {fav:.2f})"
            })

        # 2. Lay Home / DC X2 (Fav Visitante <= 1.65)
        if oa <= 1.65:
            odd_ent = round(float(oh), 2) if pd.notna(oh) and oh > 1.0 else 7.00
            candidatos.append({
                "Data": data_iso,
                "Hora": hora_brt,
                "Hora_UTC": tm_utc,
                "Liga": liga,
                "Jogo": f"{home} x {away}",
                "Home": home,
                "Away": away,
                "Método": "Lay Home / DC X2 (Fav Visitante <= 1.65)",
                "Mercado": "Home (Mandante)",
                "Lado": "LAY",
                "Odd_Entrada": odd_ent,
                "Odd_Fav": round(float(oa), 2),
                "Odd_Ref_b365": round(float(oa), 2),
                "Odd_D_b365": round(float(od), 2) if pd.notna(od) else 0.0,
                "Teto_OrbitX": "<= 8.00",
                "Spread_Max": "<= 1.15",
                "Expectativa_WR": "92.3%",
                "EV_Estimado": "+6.18%",
                "Resultado": "PENDENTE",
                "Status": "⏳ PENDENTE",
                "Placar": "vs",
                "Origem": "Football-Data (Radar OrbitX)",
                "Instrucao_OrbitX": f"Abrir OrbitX -> Seleção '{home}' -> Entrar em LAY se odd <= 8.00 (Visitante Fav {oa:.2f})"
            })

        # 3. Lay Over 4.5 FT em Jogos Under (Under 2.5 <= 1.50)
        if pd.notna(ou25) and ou25 <= 1.50:
            candidatos.append({
                "Data": data_iso,
                "Hora": hora_brt,
                "Hora_UTC": tm_utc,
                "Liga": liga,
                "Jogo": f"{home} x {away}",
                "Home": home,
                "Away": away,
                "Método": "Lay Over 4.5 FT (Under Pesado)",
                "Mercado": "Over 4.5 Goals",
                "Lado": "LAY",
                "Odd_Entrada": 10.00,
                "Odd_Fav": round(float(ou25), 2),
                "Odd_Ref_b365": round(float(ou25), 2),
                "Odd_D_b365": round(float(od), 2) if pd.notna(od) else 0.0,
                "Teto_OrbitX": "<= 20.00",
                "Spread_Max": "<= 1.25",
                "Expectativa_WR": "97.8%",
                "EV_Estimado": "+5.24%",
                "Resultado": "PENDENTE",
                "Status": "⏳ PENDENTE",
                "Placar": "vs",
                "Origem": "Football-Data (Radar OrbitX)",
                "Instrucao_OrbitX": f"Abrir OrbitX -> Mercado 'Over/Under 4.5' -> Entrar em LAY Over 4.5 se odd <= 20.00 e Spread <= 1.25"
            })

        # 4. Lay Away Fortaleza 1X em Super Fav Mandante + Jogo Controlado
        if eh_away_fortaleza:
            odd_ent = round(float(oa), 2) if pd.notna(oa) and oa > 1.0 else 8.00
            candidatos.append({
                "Data": data_iso,
                "Hora": hora_brt,
                "Hora_UTC": tm_utc,
                "Liga": liga,
                "Jogo": f"{home} x {away}",
                "Home": home,
                "Away": away,
                "Método": "Lay Away Fortaleza 1X (Fav <= 1.40 | O25 >= 1.75)",
                "Mercado": "Away (Visitante)",
                "Lado": "LAY",
                "Odd_Entrada": odd_ent,
                "Odd_Fav": round(float(oh), 2),
                "Odd_Ref_b365": round(float(oh), 2),
                "Odd_D_b365": round(float(od), 2) if pd.notna(od) else 0.0,
                "Teto_OrbitX": "<= 15.00",
                "Spread_Max": "<= 1.20",
                "Expectativa_WR": "93.8%",
                "EV_Estimado": "+3.10%",
                "Resultado": "PENDENTE",
                "Status": "⏳ PENDENTE",
                "Placar": "vs",
                "Origem": "Football-Data (Radar OrbitX)",
                "Instrucao_OrbitX": f"Abrir OrbitX -> Seleção '{away}' -> Entrar em LAY se odd <= 15.00 (Mandante Fav {oh:.2f} + O25 {o25:.2f})"
            })

    df_out = pd.DataFrame(candidatos)
    if not df_out.empty:
        df_out = df_out.sort_values(["Hora", "Liga"]).reset_index(drop=True)
    return df_out

if __name__ == "__main__":
    hoje = datetime.now().strftime("%Y-%m-%d")
    print(f"Buscando candidatos para {hoje}...")
    df_cand = escanear_jogos_candidatos_orbitx(hoje)
    print(f"Total de candidatos encontrados: {len(df_cand)}")
    if not df_cand.empty:
        print(df_cand[["Hora", "Liga", "Jogo", "Método", "Odd_Entrada", "Teto_OrbitX", "Instrucao_OrbitX"]].to_string(index=False))
