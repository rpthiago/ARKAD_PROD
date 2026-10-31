# -*- coding: utf-8 -*-
"""
radar_prejogo_orbitx.py — Radar e Ledger Oficial de Validação Forward (Lay Draw & Lay Home Stake-Zero)

Documento de Governança Vinculante: PREREGISTRO_LAY_DRAW_FORWARD_2026-10-31.md
Autor: Claude (camada de medição) & Antigravity/Gemini (implementação)
Status: Forward ZERADO em 31/10/2026. Stake 0.00 em todos os registros.

Regras Congeladas:
1. LAY DRAW (Pré-Registro Formal):
   - min(BFEH, BFEA) <= 1.40 (em copas/mata-mata, apenas BFEH <= 1.40).
   - Fonte de preço: estritamente BFED (Betfair Exchange Back do football-data). Sem BFED -> SKIP.
   - Tabela de conversão congelada (SHA256: 1cbae42b32e118a11c7e3a3926d82229):
     * 4.50 <= BFED < 6.00: razao = 1.0755 (faixa '4.5-6.0')
     * 6.00 <= BFED <= 8.00: razao = 1.0616 (faixa '6.0-8.0')
     * Fora da faixa -> SKIP ('BFED_FORA_FAIXA_CONGELADA').
   - Teto rígido: lay_est = round(BFED * razao, 2) <= 7.00.
   - Ledger oficial: ledger_lay_draw_forward.csv.

2. LAY HOME (Laboratório de Observação Stake-Zero):
   - Visitante Favorito: BFEA <= 1.65.
   - Fonte de preço: BFEH (Betfair Exchange Back Mandante). Sem BFEH -> SKIP.
   - Conversão estimada: lay_est = round(BFEH * 1.065, 2) <= 8.00.
   - Ledger dedicado: ledger_lay_home_observacao.csv (não contamina o pré-registro do Lay Draw).
   - Stake compulsoriamente 0.00 (Liability 1.0u) — acompanhamento observacional.
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
LEDGER_FORWARD_PATH = ROOT / "ledger_lay_draw_forward.csv"
LEDGER_HOME_PATH = ROOT / "ledger_lay_home_observacao.csv"

VERSAO_PREREGISTRO = "2026-10-31"
SHA_TABELA = "1cbae42b32e118a11c7e3a3926d82229"

TABELA_CONVERSAO_FROZEN = {
    "4.5-6.0": {"min": 4.50, "max": 6.00, "razao": 1.0755, "p_liq200": 0.52},
    "6.0-8.0": {"min": 6.00, "max": 8.00, "razao": 1.0616, "p_liq200": 0.65},
}

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

LEDGER_COLS = [
    "data_ko", "hora_ko_brt", "liga_fd", "home", "away",
    "bfeh", "bfed", "bfea", "fav_odd", "faixa", "razao_aplicada", "lay_est",
    "elegivel", "motivo_skip",
    "stake", "liability",
    "gh", "ga", "res", "resultado", "pnl_u",
    "versao_preregistro", "sha_tabela"
]


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
            df.columns = [str(c).strip().replace("\ufeff", "") for c in df.columns]
            CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(CACHE_FILE, index=False, encoding="utf-8-sig")
            return df
    except Exception as e:
        print(f"[!] Erro ao baixar fixtures.csv: {e}")

    if CACHE_FILE.exists():
        try:
            return pd.read_csv(CACHE_FILE, low_memory=False, encoding="utf-8-sig")
        except Exception:
            pass

    return pd.DataFrame()


def avaliar_partida_lay_draw(row: pd.Series, data_iso: str) -> dict:
    """Avalia uma linha do football-data segundo o PREREGISTRO_LAY_DRAW_FORWARD_2026-10-31.md."""
    div_code = str(row.get("Div", "")).strip()
    home = str(row.get("HomeTeam", "")).strip()
    away = str(row.get("AwayTeam", "")).strip()
    tm_utc = str(row.get("Time", "")).strip()
    hora_brt = _utc_para_brt(tm_utc)

    bfeh = pd.to_numeric(row.get("BFEH"), errors="coerce")
    bfed = pd.to_numeric(row.get("BFED"), errors="coerce")
    bfea = pd.to_numeric(row.get("BFEA"), errors="coerce")

    rec = {
        "data_ko": data_iso,
        "hora_ko_brt": hora_brt,
        "liga_fd": div_code,
        "home": home,
        "away": away,
        "bfeh": round(float(bfeh), 2) if pd.notna(bfeh) else None,
        "bfed": round(float(bfed), 2) if pd.notna(bfed) else None,
        "bfea": round(float(bfea), 2) if pd.notna(bfea) else None,
        "fav_odd": None,
        "faixa": None,
        "razao_aplicada": None,
        "lay_est": None,
        "elegivel": "SKIP",
        "motivo_skip": "",
        "stake": 0.00,
        "liability": 0.0,
        "gh": None,
        "ga": None,
        "res": None,
        "resultado": "PENDENTE",
        "pnl_u": 0.0,
        "versao_preregistro": VERSAO_PREREGISTRO,
        "sha_tabela": SHA_TABELA,
    }

    # 1. Fonte única: BFED, BFEH e BFEA
    if pd.isna(bfed) or pd.isna(bfeh) or pd.isna(bfea) or bfed <= 1.01 or bfeh <= 1.01 or bfea <= 1.01:
        rec["motivo_skip"] = "SEM_BFE_EXCHANGE"
        return rec

    fav = min(bfeh, bfea)
    rec["fav_odd"] = round(float(fav), 2)

    # 2. Exceção de copa vs liga
    eh_copa = bool(RE_COPA.search(div_code) or RE_COPA.search(home) or RE_COPA.search(away))
    if eh_copa:
        if bfeh > 1.40:
            rec["motivo_skip"] = "COPA_MANDANTE_>_1.40"
            return rec
    else:
        if fav > 1.40:
            rec["motivo_skip"] = "FAV_>_1.40"
            return rec

    # 3. Faixa de BFED congelada
    faixa = None
    razao = None
    if 4.50 <= bfed < 6.00:
        faixa = "4.5-6.0"
        razao = TABELA_CONVERSAO_FROZEN["4.5-6.0"]["razao"]
    elif 6.00 <= bfed <= 8.00:
        faixa = "6.0-8.0"
        razao = TABELA_CONVERSAO_FROZEN["6.0-8.0"]["razao"]
    else:
        rec["faixa"] = "OUT"
        rec["motivo_skip"] = "BFED_FORA_FAIXA_CONGELADA"
        return rec

    rec["faixa"] = faixa
    rec["razao_aplicada"] = razao
    lay_est = round(float(bfed * razao), 2)
    rec["lay_est"] = lay_est

    # 4. Teto rígido: lay_est <= 7.00
    if lay_est > 7.00:
        rec["motivo_skip"] = "TETO_LAY_ESTIMADO_>_7.00"
        return rec

    # Qualificado com sucesso
    rec["elegivel"] = "SIM"
    rec["motivo_skip"] = ""
    rec["liability"] = 1.0
    return rec


def sincronizar_ledger_forward(registros: list, arquivo_ledger: Path):
    """Grava append-only em ledger sem duplicar partidas."""
    if not registros:
        return

    df_novos = pd.DataFrame(registros, columns=LEDGER_COLS)
    df_novos["chave"] = df_novos["data_ko"].astype(str) + "|" + df_novos["liga_fd"].astype(str) + "|" + df_novos["home"].astype(str) + "|" + df_novos["away"].astype(str)

    if arquivo_ledger.exists():
        try:
            df_exist = pd.read_csv(arquivo_ledger, encoding="utf-8")
            df_exist["chave"] = df_exist["data_ko"].astype(str) + "|" + df_exist["liga_fd"].astype(str) + "|" + df_exist["home"].astype(str) + "|" + df_exist["away"].astype(str)
            chaves_existentes = set(df_exist["chave"].dropna())
            df_novos = df_novos[~df_novos["chave"].isin(chaves_existentes)]
            if not df_novos.empty:
                df_novos = df_novos.drop(columns=["chave"])
                df_novos.to_csv(arquivo_ledger, mode="a", header=False, index=False, encoding="utf-8")
        except Exception as e:
            print(f"[!] Erro ao anexar ao ledger ({arquivo_ledger.name}): {e}")
    else:
        df_novos = df_novos.drop(columns=["chave"])
        arquivo_ledger.parent.mkdir(parents=True, exist_ok=True)
        df_novos.to_csv(arquivo_ledger, index=False, encoding="utf-8")


def escanear_jogos_candidatos_orbitx(data_iso: str = None) -> pd.DataFrame:
    """
    Escaneia os jogos da data especificada (YYYY-MM-DD).
    Retorna os candidatos de:
      1. Lay Draw Super Fav (Pré-Registro 31/10/2026 oficial);
      2. Lay Home Fav Visitante (Laboratório de Observação Stake-Zero).
    Ambos em STAKE 0.00 (observação pura / sem dinheiro).
    """
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

    registros_ledger_draw = []
    registros_ledger_home = []
    candidatos_sim = []

    for _, row in df_day.iterrows():
        div_code = str(row.get("Div", "")).strip()
        liga_nome = DIV_NAMES.get(div_code, div_code)
        home = str(row.get("HomeTeam", "")).strip()
        away = str(row.get("AwayTeam", "")).strip()
        tm_utc = str(row.get("Time", "")).strip()
        hora_brt = _utc_para_brt(tm_utc)

        bfeh = pd.to_numeric(row.get("BFEH"), errors="coerce")
        bfed = pd.to_numeric(row.get("BFED"), errors="coerce")
        bfea = pd.to_numeric(row.get("BFEA"), errors="coerce")

        # 1. AVALIAÇÃO LAY DRAW (PRÉ-REGISTRO OFICIAL)
        rec_draw = avaliar_partida_lay_draw(row, data_iso)
        registros_ledger_draw.append(rec_draw)

        if rec_draw["elegivel"] == "SIM":
            candidatos_sim.append({
                "Data": data_iso,
                "Hora": hora_brt,
                "Hora_UTC": tm_utc,
                "Liga": liga_nome,
                "Jogo": f"{home} x {away}",
                "Home": home,
                "Away": away,
                "Método": "Lay Draw Super Fav (Pré-Registro 31/10)",
                "Mercado": "The Draw (Empate)",
                "Lado": "LAY",
                "Odd_Entrada": rec_draw["lay_est"],
                "Odd_Ref_Exchange": rec_draw["bfed"],
                "Odd_Fav": rec_draw["fav_odd"],
                "Faixa_BFED": rec_draw["faixa"],
                "Razao_Aplicada": rec_draw["razao_aplicada"],
                "Teto_OrbitX": "<= 7.00",
                "Spread_Max": "<= 1.15",
                "Stake_Sugerida_R$": 0.00,
                "Liability_u": 1.0,
                "Expectativa_WR": "87.7%",
                "EV_Estimado": "+2.87%",
                "Resultado": "PENDENTE",
                "Status": "⏳ PENDENTE (Stake-Zero)",
                "Placar": "vs",
                "Origem": "Football-Data (Exchange BFED)",
                "Instrucao_OrbitX": f"Acompanhamento Stake-Zero -> Seleção 'The Draw' -> Lay Est `{rec_draw['lay_est']:.2f}` (BFED `{rec_draw['bfed']:.2f}` x {rec_draw['razao_aplicada']})"
            })

        # 2. AVALIAÇÃO LAY HOME (LABORATÓRIO DE OBSERVAÇÃO STAKE-ZERO)
        rec_home = {
            "data_ko": data_iso,
            "hora_ko_brt": hora_brt,
            "liga_fd": div_code,
            "home": home,
            "away": away,
            "bfeh": round(float(bfeh), 2) if pd.notna(bfeh) else None,
            "bfed": round(float(bfed), 2) if pd.notna(bfed) else None,
            "bfea": round(float(bfea), 2) if pd.notna(bfea) else None,
            "fav_odd": round(float(bfea), 2) if pd.notna(bfea) else None,
            "faixa": "MATCH_ODDS",
            "razao_aplicada": 1.065,
            "lay_est": None,
            "elegivel": "SKIP",
            "motivo_skip": "",
            "stake": 0.00,
            "liability": 0.0,
            "gh": None,
            "ga": None,
            "res": None,
            "resultado": "PENDENTE",
            "pnl_u": 0.0,
            "versao_preregistro": "OBS_HOME_2026-10-31",
            "sha_tabela": "obs_lay_home_1.065",
        }

        if pd.isna(bfeh) or pd.isna(bfea) or bfeh <= 1.01 or bfea <= 1.01:
            rec_home["motivo_skip"] = "SEM_BFE_EXCHANGE"
        elif bfea > 1.65:
            rec_home["motivo_skip"] = "FAV_VISITANTE_>_1.65"
        else:
            lay_est_home = round(float(bfeh * 1.065), 2)
            rec_home["lay_est"] = lay_est_home
            if lay_est_home > 8.00:
                rec_home["motivo_skip"] = "TETO_LAY_ESTIMADO_>_8.00"
            else:
                rec_home["elegivel"] = "SIM"
                rec_home["liability"] = 1.0
                candidatos_sim.append({
                    "Data": data_iso,
                    "Hora": hora_brt,
                    "Hora_UTC": tm_utc,
                    "Liga": liga_nome,
                    "Jogo": f"{home} x {away}",
                    "Home": home,
                    "Away": away,
                    "Método": "Lay Home Fav Visitante [OBSERVAÇÃO STAKE-ZERO]",
                    "Mercado": "Home (Mandante)",
                    "Lado": "LAY",
                    "Odd_Entrada": lay_est_home,
                    "Odd_Ref_Exchange": round(float(bfeh), 2),
                    "Odd_Fav": round(float(bfea), 2),
                    "Faixa_BFED": "MATCH_ODDS",
                    "Razao_Aplicada": 1.065,
                    "Teto_OrbitX": "<= 8.00",
                    "Spread_Max": "<= 1.15",
                    "Stake_Sugerida_R$": 0.00,
                    "Liability_u": 1.0,
                    "Expectativa_WR": "92.3%",
                    "EV_Estimado": "+6.18%",
                    "Resultado": "PENDENTE",
                    "Status": "⏳ PENDENTE (Stake-Zero)",
                    "Placar": "vs",
                    "Origem": "Football-Data (Exchange BFEH)",
                    "Instrucao_OrbitX": f"Acompanhamento Stake-Zero -> Seleção '{home}' -> Lay Est `{lay_est_home:.2f}` (BFEH `{bfeh:.2f}` | Fav Visitante `{bfea:.2f}`)"
                })

        registros_ledger_home.append(rec_home)

    # Sincroniza ambos os ledgers separadamente
    sincronizar_ledger_forward(registros_ledger_draw, LEDGER_FORWARD_PATH)
    sincronizar_ledger_forward(registros_ledger_home, LEDGER_HOME_PATH)

    df_out = pd.DataFrame(candidatos_sim)
    if not df_out.empty:
        df_out = df_out.sort_values(["Hora", "Liga"]).reset_index(drop=True)
    return df_out


if __name__ == "__main__":
    hoje = datetime.now().strftime("%Y-%m-%d")
    print(f"=== RADAR & LEDGER PRÉ-REGISTRO 31/10/2026 ({hoje}) ===")
    df_cand = escanear_jogos_candidatos_orbitx(hoje)
    print(f"\n[+] Total de candidatos ELEGÍVEIS (SIM): {len(df_cand)}")
    if not df_cand.empty:
        print(df_cand[["Hora", "Liga", "Jogo", "Método", "Odd_Ref_Exchange", "Odd_Entrada", "Teto_OrbitX", "Status"]].to_string(index=False))
