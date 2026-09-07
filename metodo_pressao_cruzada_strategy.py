# -*- coding: utf-8 -*-
"""
metodo_pressao_cruzada_strategy.py — Estratégia de Pressão Cruzada (Cross-Market Index)
ARKAD_PROD

Combina as odds de 1X2, Over/Under 2.5 e BTTS para mapear a dinâmica de gols e 
identificar assimetrias de mercado com odd de lay real da Betfair Exchange:

1. Constante de Pressão Ofensiva:
   K_pressao = Odd_H * Odd_Over25 (ou Odd_Fav * Odd_Over25)
   - K < 2.20: Super Favorito com forte propensão a Over (Rolo Compressor).
   - K > 3.80: Favorito em jogo amarrado / Under pesado (Falso Favorito).

2. Constante de Simetria de Gols:
   K_ratio = Odd_Over25 / Odd_BTTS_Yes
   - K_ratio < 0.90: Over concentrado em 1 time só (goleada solitária).
   - K_ratio > 1.15: Se sair gol, ambos marcam (tendência a 1x1).

Conforme as Leis do GEMINI.md:
- Odds de entrada e liability 100% na odd de LAY real da Betfair.
- Nenhum dado fabricado: ausência de odds essenciais resulta em SKIP.
"""

from typing import Dict, Any, Tuple, Optional
import pandas as pd
import numpy as np


def calcular_constantes(
    odd_h: float,
    odd_over25: float,
    odd_btts_yes: Optional[float] = None
) -> Dict[str, float]:
    """Calcula os índices de pressão cruzada a partir das odds básicas de mercado."""
    if pd.isna(odd_h) or pd.isna(odd_over25) or odd_h <= 1.0 or odd_over25 <= 1.0:
        return {"k_pressao": np.nan, "k_ratio": np.nan}
    
    k_pressao = round(float(odd_h * odd_over25), 3)
    k_ratio = round(float(odd_over25 / odd_btts_yes), 3) if (pd.notna(odd_btts_yes) and odd_btts_yes > 1.0) else np.nan
    
    return {
        "k_pressao": k_pressao,
        "k_ratio": k_ratio
    }


def avaliar_jogo_pressao_cruzada(row: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Avalia uma partida para os dois perfis da estratégia de Pressão Cruzada:
    - Perfil 1: Lay Draw de Alta Pressão (Super Fav + Over)
    - Perfil 2: Lay Home Falso Favorito (Fav mandante + Under Pesado)
    
    Retorna:
    (aprovado: bool, motivo_ou_perfil: str, metricas: dict)
    """
    # 1. Extração e sanitização das odds
    odd_h_back = pd.to_numeric(row.get("Odd_H_Back") or row.get("Odd_H_FT_Back") or row.get("Odd_H_FT") or row.get("Odd_H"), errors="coerce")
    odd_a_back = pd.to_numeric(row.get("Odd_A_Back") or row.get("Odd_A_FT_Back") or row.get("Odd_A_FT") or row.get("Odd_A"), errors="coerce")
    odd_d_lay = pd.to_numeric(row.get("Odd_D_Lay") or row.get("Odd_CS_Draw_Lay"), errors="coerce")
    odd_h_lay = pd.to_numeric(row.get("Odd_H_Lay"), errors="coerce")
    
    odd_over25 = pd.to_numeric(row.get("Odd_Over25_FT_Back") or row.get("Odd_Over25_FT") or row.get("Odd_Over25"), errors="coerce")
    odd_btts = pd.to_numeric(row.get("Odd_BTTS_Yes_Back") or row.get("Odd_BTTS_Yes") or row.get("Odd_BTTS"), errors="coerce")
    
    # Validações mínimas estruturais
    if pd.isna(odd_h_back) or pd.isna(odd_over25) or odd_h_back <= 1.0 or odd_over25 <= 1.0:
        return False, "ODDS_BASICAS_AUSENTES", {}
        
    fav_odd = min(odd_h_back, odd_a_back) if (pd.notna(odd_a_back) and odd_a_back > 1.0) else odd_h_back
    
    indices = calcular_constantes(odd_h_back, odd_over25, odd_btts)
    k_pressao = indices["k_pressao"]
    k_ratio = indices["k_ratio"]
    
    # -------------------------------------------------------------
    # PERFIL 1: Lay Draw de Alta Pressão (Super Favorito Rolo Compressor)
    # -------------------------------------------------------------
    # - Favorito muito forte (Odd <= 1.40)
    # - K_pressao <= 2.20 (Combinação de vitória provável com expectativa alta de gols)
    # - Odd Lay do Empate na Betfair entre 4.5 e 10.0
    if fav_odd <= 1.40 and k_pressao <= 2.20 and pd.notna(odd_d_lay) and (4.5 <= odd_d_lay <= 10.0):
        metricas = {
            "perfil": "LAY_DRAW_ALTA_PRESSAO",
            "mercado": "Match Odds (Draw)",
            "lado": "LAY",
            "odd_entrada": odd_d_lay,
            "odd_fav": fav_odd,
            "k_pressao": k_pressao,
            "k_ratio": k_ratio,
            "be_wr": round((odd_d_lay - 1.0) / (odd_d_lay - 0.05) * 100, 1),
            "expected_wr": 86.5
        }
        return True, "APROVADO_LAY_DRAW_ALTA_PRESSAO", metricas

    # -------------------------------------------------------------
    # PERFIL 2: Lay Home no Falso Favorito (Mandante Frágil em Jogo Truncado)
    # -------------------------------------------------------------
    # - Mandante favorito nominal (1.45 <= Odd_H <= 1.70)
    # - K_pressao >= 3.80 (Indica que o Over está caro @ 2.30+, jogo muito truncado)
    # - Odd Lay do Mandante entre 1.50 e 4.00 (risco controlado de liability)
    if (1.45 <= odd_h_back <= 1.70) and (k_pressao >= 3.80) and pd.notna(odd_h_lay) and (1.50 <= odd_h_lay <= 4.00):
        metricas = {
            "perfil": "LAY_HOME_FALSO_FAVORITO",
            "mercado": "Match Odds (Home)",
            "lado": "LAY",
            "odd_entrada": odd_h_lay,
            "odd_fav": odd_h_back,
            "k_pressao": k_pressao,
            "k_ratio": k_ratio,
            "be_wr": round((odd_h_lay - 1.0) / (odd_h_lay - 0.05) * 100, 1),
            "expected_wr": 46.0  # Lay Home ganha se empate ou derrota do mandante
        }
        return True, "APROVADO_LAY_HOME_FALSO_FAVORITO", metricas

    return False, "FORA_CRITERIOS_PRESSAO_CRUZADA", {"k_pressao": k_pressao, "k_ratio": k_ratio}


def escanear_grade_pressao_cruzada(df: pd.DataFrame) -> pd.DataFrame:
    """Escaneia um DataFrame de partidas diárias e retorna todos os sinais qualificados."""
    if df.empty:
        return pd.DataFrame()
        
    sinais = []
    for idx, row in df.iterrows():
        r_dict = row.to_dict()
        aprovado, motivo, metricas = avaliar_jogo_pressao_cruzada(r_dict)
        if not aprovado:
            continue
            
        home = str(row.get("Home") or row.get("Home_Team") or "")
        away = str(row.get("Away") or row.get("Away_Team") or "")
        liga = str(row.get("League") or row.get("Liga") or "")
        hora = str(row.get("Time") or row.get("Hora") or "")
        data_str = str(row.get("Date") or row.get("Data") or "")[:10]
        
        sinais.append({
            "Data": data_str,
            "Hora": hora,
            "Liga": liga,
            "Confronto": f"{home} x {away}",
            "Home": home,
            "Away": away,
            "Perfil": metricas["perfil"],
            "Mercado": metricas["mercado"],
            "Lado": metricas["lado"],
            "Odd_Entrada": metricas["odd_entrada"],
            "Odd_Fav": metricas["odd_fav"],
            "K_pressao": metricas["k_pressao"],
            "K_ratio": metricas["k_ratio"],
            "Break_Even_WR": metricas["be_wr"],
            "WR_Esperada": metricas["expected_wr"],
            "Motivo": motivo
        })
        
    return pd.DataFrame(sinais)
