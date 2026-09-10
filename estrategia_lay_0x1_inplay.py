# -*- coding: utf-8 -*-
"""
estrategia_lay_0x1_inplay.py — Estratégia Quantitativa In-Play Lay 0x1 (Minuto 55-75)
===================================================================================
TESE OPERACIONAL (ROTA C):
1. No pré-jogo, a odd de Lay 0x1 é inflada (12 a 35) e o mercado é hiper-eficiente.
2. No In-Play, quando o jogo chega aos 55'-75' empatado em 0x0:
   - A odd de Lay 0x1 cai drasticamente para a faixa de 2.00 a 5.50 (liability baixo).
   - Se o mandante favorito estiver pressionando, a chance de o jogo terminar
     exatamente 0x1 (visitante marcar 1 gol e mandante ficar no zero até o final)
     é estatisticamente reduzida.
   - O apostador vence (GREEN) se:
     * O mandante marcar gol (1x0, 2x0, 2x1, etc.)
     * A partida terminar 0x0
     * O visitante marcar 2 ou mais gols (0x2, 1x2, 0x3, etc.)
   - O apostador SÓ perde (RED) se:
     * O placar final for exatamente 0 x 1 (0 gols do mandante, 1 gol do visitante).
"""

import math
import numpy as np
import pandas as pd

# Parâmetros Estritos de Entrada (Pré-Registrados)
MINUTO_INPLAY_MIN = 55
MINUTO_INPLAY_MAX = 75
ODD_LAY_0X1_MIN = 2.00
ODD_LAY_0X1_MAX = 5.50
ODD_H_BACK_PRE_MAX = 2.25  # Mandante não pode ser zebraça pré-jogo
COMISSAO_BETFAIR = 0.05


def calcular_break_even(odd_lay: float, comissao: float = COMISSAO_BETFAIR) -> float:
    """Calcula a taxa de acerto mínima necessária (Break-Even Win Rate) pela Lei 4 do GEMINI.md."""
    if odd_lay <= 1.05:
        return 50.0
    return ((odd_lay - 1.0) / (odd_lay - comissao)) * 100.0


def calcular_financeiro_lay(odd_lay: float, stake: float = 100.0, comissao: float = COMISSAO_BETFAIR) -> dict:
    """Calcula os valores nominais de risco e retorno de uma entrada em Lay."""
    liability = stake * (odd_lay - 1.0)
    lucro_green = stake * (1.0 - comissao)
    be_wr = calcular_break_even(odd_lay, comissao)
    
    return {
        "stake": round(stake, 2),
        "odd_lay": round(odd_lay, 2),
        "liability": round(liability, 2),
        "lucro_green": round(lucro_green, 2),
        "break_even_wr": round(be_wr, 2)
    }


def validar_entrada_lay0x1_inplay(
    minuto: int,
    gols_h_atual: int,
    gols_a_atual: int,
    odd_lay_0x1: float,
    odd_h_back_pre: float = None,
    sot_h: int = None,
    sot_a: int = None,
    xg_h: float = None,
    xg_a: float = None,
    posse_h: float = None
) -> tuple:
    """
    Validação estrita de entrada ao vivo no Lay 0x1.
    Retorna: (aprovado: bool, motivo: str, metricas: dict)
    """
    # 1. Validação de Minuto
    if minuto is None or minuto < MINUTO_INPLAY_MIN or minuto > MINUTO_INPLAY_MAX:
        return False, f"MINUTO_FORA_FAIXA ({minuto}' fora de [{MINUTO_INPLAY_MIN}, {MINUTO_INPLAY_MAX}])", {}

    # 2. Validação de Placar Atual (deve estar 0x0)
    if gols_h_atual is None or gols_a_atual is None or gols_h_atual != 0 or gols_a_atual != 0:
        return False, f"PLACAR_INVALIDO ({gols_h_atual}x{gols_a_atual} - deve estar 0x0)", {}

    # 3. Validação de Odd Executável de Lay 0x1
    if odd_lay_0x1 is None or pd.isna(odd_lay_0x1) or odd_lay_0x1 < ODD_LAY_0X1_MIN or odd_lay_0x1 > ODD_LAY_0X1_MAX:
        return False, f"ODD_LAY_FORA_FAIXA (Odd {odd_lay_0x1} fora de [{ODD_LAY_0X1_MIN}, {ODD_LAY_0X1_MAX}])", {}

    # 4. Validação de Perfil Pré-Jogo (Mandante não pode ser zebraça)
    if odd_h_back_pre is not None and pd.notna(odd_h_back_pre):
        if odd_h_back_pre > ODD_H_BACK_PRE_MAX:
            return False, f"MANDANTE_ZEBRA_PRE (Odd {odd_h_back_pre:.2f} > {ODD_H_BACK_PRE_MAX:.2f})", {}

    # 5. Indicadores de Pressão Ofensiva (quando disponíveis)
    score_pressao = 0
    detalhes_pressao = []
    
    if sot_h is not None and sot_a is not None:
        if sot_h >= 2 and sot_h >= sot_a:
            score_pressao += 1
            detalhes_pressao.append(f"SoT: {sot_h}x{sot_a}")
            
    if xg_h is not None and xg_a is not None:
        if xg_h >= 0.70 and xg_h >= xg_a:
            score_pressao += 1
            detalhes_pressao.append(f"xG: {xg_h:.2f}x{xg_a:.2f}")
            
    if posse_h is not None and posse_h >= 52.0:
        score_pressao += 1
        detalhes_pressao.append(f"Posse: {posse_h:.0f}%")

    # Métricas de Risco da Entrada
    fin = calcular_financeiro_lay(odd_lay_0x1)
    fin["score_pressao"] = score_pressao
    fin["detalhes_pressao"] = " | ".join(detalhes_pressao) if detalhes_pressao else "Sem telemetria detalhada"
    
    return True, "APROVADO_LAY_0X1_INPLAY", fin


def liquidar_resultado_lay0x1(goals_h_ft: int, goals_a_ft: int, odd_lay: float, stake: float = 100.0, comissao: float = COMISSAO_BETFAIR) -> dict:
    """
    Liquidação pós-jogo da entrada Lay 0x1.
    GREEN: qualquer placar FT exceto 0x1.
    RED: exatamente 0x1 FT.
    """
    if goals_h_ft is None or goals_a_ft is None or pd.isna(goals_h_ft) or pd.isna(goals_a_ft):
        return {"status": "PENDENTE", "resultado": "PENDENTE", "pnl_u": 0.0, "pnl_r": 0.0}

    gh, ga = int(goals_h_ft), int(goals_a_ft)
    is_red = (gh == 0 and ga == 1)
    res_str = "RED" if is_red else "GREEN"
    
    if is_red:
        pnl_u = -(odd_lay - 1.0)
        pnl_r = -(stake * (odd_lay - 1.0))
        pontuacao = 0
    else:
        pnl_u = (1.0 - comissao)
        pnl_r = stake * (1.0 - comissao)
        pontuacao = 1

    return {
        "placar_ft": f"{gh}x{ga}",
        "resultado": res_str,
        "1/0": pontuacao,
        "pnl_u": round(pnl_u, 4),
        "pnl_r": round(pnl_r, 2),
        "status": res_str
    }
