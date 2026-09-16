# -*- coding: utf-8 -*-
"""
trader_inplay_engine.py — Motor Analítico e Calculadora Financeira para Métodos Trader In-Play.

Autoridade: PREREGISTRO_SUITE_TRADER_INPLAY.md & GEMINI.md
Status: OBSERVACAO_STAKE_ZERO (stake: 0.0)

Contém:
1. Funções matemáticas de Cashout, Hedging, Freebet e Stop Loss com comissão Betfair real (5%).
2. Avaliadores de oportunidade em tempo real para os 4 Métodos Trader In-Play:
   - LTD Trader Clássico (15'-25' 0-0, saída no 1º gol)
   - Swing Trade: Favorito em Desvantagem (20'-45' 0-1, saída no empate)
   - Scalping de Janela Morta (33'-38' HT ou 55'-62' FT)
   - Late Goal Trader (78'-84' com 1 gol de diferença)
3. Scanner consolidado sem fabricação de dados (AGUARDANDO_ODD em ausência de odd real).
"""

from typing import Optional, Union, Dict, Any, List
import math
import unicodedata
import re
import pandas as pd
import numpy as np


def _canon(s: str) -> str:
    if not isinstance(s, str) or not s:
        return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)


# ==============================================================================
# 1. MATEMÁTICA REAL DE CASHOUT, HEDGING E STOP LOSS (BETFAIR COMMISSION = 5%)
# ==============================================================================

def calcular_cashout_lay(odd_lay_in: float, odd_back_out: float, liability: float, commission: float = 0.05) -> dict:
    """
    Calcula o cashout de uma posição aberta em LAY (ex: LTD Trader).
    - odd_lay_in: Odd em que o Lay foi feito.
    - odd_back_out: Odd atual disponível para Back no mercado.
    - liability: Responsabilidade assumida em R$ (capital em risco).
    - commission: Comissão da Betfair sobre o lucro líquido (padrão 5%).
    """
    if odd_lay_in <= 1.01 or odd_back_out <= 1.01 or liability <= 0:
        return {
            "valido": False,
            "stake_fechamento": 0.0,
            "pnl_bruto": 0.0,
            "pnl_liquido": 0.0,
            "roi_pct": 0.0,
            "tipo": "ERRO",
            "descricao": "Odds ou liability inválidas."
        }

    # Stake nominal correspondente à liability do Lay
    stake_in = liability / (odd_lay_in - 1.0)

    # Stake necessária no Back para zerar o risco e distribuir lucro/prejuízo idêntico
    stake_out = (stake_in * odd_lay_in) / odd_back_out

    # Lucro ou prejuízo bruto uniforme em qualquer resultado
    pnl_bruto = stake_in - stake_out

    if pnl_bruto > 0:
        pnl_liquido = pnl_bruto * (1.0 - commission)
        tipo = "GREEN"
    else:
        pnl_liquido = pnl_bruto  # Sem comissão em prejuízo
        tipo = "RED" if pnl_bruto < -0.01 else "BREAKEVEN"

    roi_pct = (pnl_liquido / liability) * 100.0

    # PnL nominal alternativo considerando liability como base direta de 1 unidade
    pnl_nominal = liability * (1.0 - odd_lay_in / odd_back_out) * (1.0 - commission if odd_back_out > odd_lay_in else 1.0)

    return {
        "valido": True,
        "stake_in": round(stake_in, 2),
        "stake_fechamento": round(stake_out, 2),
        "pnl_bruto": round(pnl_bruto, 2),
        "pnl_liquido": round(pnl_liquido, 2),
        "pnl_nominal": round(pnl_nominal, 2),
        "roi_pct": round(roi_pct, 2),
        "tipo": tipo,
        "descricao": f"Back de R$ {stake_out:.2f} @ {odd_back_out:.2f} para travar {tipo} de R$ {pnl_liquido:.2f} ({roi_pct:+.1f}% sobre liability)"
    }


def calcular_cashout_ltd(odd_lay_entrada: float, odd_draw_atual: float, liability: float, commission: float = 0.05) -> dict:
    """
    Alias explícito para o cálculo de Cashout do Lay the Draw (LTD Trader).
    - odd_lay_entrada: Odd de entrada em Lay Draw.
    - odd_draw_atual: Odd atual de Back no Empate.
    - liability: Capital em risco em R$.
    - commission: Comissão Betfair (padrão 5%).
    """
    return calcular_cashout_lay(odd_lay_in=odd_lay_entrada, odd_back_out=odd_draw_atual, liability=liability, commission=commission)


def calcular_cashout_back(odd_back_in: float, odd_lay_out: float, stake_in: float, commission: float = 0.05) -> dict:
    """
    Calcula o cashout de uma posição aberta em BACK (ex: Fav em Desvantagem ou Scalping Under).
    - odd_back_in: Odd em que o Back foi feito.
    - odd_lay_out: Odd atual disponível para Lay no mercado.
    - stake_in: Valor apostado no Back em R$ (capital em risco).
    - commission: Comissão da Betfair sobre o lucro líquido (padrão 5%).
    """
    if odd_back_in <= 1.01 or odd_lay_out <= 1.01 or stake_in <= 0:
        return {
            "valido": False,
            "stake_fechamento": 0.0,
            "liability_fechamento": 0.0,
            "pnl_bruto": 0.0,
            "pnl_liquido": 0.0,
            "roi_pct": 0.0,
            "tipo": "ERRO",
            "descricao": "Odds ou stake inválidas."
        }

    # Stake necessária no Lay para equilibrar o retorno em todos os placares
    stake_out = (stake_in * odd_back_in) / odd_lay_out

    # Lucro ou prejuízo bruto uniforme
    pnl_bruto = stake_out - stake_in

    if pnl_bruto > 0:
        pnl_liquido = pnl_bruto * (1.0 - commission)
        tipo = "GREEN"
    else:
        pnl_liquido = pnl_bruto
        tipo = "RED" if pnl_bruto < -0.01 else "BREAKEVEN"

    roi_pct = (pnl_liquido / stake_in) * 100.0

    return {
        "valido": True,
        "stake_in": round(stake_in, 2),
        "stake_fechamento": round(stake_out, 2),
        "liability_fechamento": round(stake_out * (odd_lay_out - 1.0), 2),
        "pnl_bruto": round(pnl_bruto, 2),
        "pnl_liquido": round(pnl_liquido, 2),
        "roi_pct": round(roi_pct, 2),
        "tipo": tipo,
        "descricao": f"Lay de R$ {stake_out:.2f} @ {odd_lay_out:.2f} para travar {tipo} de R$ {pnl_liquido:.2f} ({roi_pct:+.1f}% sobre stake)"
    }


def calcular_freebet_back(odd_back_in: float, odd_lay_out: float, stake_in: float, commission: float = 0.05) -> dict:
    """
    Calcula a Freebet (Stop-at-Zero) para posição Back:
    Fecha exatamente a stake inicial em Lay, garantindo R$ 0,00 de perda em caso adverso
    e mantendo todo o lucro potencial concentrado no evento.
    """
    if odd_back_in <= 1.01 or odd_lay_out <= 1.01 or stake_in <= 0:
        return {"valido": False, "stake_fechamento": 0.0, "lucro_potencial": 0.0, "descricao": "Odds ou stake inválidas."}

    stake_out = stake_in  # Lay com o mesmo valor da entrada
    liability_lay = stake_out * (odd_lay_out - 1.0)
    retorno_back = stake_in * (odd_back_in - 1.0)

    lucro_se_vencer = (retorno_back - liability_lay) * (1.0 - commission)

    return {
        "valido": True,
        "stake_fechamento": round(stake_out, 2),
        "perda_se_nao_vencer": 0.0,
        "lucro_se_vencer": round(lucro_se_vencer, 2),
        "descricao": f"Lay de R$ {stake_out:.2f} @ {odd_lay_out:.2f}: R$ 0,00 de risco, R$ {lucro_se_vencer:.2f} se vencer."
    }


def calcular_stop_loss_tempo(
    odd_entrada: float,
    odd_atual: float,
    liability_ou_stake: float,
    tipo: str = "LAY",
    minuto_atual: int = 0,
    minuto_limite: int = 68,
    commission: float = 0.05
) -> dict:
    """
    Calcula a situação de saída por tempo ou stop loss dinâmico.
    - odd_entrada: Odd de abertura.
    - odd_atual: Odd atual de saída (Back se entrada foi Lay, Lay se entrada foi Back).
    - liability_ou_stake: Capital em risco (Liability para Lay, Stake para Back).
    - tipo: 'LAY' ou 'BACK'.
    - minuto_atual: Minuto decorrido na partida.
    - minuto_limite: Minuto de corte planejado para Stop Loss (ex: 68' no LTD, 70' no Fav Desvantagem).
    - commission: Taxa Betfair (0.05).
    
    Retorna:
    - stake_fechamento: valor a apostar no mercado inverso.
    - pnl_liquido: resultado financeiro em R$.
    - roi_pct: retorno sobre o capital em risco.
    - recomendacao: 'CASHOUT_GREEN', 'MANTER', ou 'STOP_LOSS'.
    - descricao: mensagem operacional clara.
    """
    tipo_upper = str(tipo).strip().upper()

    if tipo_upper == "LAY":
        calc = calcular_cashout_lay(odd_lay_in=odd_entrada, odd_back_out=odd_atual, liability=liability_ou_stake, commission=commission)
    else:
        calc = calcular_cashout_back(odd_back_in=odd_entrada, odd_lay_out=odd_atual, stake_in=liability_ou_stake, commission=commission)

    if not calc.get("valido"):
        return {
            "valido": False,
            "recomendacao": "ERRO",
            "stake_fechamento": 0.0,
            "pnl_liquido": 0.0,
            "roi_pct": 0.0,
            "descricao": calc.get("descricao", "Cálculo inválido.")
        }

    pnl = calc["pnl_liquido"]
    roi = calc["roi_pct"]
    stake_fechamento = calc["stake_fechamento"]

    if roi > 0:
        recomendacao = "CASHOUT_GREEN"
        descricao = f"Take Profit disponível! P&L líquido de R$ {pnl:+.2f} ({roi:+.1f}%). Recomenda-se encerrar e travar o lucro."
    elif (minuto_atual >= minuto_limite and minuto_limite > 0) or roi <= -50.0:
        recomendacao = "STOP_LOSS"
        descricao = f"Ponto Crítico de Stop Loss aos {minuto_atual}' (limite {minuto_limite}') ou drawdown de {roi:.1f}%. Encerrar para conter perdas a R$ {pnl:.2f}."
    else:
        recomendacao = "MANTER"
        descricao = f"Operação em andamento ({minuto_atual}'). Drawdown controlado ({roi:.1f}%). Manter posição até gatilho de gol ou {minuto_limite}'."

    return {
        "valido": True,
        "tipo_operacao": tipo_upper,
        "stake_fechamento": stake_fechamento,
        "pnl_liquido": pnl,
        "roi_pct": roi,
        "recomendacao": recomendacao,
        "descricao": descricao
    }


# ==============================================================================
# 2. AVALIADORES DE OPORTUNIDADE EM TEMPO REAL (SEM FABRICAÇÃO DE DADOS)
# ==============================================================================

def avaliar_ltd_trader(match: dict) -> Optional[dict]:
    """
    Método 1: Lay the Draw Trader Clássico
    - Placar: 0 - 0
    - Minuto: 15' a 25' (ou pré-jogo para radar preparatório)
    - Mandante Fav Pré <= 1.45
    - Odd Lay Draw in-play: [3.00, 4.20]
    - Sem Fabricação de Dados: se a odd não estiver no feed, marca status AGUARDANDO_ODD.
    """
    odd_h_pre = match.get("Odd_H_Back") or match.get("Odd_H_FT") or match.get("Odd_H") or 0.0
    try:
        odd_h_pre = float(odd_h_pre)
    except (ValueError, TypeError):
        odd_h_pre = 0.0

    minuto = int(match.get("minuto", 0) or 0)
    gh = match.get("gh", 0)
    ga = match.get("ga", 0)
    placar = str(match.get("placar_live", "0 - 0")).strip()

    # Condição de favoritismo mandante pré-jogo
    if odd_h_pre <= 1.01 or odd_h_pre > 1.45:
        return None

    # Placar deve ser 0x0
    if gh != 0 or ga != 0:
        if placar not in ["0 - 0", "0-0"]:
            return None

    # Se estiver em pré-jogo (minuto 0), emitir sinal preparatório
    is_pre = (minuto == 0)
    is_inplay = (15 <= minuto <= 28)

    if not (is_pre or is_inplay):
        return None

    # Captura da odd real de Lay Draw (sem inventar valor default)
    odd_d_lay_raw = match.get("Odd_D_Lay") or match.get("Odd_Draw_Lay") or match.get("lay_draw")
    odd_d_lay = None
    if odd_d_lay_raw is not None:
        try:
            val = float(odd_d_lay_raw)
            if val > 1.01:
                odd_d_lay = val
        except (ValueError, TypeError):
            odd_d_lay = None

    status_odd = "ODD_DISPONIVEL" if odd_d_lay is not None else "AGUARDANDO_ODD"

    if is_inplay:
        status_tempo = "AO VIVO (Janela Ativa)" if status_odd == "ODD_DISPONIVEL" else "AO VIVO (Aguardando Odd Betfair)"
    else:
        status_tempo = "PREPARAR (Entrada aos 15'-25')"

    market_id = match.get("MarketId") or match.get("market_id") or ""
    link_betfair = f"https://www.betfair.com/exchange/plus/football/market/{market_id}" if market_id else "https://www.betfair.com/exchange/plus/football"

    return {
        "id_metodo": "LTD_TRADER",
        "nome_metodo": "LTD Trader Clássico",
        "icone": "🤝",
        "status_tempo": status_tempo,
        "status_odd": status_odd,
        "mercado": "Match Odds (The Draw)",
        "lado": "LAY",
        "runner": "Empate",
        "faixa_odd_entrada": "3.00 a 4.20",
        "odd_atual": round(odd_d_lay, 2) if odd_d_lay is not None else "AGUARDANDO_ODD",
        "placar_gatilho": "0 - 0",
        "minuto_janela": "15' a 25'",
        "take_profit": "Gol do Favorito (1-0) -> Back Empate @ 7.00+ (+50% a +70% PnL)",
        "stop_loss": "0-0 aos 68' sem pressão -> Cashout @ ~2.00 (-55% PnL) ou se Zebra fizer 0-1",
        "tipo_gestao": "Liability Fixa",
        "risco_sugerido_pct": 5.0,
        "stake": 0.0,
        "tipo_registro": "OBSERVACAO_STAKE_ZERO",
        "link_betfair": link_betfair,
        "observacao": "Sair no gol do favorito. Não segurar até os 90'."
    }


def avaliar_fav_desvantagem(match: dict) -> Optional[dict]:
    """
    Método 2: Swing Trade — Favorito em Desvantagem
    - Placar: 0 - 1 (Mandante atrás)
    - Minuto: 20' a 45'
    - Mandante Fav Pré <= 1.35
    - Odd Back Fav in-play: [2.10, 3.10]
    - Sem Fabricação de Dados: se a odd não estiver no feed, marca status AGUARDANDO_ODD.
    """
    odd_h_pre = match.get("Odd_H_Back") or match.get("Odd_H_FT") or match.get("Odd_H") or 0.0
    try:
        odd_h_pre = float(odd_h_pre)
    except (ValueError, TypeError):
        odd_h_pre = 0.0

    minuto = int(match.get("minuto", 0) or 0)
    gh = match.get("gh", -1)
    ga = match.get("ga", -1)
    placar = str(match.get("placar_live", "")).strip()

    # Super favorito mandante pré-jogo
    if odd_h_pre <= 1.01 or odd_h_pre > 1.35:
        return None

    # Placar deve ser 0x1
    is_triggered = (gh == 0 and ga == 1) or ("0 - 1" in placar) or ("0-1" in placar)
    is_inplay = (20 <= minuto <= 48) and is_triggered

    if not is_inplay:
        return None

    # Captura da odd real in-play do mandante
    odd_h_inplay_raw = match.get("Odd_H_InPlay") or match.get("Odd_H_Back_InPlay") or match.get("Odd_H_Back")
    odd_h_inplay = None
    if odd_h_inplay_raw is not None:
        try:
            val = float(odd_h_inplay_raw)
            if val > 1.01:
                odd_h_inplay = val
        except (ValueError, TypeError):
            odd_h_inplay = None

    status_odd = "ODD_DISPONIVEL" if odd_h_inplay is not None else "AGUARDANDO_ODD"
    status_tempo = "GATILHO ATIVO (0-1 In-Play)" if status_odd == "ODD_DISPONIVEL" else "GATILHO ATIVO (Aguardando Odd)"

    market_id = match.get("MarketId") or match.get("market_id") or ""
    link_betfair = f"https://www.betfair.com/exchange/plus/football/market/{market_id}" if market_id else "https://www.betfair.com/exchange/plus/football"

    return {
        "id_metodo": "FAV_DESVANTAGEM",
        "nome_metodo": "Swing Trade: Fav em Desvantagem",
        "icone": "🔥",
        "status_tempo": status_tempo,
        "status_odd": status_odd,
        "mercado": "Match Odds (Mandante)",
        "lado": "BACK",
        "runner": match.get("Home", "Mandante Favorito"),
        "faixa_odd_entrada": "2.10 a 3.10",
        "odd_atual": round(odd_h_inplay, 2) if odd_h_inplay is not None else "AGUARDANDO_ODD",
        "placar_gatilho": "0 - 1",
        "minuto_janela": "20' a 45'",
        "take_profit": "Empate (1-1) -> Cashout Lay Mandante @ ~1.50 (+40% a +60% PnL) ou Freebet",
        "stop_loss": "Minuto 70' se persistir 0-1 e pressão esfriar, ou stop imediato em 0-2",
        "tipo_gestao": "Stake Fixa",
        "risco_sugerido_pct": 5.0,
        "stake": 0.0,
        "tipo_registro": "OBSERVACAO_STAKE_ZERO",
        "link_betfair": link_betfair,
        "observacao": "Excelente relação Risco x Retorno. Fechar na igualdade sem precisar da virada."
    }


def avaliar_scalping_under(match: dict) -> Optional[dict]:
    """
    Método 3: Scalping de Janela Morta (Under 1.5 HT ou 2.5 FT)
    - Janela A: 33' a 38' HT (0-0) -> Under 1.5 HT
    - Janela B: 55' a 62' FT (0-0 ou 1 gol) -> Under 2.5 FT
    - Sem Fabricação de Dados: se a odd não estiver no feed, marca status AGUARDANDO_ODD.
    """
    minuto = int(match.get("minuto", 0) or 0)
    gh = match.get("gh", 0)
    ga = match.get("ga", 0)
    tot_g = gh + ga if (gh >= 0 and ga >= 0) else 0

    janela_ht = (33 <= minuto <= 38) and (tot_g == 0)
    janela_ft = (55 <= minuto <= 62) and (tot_g <= 1)

    if not (janela_ht or janela_ft):
        return None

    mercado = "Under 1.5 HT" if janela_ht else "Under 2.5 FT"

    # Captura da odd real do mercado under correspondente
    odd_under_raw = match.get("Odd_Under_InPlay") or match.get("Odd_Under15_HT_Back" if janela_ht else "Odd_Under25_FT_Back")
    odd_under = None
    if odd_under_raw is not None:
        try:
            val = float(odd_under_raw)
            if val > 1.01:
                odd_under = val
        except (ValueError, TypeError):
            odd_under = None

    status_odd = "ODD_DISPONIVEL" if odd_under is not None else "AGUARDANDO_ODD"
    status_tempo = f"OPERAÇÃO RÁPIDA ({minuto}')" if status_odd == "ODD_DISPONIVEL" else f"OPERAÇÃO RÁPIDA ({minuto}' - Aguardando Odd)"

    market_id = match.get("MarketId") or match.get("market_id") or ""
    link_betfair = f"https://www.betfair.com/exchange/plus/football/market/{market_id}" if market_id else "https://www.betfair.com/exchange/plus/football"

    return {
        "id_metodo": "SCALPING_UNDER",
        "nome_metodo": "Scalping de Janela Morta",
        "icone": "⏱️",
        "status_tempo": status_tempo,
        "status_odd": status_odd,
        "mercado": mercado,
        "lado": "BACK",
        "runner": mercado,
        "faixa_odd_entrada": "1.30 a 1.65",
        "odd_atual": round(odd_under, 2) if odd_under is not None else "AGUARDANDO_ODD",
        "placar_gatilho": f"{gh} - {ga}",
        "minuto_janela": "33'-38' HT ou 55'-62' FT",
        "take_profit": "Permanecer 5 a 8 minutos (queda 4-6 ticks) -> Lay Under (+8% a +15% PnL)",
        "stop_loss": "Se sair gol durante a janela -> Fechar imediatamente no repique",
        "tipo_gestao": "Stake Fixa",
        "risco_sugerido_pct": 3.0,
        "stake": 0.0,
        "tipo_registro": "OBSERVACAO_STAKE_ZERO",
        "link_betfair": link_betfair,
        "observacao": "Operação cirúrgica. Não prolongar além da janela estipulada."
    }


def avaliar_late_goal_trader(match: dict) -> Optional[dict]:
    """
    Método 4: Late Goal Trader (Over Limite 78'-84')
    - Diferença de 1 gol (1-0, 0-1, 2-1, 1-2) ou empate com fav buscando vitória
    - Minuto: 78' a 84'
    - Odd Over Limite: [1.80, 2.50]
    - Sem Fabricação de Dados: se a odd não estiver no feed, marca status AGUARDANDO_ODD.
    """
    minuto = int(match.get("minuto", 0) or 0)
    gh = match.get("gh", -1)
    ga = match.get("ga", -1)
    tot_g = gh + ga if (gh >= 0 and ga >= 0) else 0
    diff = abs(gh - ga) if (gh >= 0 and ga >= 0) else -1

    if not (78 <= minuto <= 85):
        return None

    odd_h_pre = match.get("Odd_H_Back") or match.get("Odd_H_FT") or match.get("Odd_H") or 2.0
    try:
        odd_h_pre = float(odd_h_pre)
    except (ValueError, TypeError):
        odd_h_pre = 2.0

    is_diff_1 = (diff == 1)
    is_draw_fav = (diff == 0 and odd_h_pre <= 1.45)

    if not (is_diff_1 or is_draw_fav):
        return None

    linha_over = tot_g + 0.5
    odd_over_raw = match.get("Odd_Over_InPlay") or match.get("Odd_OverLimite_Back")
    odd_over = None
    if odd_over_raw is not None:
        try:
            val = float(odd_over_raw)
            if val > 1.01:
                odd_over = val
        except (ValueError, TypeError):
            odd_over = None

    status_odd = "ODD_DISPONIVEL" if odd_over is not None else "AGUARDANDO_ODD"
    status_tempo = f"PRESSÃO FINAL ({minuto}')" if status_odd == "ODD_DISPONIVEL" else f"PRESSÃO FINAL ({minuto}' - Aguardando Odd)"

    market_id = match.get("MarketId") or match.get("market_id") or ""
    link_betfair = f"https://www.betfair.com/exchange/plus/football/market/{market_id}" if market_id else "https://www.betfair.com/exchange/plus/football"

    return {
        "id_metodo": "LATE_GOAL",
        "nome_metodo": "Late Goal Trader (Over Limite)",
        "icone": "⚡",
        "status_tempo": status_tempo,
        "status_odd": status_odd,
        "mercado": f"Over {linha_over:.1f} FT",
        "lado": "BACK",
        "runner": f"Mais de {linha_over:.1f} Gols",
        "faixa_odd_entrada": "1.80 a 2.50",
        "odd_atual": round(odd_over, 2) if odd_over is not None else "AGUARDANDO_ODD",
        "placar_gatilho": f"{gh} - {ga}",
        "minuto_janela": "78' a 84'",
        "take_profit": "Gol nos minutos finais -> 100% Green Automático (sem necessidade de cashout)",
        "stop_loss": "Apito final sem gols (perda da stake de 1u)",
        "tipo_gestao": "Stake Fixa (1u)",
        "risco_sugerido_pct": 5.0,
        "stake": 0.0,
        "tipo_registro": "OBSERVACAO_STAKE_ZERO",
        "link_betfair": link_betfair,
        "observacao": "Jogo totalmente quebrado taticamente. Explora contra-ataques e desespero."
    }


# ==============================================================================
# 3. VARREDURA CONSOLIDADA DE JOGOS
# ==============================================================================

def escanear_oportunidades_trader(df_jogos: pd.DataFrame, mapa_telemetria: dict = None) -> List[Dict[str, Any]]:
    """
    Varre um conjunto de partidas (do dia ou ao vivo) e devolve a lista de
    todas as oportunidades trader elegíveis pelos 4 métodos.
    """
    if df_jogos is None or df_jogos.empty:
        return []

    mapa_telemetria = mapa_telemetria or {}
    oportunidades = []

    for _, row in df_jogos.iterrows():
        m_dict = row.to_dict()
        home = str(m_dict.get("Home", "")).strip()
        away = str(m_dict.get("Away", "")).strip()
        date_str = str(m_dict.get("Date", ""))[:10]

        # Cruzar com telemetria se disponível
        tele_key = f"{date_str}_{_canon(home)}_{_canon(away)}"
        tele = mapa_telemetria.get(tele_key, {})

        minuto = tele.get("min_decorrido", m_dict.get("minuto", 0))
        gh = tele.get("gh", m_dict.get("gh", 0))
        ga = tele.get("ga", m_dict.get("ga", 0))
        placar = tele.get("placar_live", m_dict.get("placar_live", "0 - 0"))

        m_dict["minuto"] = minuto
        m_dict["gh"] = gh
        m_dict["ga"] = ga
        m_dict["placar_live"] = placar

        # Testar cada um dos 4 avaliadores
        for avaliador in [avaliar_ltd_trader, avaliar_fav_desvantagem, avaliar_scalping_under, avaliar_late_goal_trader]:
            res = avaliador(m_dict)
            if res:
                res["jogo"] = f"{home} x {away}"
                res["home"] = home
                res["away"] = away
                res["data"] = date_str
                res["hora"] = str(m_dict.get("Time", ""))
                res["liga"] = str(m_dict.get("League", ""))
                res["placar_atual"] = placar
                res["minuto_atual"] = f"{minuto}'" if minuto > 0 else "Pré-Jogo"
                oportunidades.append(res)

    return oportunidades
