# -*- coding: utf-8 -*-
"""
trader_inplay_engine.py — Motor Analítico e Telemetria In-Play dos 4 Métodos Trader.

Autoridade: PREREGISTRO_SUITE_TRADER_INPLAY.md (Revisão v2) & GEMINI.md (Leis 1, 2, 3)
Status: OBSERVACAO_STAKE_ZERO (stake: 0.0)

Especificação:
1. Fonte de Dados Canônica: /home/ubuntu/betfair-collector/betfair_live_odds.csv
2. Minuto Canônico: minuto = int(-min_to_ko - 15)
3. Estado do Jogo Factual: Linhas O/U batidas (gols_por_ou), NUNCA pelo menor lay de CS.
4. Entrada e Saída: 1ª captura elegível da janela / 1ª captura após o evento.
5. Matemática de Cashout: Comissão de 5% sobre lucros reais.
"""

from typing import Optional, Union, Dict, Any, List, Tuple
import math
import unicodedata
import re
import pandas as pd
import numpy as np

LINHAS_OU = {
    "OVER_UNDER_05": 0.5,
    "OVER_UNDER_15": 1.5,
    "OVER_UNDER_25": 2.5,
    "OVER_UNDER_35": 3.5
}


def _canon(s: str) -> str:
    if not isinstance(s, str) or not s:
        return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)


# ==============================================================================
# 1. ESTADO DO JOGO FACTUAL: GOLS POR LINHAS OVER/UNDER BATIDAS
# ==============================================================================

def gols_por_ou(df_ou_match: pd.DataFrame) -> Dict[str, Optional[int]]:
    """
    Determina os gols já saídos por captura (capture_ts) pelas linhas Over/Under batidas (fato, não previsão).
    Proibido usar o runner de menor lay do Correct Score como placar atual (ele erra 72.5% aos 10-25').
    
    Regra por linha L (0.5..3.5) numa captura ts:
      - Batida: 'Over L' está a <= 1.02, OU o mercado da linha sumiu e não volta em nenhuma captura posterior.
      - Não batida: presente com back > 1.02.
    Gols exato: quando max(batidos) + 0.5 == min(não batidos) - 0.5. Senão None (indefinido).
    """
    if df_ou_match.empty:
        return {}

    o = df_ou_match[df_ou_match["runner"].str.startswith("Over", na=False)].copy()
    if "market_type" in o.columns:
        o["L"] = o["market_type"].map(LINHAS_OU)
    elif "mtype" in o.columns:
        o["L"] = o["mtype"].map(LINHAS_OU)
    else:
        return {}

    o = o.dropna(subset=["L"])
    o = o.sort_values("minuto")
    ult_min_por_linha = o.groupby("L")["minuto"].max().to_dict()

    resultado_ts = {}
    for ts, cap in o.groupby("capture_ts"):
        mn = float(cap["minuto"].iloc[0])
        pres = {float(r.L): float(r.back) for r in cap.itertuples() if pd.notna(r.back)}
        batidos, nao_batidos = [], []

        for L in LINHAS_OU.values():
            if L in pres:
                if pres[L] <= 1.02:
                    batidos.append(L)
                else:
                    nao_batidos.append(L)
            elif L in ult_min_por_linha and ult_min_por_linha[L] < mn:
                batidos.append(L)  # sumiu antes desta captura e não volta

        lo_ = (max(batidos) + 0.5) if batidos else 0.0
        hi_ = (min(nao_batidos) - 0.5) if nao_batidos else None

        if hi_ is not None and lo_ == hi_:
            resultado_ts[ts] = int(lo_)
        elif hi_ is None and batidos and max(batidos) == 3.5:
            resultado_ts[ts] = 4  # 4 ou mais gols
        else:
            resultado_ts[ts] = None  # Indefinido: não assumir default

    return resultado_ts


# ==============================================================================
# 2. MATEMÁTICA REAL DE CASHOUT & HEDGING (BETFAIR COMMISSION = 5%)
# ==============================================================================

def calcular_cashout_ltd(odd_lay_entrada: float, odd_draw_atual: float, liability: float, commission: float = 0.05) -> dict:
    """
    Calcula o cashout de uma posição aberta em Lay the Draw (LTD Trader).
    - odd_lay_entrada: Odd de entrada em Lay Draw.
    - odd_draw_atual: Odd atual de Back no Empate.
    - liability: Capital em risco em R$.
    - commission: Comissão Betfair (padrão 5%).
    """
    if odd_lay_entrada <= 1.01 or odd_draw_atual <= 1.01 or liability <= 0:
        return {"valido": False, "stake_fechamento": 0.0, "pnl_liquido": 0.0, "roi_pct": 0.0, "tipo": "ERRO"}

    # Stake nominal correspondente à liability do Lay
    stake_in = liability / (odd_lay_entrada - 1.0)
    # Stake necessária no Back para equilibrar retorno
    stake_out = (stake_in * odd_lay_entrada) / odd_draw_atual
    pnl_bruto = stake_in - stake_out

    if pnl_bruto > 0:
        pnl_liquido = pnl_bruto * (1.0 - commission)
        tipo = "GREEN"
    else:
        pnl_liquido = pnl_bruto
        tipo = "RED" if pnl_bruto < -0.01 else "BREAKEVEN"

    roi_pct = (pnl_liquido / liability) * 100.0

    return {
        "valido": True,
        "stake_in": round(stake_in, 2),
        "stake_fechamento": round(stake_out, 2),
        "pnl_bruto": round(pnl_bruto, 2),
        "pnl_liquido": round(pnl_liquido, 2),
        "roi_pct": round(roi_pct, 2),
        "tipo": tipo,
        "descricao": f"Back de R$ {stake_out:.2f} @ {odd_draw_atual:.2f} -> {tipo} de R$ {pnl_liquido:.2f} ({roi_pct:+.1f}%)"
    }


def calcular_cashout_back(odd_back_in: float, odd_lay_out: float, stake_in: float, commission: float = 0.05) -> dict:
    """
    Calcula o cashout de uma posição aberta em BACK (ex: Fav em Desvantagem ou Scalping Under).
    """
    if odd_back_in <= 1.01 or odd_lay_out <= 1.01 or stake_in <= 0:
        return {"valido": False, "stake_fechamento": 0.0, "pnl_liquido": 0.0, "roi_pct": 0.0, "tipo": "ERRO"}

    stake_out = (stake_in * odd_back_in) / odd_lay_out
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
        "descricao": f"Lay de R$ {stake_out:.2f} @ {odd_lay_out:.2f} -> {tipo} de R$ {pnl_liquido:.2f} ({roi_pct:+.1f}%)"
    }


def calcular_freebet_back(odd_back_in: float, odd_lay_out: float, stake_in: float, commission: float = 0.05) -> dict:
    """Calcula a Freebet (Stop-at-Zero) para posição Back."""
    if odd_back_in <= 1.01 or odd_lay_out <= 1.01 or stake_in <= 0:
        return {"valido": False, "stake_fechamento": 0.0, "lucro_potencial": 0.0}

    stake_out = stake_in
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
    """Calcula saída por tempo ou stop loss dinâmico."""
    tipo_upper = str(tipo).strip().upper()
    if tipo_upper == "LAY":
        calc = calcular_cashout_ltd(odd_lay_entrada=odd_entrada, odd_draw_atual=odd_atual, liability=liability_ou_stake, commission=commission)
    else:
        calc = calcular_cashout_back(odd_back_in=odd_entrada, odd_lay_out=odd_atual, stake_in=liability_ou_stake, commission=commission)

    if not calc.get("valido"):
        return {"valido": False, "recomendacao": "ERRO", "stake_fechamento": 0.0, "pnl_liquido": 0.0, "roi_pct": 0.0}

    pnl = calc["pnl_liquido"]
    roi = calc["roi_pct"]
    if roi > 0:
        recomendacao = "CASHOUT_GREEN"
        descricao = f"Take Profit disponível! PnL R$ {pnl:+.2f} ({roi:+.1f}%)."
    elif (minuto_atual >= minuto_limite and minuto_limite > 0) or roi <= -50.0:
        recomendacao = "STOP_LOSS"
        descricao = f"Ponto Crítico de Stop Loss aos {minuto_atual}' (limite {minuto_limite}')."
    else:
        recomendacao = "MANTER"
        descricao = f"Posição em andamento ({minuto_atual}')."

    return {
        "valido": True,
        "stake_fechamento": calc["stake_fechamento"],
        "pnl_liquido": pnl,
        "roi_pct": roi,
        "recomendacao": recomendacao,
        "descricao": descricao
    }


# ==============================================================================
# 3. AVALIADORES DE ENTRADA IN-PLAY (REGRAS CONGELADAS REVISADAS V2)
# ==============================================================================

def avaliar_ltd_trader(match_row: dict) -> Optional[dict]:
    """
    Método 1: LTD Trader Clássico (15'–25' 0-0, Super Fav Mandante <= 1.45)
    Faixa de Lay Draw medida no coletor: [3.50, 10.00] (mediana 7.80).
    """
    minuto = match_row.get("minuto")
    if minuto is None or not (15 <= minuto <= 25):
        return None

    # Estado: 0-0 Factual pelo O/U
    tot_gols = match_row.get("tot_gols_ou")
    if tot_gols != 0:
        return None

    fav_pre = match_row.get("fav_pre", 99.0)
    if fav_pre > 1.45:
        return None

    lay_draw = match_row.get("lay_draw")
    if lay_draw is None or not (3.50 <= lay_draw <= 10.00):
        return None

    liq_lay = match_row.get("liq_draw_lay", 0.0)
    if liq_lay < 200.0:
        return None

    spread = match_row.get("spread_draw", 0.0)
    if spread > 0.15:
        return None

    return {
        "id_metodo": "LTD_TRADER",
        "nome_metodo": "LTD Trader Clássico",
        "icone": "🤝",
        "mercado": "Match Odds (The Draw)",
        "lado": "LAY",
        "runner": "Empate",
        "odd_entrada": round(lay_draw, 2),
        "faixa_odd_entrada": "3.50 a 10.00",
        "minuto_entrada": minuto,
        "placar_entrada": "0 - 0",
        "take_profit_regra": "Gol do Fav (1-0) -> Back Empate na 1ª captura pós-gol",
        "stop_loss_regra": "0-0 aos 68' ou gol da zebra (0-1) -> Back Empate imediato",
        "tipo_gestao": "Liability Fixa",
        "stake": 0.0,
        "tipo_registro": "OBSERVACAO_STAKE_ZERO"
    }


def avaliar_fav_desvantagem(match_row: dict) -> Optional[dict]:
    """
    Método 2: Swing Trade — Fav em Desvantagem (20'–45' 0-1, Fav Mandante <= 1.35)
    Faixa de Back Fav medida no coletor: [2.00, 3.20] (mediana 2.40).
    """
    minuto = match_row.get("minuto")
    if minuto is None or not (20 <= minuto <= 45):
        return None

    tot_gols = match_row.get("tot_gols_ou")
    if tot_gols != 1:
        return None

    # Verifica se quem marcou foi a zebra (Mandante perdendo 0-1)
    # Confirmado quando o Mandante está atrás no placar (ou CS ativo é 0-1)
    placar = match_row.get("placar_confirmado", "")
    if "0-1" not in placar and "0 - 1" not in placar:
        return None

    fav_pre = match_row.get("fav_pre", 99.0)
    if fav_pre > 1.35:
        return None

    back_fav = match_row.get("back_fav")
    if back_fav is None or not (2.00 <= back_fav <= 3.20):
        return None

    liq_back = match_row.get("liq_fav_back", 0.0)
    if liq_back < 200.0:
        return None

    spread = match_row.get("spread_fav", 0.0)
    if spread > 0.10:
        return None

    return {
        "id_metodo": "FAV_DESVANTAGEM",
        "nome_metodo": "Swing Trade: Fav em Desvantagem",
        "icone": "🔥",
        "mercado": "Match Odds (Mandante)",
        "lado": "BACK",
        "runner": match_row.get("home", "Mandante"),
        "odd_entrada": round(back_fav, 2),
        "faixa_odd_entrada": "2.00 a 3.20",
        "minuto_entrada": minuto,
        "placar_entrada": "0 - 1",
        "take_profit_regra": "Empate (1-1) -> Lay Mandante na 1ª captura pós-gol",
        "stop_loss_regra": "Minuto 70' se 0-1 ou gol 0-2 -> Lay imediato",
        "tipo_gestao": "Stake Fixa",
        "stake": 0.0,
        "tipo_registro": "OBSERVACAO_STAKE_ZERO"
    }


def avaliar_scalping_under(match_row: dict) -> Optional[dict]:
    """
    Método 3: Scalping de Janela Morta (33'–38' HT ou 55'–62' FT)
    Faixas medidas: Under 1.5 HT [1.35, 2.20]; Under 2.5 FT [1.35, 1.85].
    """
    minuto = match_row.get("minuto")
    if minuto is None:
        return None

    tot_gols = match_row.get("tot_gols_ou")
    if tot_gols is None:
        return None

    janela_ht = (33 <= minuto <= 38) and (tot_gols == 0)
    janela_ft = (55 <= minuto <= 62) and (tot_gols <= 1)

    if not (janela_ht or janela_ft):
        return None

    mercado = "Under 1.5 HT" if janela_ht else "Under 2.5 FT"
    faixa_lo, faixa_hi = (1.35, 2.20) if janela_ht else (1.35, 1.85)

    back_under = match_row.get("back_under")
    if back_under is None or not (faixa_lo <= back_under <= faixa_hi):
        return None

    liq_back = match_row.get("liq_under_back", 0.0)
    if liq_back < 200.0:
        return None

    spread = match_row.get("spread_under", 0.0)
    if spread > 0.06:
        return None

    return {
        "id_metodo": "SCALPING_UNDER",
        "nome_metodo": "Scalping de Janela Morta",
        "icone": "⏱️",
        "mercado": mercado,
        "lado": "BACK",
        "runner": mercado,
        "odd_entrada": round(back_under, 2),
        "faixa_odd_entrada": f"{faixa_lo:.2f} a {faixa_hi:.2f}",
        "minuto_entrada": minuto,
        "placar_entrada": match_row.get("placar_confirmado", f"{tot_gols} gols"),
        "take_profit_regra": "Permanecer 5 a 8 min (queda de ticks) -> Lay Under",
        "stop_loss_regra": "Gol na janela -> Lay Under imediato no repique",
        "tipo_gestao": "Stake Fixa",
        "stake": 0.0,
        "tipo_registro": "OBSERVACAO_STAKE_ZERO"
    }


def avaliar_late_goal_trader(match_row: dict) -> Optional[dict]:
    """
    Método 4: Late Goal Trader (Unificado com Late Goal v2)
    Minuto 82'–86', diff de exatamente 1 gol, Back Over da próxima linha @ [1.50, 2.60].
    """
    minuto = match_row.get("minuto")
    if minuto is None or not (82 <= minuto <= 86):
        return None

    tot_gols = match_row.get("tot_gols_ou")
    if tot_gols is None or tot_gols < 1:
        return None

    diff = match_row.get("diff_gols")
    if diff != 1:
        return None

    linha_over = tot_gols + 0.5
    back_over = match_row.get("back_over")
    if back_over is None or not (1.50 <= back_over <= 2.60):
        return None

    liq_back = match_row.get("liq_over_back", 0.0)
    if liq_back < 200.0:
        return None

    spread = match_row.get("spread_over", 0.0)
    if spread > 0.10:
        return None

    return {
        "id_metodo": "LATE_GOAL",
        "nome_metodo": "Late Goal Trader (Over Limite)",
        "icone": "⚡",
        "mercado": f"Over {linha_over:.1f} FT",
        "lado": "BACK",
        "runner": f"Mais de {linha_over:.1f} Gols",
        "odd_entrada": round(back_over, 2),
        "faixa_odd_entrada": "1.50 a 2.60",
        "minuto_entrada": minuto,
        "placar_entrada": match_row.get("placar_confirmado", f"diff 1 ({tot_gols}g)"),
        "take_profit_regra": "Gol pós-80' -> Green automático (+100% da aposta líquida)",
        "stop_loss_regra": "Apito final sem gols -> Perda de 1u (placares_ft.csv)",
        "tipo_gestao": "Stake Fixa (1u)",
        "stake": 0.0,
        "tipo_registro": "OBSERVACAO_STAKE_ZERO"
    }


# ==============================================================================
# 4. AVALIAÇÃO DE SAÍDA REAL (SEGUNDA PERNA DO TRADE)
# ==============================================================================

def avaliar_saida_trader(sinal_in: dict, capturas_pos: pd.DataFrame, placar_ft: Optional[Tuple[int, int]] = None) -> Optional[dict]:
    """
    Avalia a saída real da operação a partir das capturas posteriores da partida no coletor.
    A odd de saída é a PRIMEIRA captura após o evento com o runner presente no book.
    Nunca estimar odd.
    """
    if capturas_pos.empty:
        return None

    metodo = sinal_in["id_metodo"]
    ts_in = sinal_in["capture_ts_in"]
    odd_in = sinal_in["odd_entrada"]
    minuto_in = sinal_in["minuto_entrada"]

    # Filtra capturas estritamente posteriores
    pos = capturas_pos[capturas_pos["capture_ts"] > ts_in].sort_values("minuto")
    if pos.empty:
        return None

    # M1: LTD TRADER (Entrada Lay Draw @ 0-0)
    if metodo == "LTD_TRADER":
        # Evento Green: total_gols passa de 0 para 1
        # Evento Stop: minuto >= 68 ainda em 0-0
        for ts, cap in pos.groupby("capture_ts"):
            mn = int(cap["minuto"].iloc[0])
            gols = cap.get("tot_gols_ou", 0)

            # Caso A: Saiu gol
            if gols and gols >= 1:
                mo_d = cap[(cap["market_type"] == "MATCH_ODDS") & (cap["runner"] == "The Draw")]
                if not mo_d.empty and pd.notna(mo_d["back"].iloc[0]):
                    odd_out = float(mo_d["back"].iloc[0])
                    pnl_calc = calcular_cashout_ltd(odd_lay_entrada=odd_in, odd_draw_atual=odd_out, liability=100.0)
                    return {
                        "capture_ts_out": ts,
                        "minuto_saida": mn,
                        "odd_saida": odd_out,
                        "placar_saida": cap.get("placar_confirmado", f"{gols} gol(s)"),
                        "pnl_bruto": pnl_calc["pnl_bruto"],
                        "pnl_liquido": pnl_calc["pnl_liquido"],
                        "roi_pct": pnl_calc["roi_pct"],
                        "resultado": pnl_calc["tipo"],
                        "motivo_saida": f"Gol aos {mn}' (total {gols}) -> Cashout Back @ {odd_out:.2f}"
                    }

            # Caso B: Stop loss por tempo aos 68'
            if mn >= 68:
                mo_d = cap[(cap["market_type"] == "MATCH_ODDS") & (cap["runner"] == "The Draw")]
                if not mo_d.empty and pd.notna(mo_d["back"].iloc[0]):
                    odd_out = float(mo_d["back"].iloc[0])
                    pnl_calc = calcular_cashout_ltd(odd_lay_entrada=odd_in, odd_draw_atual=odd_out, liability=100.0)
                    return {
                        "capture_ts_out": ts,
                        "minuto_saida": mn,
                        "odd_saida": odd_out,
                        "placar_saida": "0 - 0",
                        "pnl_bruto": pnl_calc["pnl_bruto"],
                        "pnl_liquido": pnl_calc["pnl_liquido"],
                        "roi_pct": pnl_calc["roi_pct"],
                        "resultado": pnl_calc["tipo"],
                        "motivo_saida": f"Stop loss por tempo aos {mn}' (0-0 persistente) -> Back @ {odd_out:.2f}"
                    }

    # M2: FAV EM DESVANTAGEM (Entrada Back Fav @ 0-1)
    elif metodo == "FAV_DESVANTAGEM":
        home_runner = sinal_in.get("home", "")
        for ts, cap in pos.groupby("capture_ts"):
            mn = int(cap["minuto"].iloc[0])
            gols = cap.get("tot_gols_ou", 1)
            placar = cap.get("placar_confirmado", "")

            # Caso A: Empate 1-1
            if gols >= 2 and ("1-1" in placar or "1 - 1" in placar or cap.get("diff_gols") == 0):
                mo_fav = cap[(cap["market_type"] == "MATCH_ODDS") & (cap["runner"] == home_runner)]
                if not mo_fav.empty and pd.notna(mo_fav["lay"].iloc[0]):
                    odd_out = float(mo_fav["lay"].iloc[0])
                    pnl_calc = calcular_cashout_back(odd_back_in=odd_in, odd_lay_out=odd_out, stake_in=100.0)
                    return {
                        "capture_ts_out": ts,
                        "minuto_saida": mn,
                        "odd_saida": odd_out,
                        "placar_saida": "1 - 1",
                        "pnl_bruto": pnl_calc["pnl_bruto"],
                        "pnl_liquido": pnl_calc["pnl_liquido"],
                        "roi_pct": pnl_calc["roi_pct"],
                        "resultado": pnl_calc["tipo"],
                        "motivo_saida": f"Empate aos {mn}' (1-1) -> Cashout Lay @ {odd_out:.2f}"
                    }

            # Caso B: Stop loss aos 70' ou 0-2
            if mn >= 70 or ("0-2" in placar or "0 - 2" in placar):
                mo_fav = cap[(cap["market_type"] == "MATCH_ODDS") & (cap["runner"] == home_runner)]
                if not mo_fav.empty and pd.notna(mo_fav["lay"].iloc[0]):
                    odd_out = float(mo_fav["lay"].iloc[0])
                    pnl_calc = calcular_cashout_back(odd_back_in=odd_in, odd_lay_out=odd_out, stake_in=100.0)
                    return {
                        "capture_ts_out": ts,
                        "minuto_saida": mn,
                        "odd_saida": odd_out,
                        "placar_saida": placar or f"{gols} gols",
                        "pnl_bruto": pnl_calc["pnl_bruto"],
                        "pnl_liquido": pnl_calc["pnl_liquido"],
                        "roi_pct": pnl_calc["roi_pct"],
                        "resultado": pnl_calc["tipo"],
                        "motivo_saida": f"Stop loss aos {mn}' ({placar}) -> Lay @ {odd_out:.2f}"
                    }

    # M3: SCALPING UNDER (Saída após 5-8 min ou gol)
    elif metodo == "SCALPING_UNDER":
        mkt = "OVER_UNDER_15" if "1.5" in sinal_in["mercado"] else "OVER_UNDER_25"
        for ts, cap in pos.groupby("capture_ts"):
            mn = int(cap["minuto"].iloc[0])
            gols = cap.get("tot_gols_ou", 0)

            # Decorridos 5 a 8 minutos
            if (mn - minuto_in >= 5) or (gols and gols > sinal_in.get("gols_in", 0)):
                u_mkt = cap[(cap["market_type"] == mkt) & (cap["runner"].str.startswith("Under", na=False))]
                if not u_mkt.empty and pd.notna(u_mkt["lay"].iloc[0]):
                    odd_out = float(u_mkt["lay"].iloc[0])
                    pnl_calc = calcular_cashout_back(odd_back_in=odd_in, odd_lay_out=odd_out, stake_in=100.0)
                    motivo = f"Fechamento da janela aos {mn}' (+{mn-minuto_in} min)" if not gols else f"Gol durante scalping aos {mn}'"
                    return {
                        "capture_ts_out": ts,
                        "minuto_saida": mn,
                        "odd_saida": odd_out,
                        "placar_saida": cap.get("placar_confirmado", ""),
                        "pnl_bruto": pnl_calc["pnl_bruto"],
                        "pnl_liquido": pnl_calc["pnl_liquido"],
                        "roi_pct": pnl_calc["roi_pct"],
                        "resultado": pnl_calc["tipo"],
                        "motivo_saida": f"{motivo} -> Lay @ {odd_out:.2f}"
                    }

    # M4: LATE GOAL TRADER (Liquidado pelo placar final / oficial)
    elif metodo == "LATE_GOAL":
        if placar_ft is not None:
            gh_ft, ga_ft = placar_ft
            tot_ft = gh_ft + ga_ft
            # Linha alvo
            tot_in = sinal_in.get("total_gols_in", 1)
            linha_alvo = tot_in + 0.5
            ganhou = (tot_ft > linha_alvo)

            if ganhou:
                pnl_liq = 100.0 * (odd_in - 1.0) * 0.95
                roi = (pnl_liq / 100.0) * 100.0
                tipo = "GREEN"
            else:
                pnl_liq = -100.0
                roi = -100.0
                tipo = "RED"

            return {
                "capture_ts_out": "FT_OFICIAL",
                "minuto_saida": 90,
                "odd_saida": 1.01 if ganhou else 999.0,
                "placar_saida": f"{gh_ft} - {ga_ft}",
                "pnl_bruto": 100.0 * (odd_in - 1.0) if ganhou else -100.0,
                "pnl_liquido": pnl_liq,
                "roi_pct": roi,
                "resultado": tipo,
                "motivo_saida": f"Placar Final FT {gh_ft}-{ga_ft} (Over {linha_alvo:.1f} {'bateu' if ganhou else 'perdeu'})"
            }

    return None
