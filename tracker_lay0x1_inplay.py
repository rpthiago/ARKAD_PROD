# -*- coding: utf-8 -*-
"""
tracker_lay0x1_inplay.py — Rastreador e Logger de Paper Trading In-Play Lay 0x1
=============================================================================
STATUS: OBSERVACAO_STAKE_ZERO (stake: 0.0) — GEMINI.md Lei 10.
MÉTODO: Rota C — Lay 0x1 In-Play (Minuto 55-75 em 0x0).

Executa:
1. Varredura dos jogos em andamento (via telemetria do coletor / API Betfair).
2. Filtragem estrita pelas regras de estrategia_lay_0x1_inplay.py.
3. Registro append-only desacoplado em paper_trading_lay0x1_inplay.csv.
4. Liquidação pós-jogo automática pelo placar FT oficial.
"""

import os
import sys
import argparse
from datetime import datetime
from pathlib import Path
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from estrategia_lay_0x1_inplay import (
    validar_entrada_lay0x1_inplay,
    liquidar_resultado_lay0x1,
    calcular_break_even,
    MINUTO_INPLAY_MIN,
    MINUTO_INPLAY_MAX,
    ODD_LAY_0X1_MIN,
    ODD_LAY_0X1_MAX
)
from inplay_telemetry_engine import InPlayTelemetryEngine, _canon

LOG_CSV = ROOT / "paper_trading_lay0x1_inplay.csv"
COLS_LOG = [
    "Timestamp", "Data", "Hora", "Liga", "Jogo", "Home", "Away",
    "Minuto", "Placar_Entrada", "Odd_CS_0x1_Lay", "Odd_H_Back_Pre",
    "Break_Even_WR", "Score_Pressao", "Detalhes_Pressao",
    "Stake_Nominal", "Stake_Real", "Liability_Nominal",
    "Placar_FT", "Resultado", "1/0", "PnL_u", "PnL_R$", "Status"
]


def inicializar_log():
    if not LOG_CSV.exists():
        df_init = pd.DataFrame(columns=COLS_LOG)
        df_init.to_csv(LOG_CSV, index=False, encoding="utf-8-sig")


def escanear_jogos_inplay(verbose: bool = True):
    """Varre partidas ativas no cache de telemetria e identifica entradas aprovadas."""
    inicializar_log()
    engine = InPlayTelemetryEngine()
    engine.recarregar_telemetria()
    
    if not engine.cache_file.exists():
        if verbose:
            print("[-] Cache de telemetria não encontrado.")
        return []
        
    df_ticks = pd.read_csv(engine.cache_file)
    if df_ticks.empty:
        return []
        
    df_ticks['min_to_ko'] = pd.to_numeric(df_ticks.get('min_to_ko'), errors='coerce')
    df_ticks['lay'] = pd.to_numeric(df_ticks.get('lay'), errors='coerce')
    
    # Jogos com dados recentes (min_to_ko entre -50 e -85)
    inplay_ticks = df_ticks[(df_ticks['min_to_ko'] <= -MINUTO_INPLAY_MIN) & (df_ticks['min_to_ko'] >= -MINUTO_INPLAY_MAX)].copy()
    if inplay_ticks.empty:
        if verbose:
            print("[*] Nenhuma partida na janela de minuto 55' a 75' no momento.")
        return []

    # Carregar log existente para evitar duplicar sinal no mesmo jogo
    df_log = pd.read_csv(LOG_CSV)
    chaves_registradas = set()
    if not df_log.empty:
        chaves_registradas = set(df_log['Data'].astype(str) + "_" + df_log['Home'].map(_canon) + "_" + df_log['Away'].map(_canon))
        
    novos_sinais = []
    for (ko, home, away), g in inplay_ticks.groupby(['ko', 'home', 'away']):
        d_str = str(ko)[:10]
        chk_key = f"{d_str}_{_canon(home)}_{_canon(away)}"
        if chk_key in chaves_registradas:
            continue
            
        g_sorted = g.sort_values('min_to_ko')
        min_decorrido = abs(int(g_sorted['min_to_ko'].iloc[-1]))
        
        # Placar atual estimado pelo runner CS mais provável
        latest = g_sorted.tail(15).dropna(subset=['lay'])
        if latest.empty:
            continue
            
        cur_runner = latest.loc[latest['lay'].idxmin()]['runner']
        gh_cur, ga_cur = 0, 0
        if ' - ' in str(cur_runner):
            partes = str(cur_runner).split(' - ')
            gh_cur, ga_cur = int(partes[0]), int(partes[1])
            
        # Odd Lay 0x1 atual
        ticks_0x1 = g_sorted[g_sorted['runner'] == '0 - 1'].dropna(subset=['lay'])
        if ticks_0x1.empty:
            continue
        odd_lay_0x1 = float(ticks_0x1['lay'].iloc[-1])
        
        # Validação
        aprovado, motivo, fin = validar_entrada_lay0x1_inplay(
            minuto=min_decorrido,
            gols_h_atual=gh_cur,
            gols_a_atual=ga_cur,
            odd_lay_0x1=odd_lay_0x1
        )
        
        if aprovado:
            hora_str = str(ko)[11:16] if len(str(ko)) >= 16 else "15:00"
            sinal = {
                "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "Data": d_str,
                "Hora": hora_str,
                "Liga": "In-Play Telemetry",
                "Jogo": f"{home} x {away}",
                "Home": home,
                "Away": away,
                "Minuto": min_decorrido,
                "Placar_Entrada": f"{gh_cur}x{ga_cur}",
                "Odd_CS_0x1_Lay": odd_lay_0x1,
                "Odd_H_Back_Pre": np.nan,
                "Break_Even_WR": fin.get("break_even_wr", calcular_break_even(odd_lay_0x1)),
                "Score_Pressao": fin.get("score_pressao", 0),
                "Detalhes_Pressao": fin.get("detalhes_pressao", "0x0 aos " + str(min_decorrido) + "'"),
                "Stake_Nominal": 100.0,
                "Stake_Real": 0.0,  # GEMINI.md Lei 10: Stake zero em observação
                "Liability_Nominal": fin.get("liability", round(100.0 * (odd_lay_0x1 - 1.0), 2)),
                "Placar_FT": np.nan,
                "Resultado": "PENDENTE",
                "1/0": np.nan,
                "PnL_u": 0.0,
                "PnL_R$": 0.0,
                "Status": "OBSERVACAO_STAKE_ZERO"
            }
            novos_sinais.append(sinal)
            if verbose:
                print(f"[+] SINAL IN-PLAY DETECTADO: {home} x {away} aos {min_decorrido}' | Odd Lay 0x1: {odd_lay_0x1:.2f} | BE: {sinal['Break_Even_WR']:.1f}%")

    if novos_sinais:
        df_novos = pd.DataFrame(novos_sinais)
        df_novos.to_csv(LOG_CSV, mode="a", header=False, index=False, encoding="utf-8-sig")
        if verbose:
            print(f"[+] {len(novos_sinais)} novos sinais gravados em {LOG_CSV.name}")
            
    return novos_sinais


def liquidar_paper_trading(verbose: bool = True):
    """Liquida sinais pendentes com base nos placares finais capturados."""
    if not LOG_CSV.exists():
        return
    df_log = pd.read_csv(LOG_CSV)
    if df_log.empty:
        return
        
    engine = InPlayTelemetryEngine()
    engine.recarregar_telemetria()
    
    n_liq = 0
    for idx, r in df_log.iterrows():
        if r.get("Resultado") in ["GREEN", "RED"]:
            continue
            
        d_str = str(r["Data"])
        home = str(r["Home"])
        away = str(r["Away"])
        odd_lay = float(r["Odd_CS_0x1_Lay"])
        
        # Buscar placar FT
        tele = engine.consultar_jogo(d_str, home, away)
        final_sc = tele.get("final_score")
        if final_sc and final_sc != "N/A" and " - " in str(final_sc):
            p = str(final_sc).split(" - ")
            gh, ga = int(p[0]), int(p[1])
            res = liquidar_resultado_lay0x1(gh, ga, odd_lay, stake=100.0)
            
            df_log.at[idx, "Placar_FT"] = f"{gh}x{ga}"
            df_log.at[idx, "Resultado"] = res["resultado"]
            df_log.at[idx, "1/0"] = res["1/0"]
            df_log.at[idx, "PnL_u"] = res["pnl_u"]
            df_log.at[idx, "PnL_R$"] = res["pnl_r"]
            df_log.at[idx, "Status"] = f"LIQUIDADO_{res['resultado']}"
            n_liq += 1
            
    if n_liq > 0:
        df_log.to_csv(LOG_CSV, index=False, encoding="utf-8-sig")
        if verbose:
            print(f"[+] {n_liq} apostas in-play liquidadas com sucesso em {LOG_CSV.name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tracker In-Play Lay 0x1")
    parser.add_argument("--scan", action="store_true", help="Executar scan ao vivo")
    parser.add_argument("--settle", action="store_true", help="Liquidar apostas pendentes")
    args = parser.parse_args()
    
    if args.settle:
        liquidar_paper_trading(verbose=True)
    else:
        escanear_jogos_inplay(verbose=True)
        liquidar_paper_trading(verbose=True)
