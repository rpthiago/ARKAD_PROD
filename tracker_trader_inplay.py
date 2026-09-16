#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tracker_trader_inplay.py — Daemon Coletor e Logger In-Play dos 4 Métodos Trader.

Autoridade: PREREGISTRO_SUITE_TRADER_INPLAY.md & GEMINI.md
Status: OBSERVACAO_STAKE_ZERO (stake: 0.0)

Registra em tempo real todas as entradas e execuções dos 4 Métodos Trader:
  1. LTD Trader Clássico (15'-25' 0-0)
  2. Swing Trade: Fav em Desvantagem (20'-45' 0-1)
  3. Scalping de Janela Morta (33'-38' HT ou 55'-62' FT)
  4. Late Goal Trader (78'-84' diff 1 gol)

Uso:
  python tracker_trader_inplay.py --once        # Executa uma única varredura e encerra
  python tracker_trader_inplay.py --loop 60     # Executa continuamente a cada 60s
  python tracker_trader_inplay.py --settle      # Liquida partidas finalizadas no log
"""

import os
import sys
import csv
import json
import time
import argparse
import unicodedata
import re
from datetime import datetime, timezone, date
from pathlib import Path
import pandas as pd
import numpy as np

AQUI = Path(__file__).resolve().parent
if str(AQUI) not in sys.path:
    sys.path.insert(0, str(AQUI))

from trader_inplay_engine import (
    avaliar_ltd_trader,
    avaliar_fav_desvantagem,
    avaliar_scalping_under,
    avaliar_late_goal_trader,
    calcular_cashout_ltd,
    calcular_cashout_back,
    escanear_oportunidades_trader,
    _canon
)

LOG_CSV = AQUI / "trader_inplay_log.csv"

COLS_LOG = [
    "timestamp_utc", "data", "hora", "home", "away", "liga",
    "id_metodo", "nome_metodo", "mercado", "lado", "runner",
    "minuto_entrada", "placar_entrada", "odd_entrada", "status_odd",
    "faixa_alvo", "take_profit_regra", "stop_loss_regra",
    "stake", "tipo_registro", "status",
    "minuto_saida", "placar_saida", "odd_saida", "pnl_bruto", "pnl_liquido",
    "roi_pct", "resultado", "motivo_saida"
]


def inicializar_log():
    """Garante que o arquivo CSV de log exista com os cabeçalhos corretos."""
    if not LOG_CSV.exists():
        with open(LOG_CSV, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(COLS_LOG)


def carregar_chaves_registradas() -> set:
    """Carrega chaves únicas de sinais já registrados para evitar duplicidade."""
    if not LOG_CSV.exists():
        return set()
    try:
        df = pd.read_csv(LOG_CSV)
        chaves = set()
        for _, r in df.iterrows():
            k = f"{str(r.get('data',''))[:10]}_{_canon(str(r.get('home','')))}_{_canon(str(r.get('away','')))}_{r.get('id_metodo','')}"
            chaves.add(k)
        return chaves
    except Exception:
        return set()


def obter_dados_inplay(data_str: str):
    """
    Obtém dataframe do dia e mapa de telemetria ao vivo.
    """
    df_jogos = None
    try:
        from futpythontrader_client import get_daily_dataframe
        df_jogos = get_daily_dataframe(source="betfair", date_str=data_str)
    except Exception:
        df_jogos = None

    if df_jogos is None or df_jogos.empty:
        try:
            import b365_data_utils
            games_list = b365_data_utils.fetch_betfair_daily(data_str)
            if games_list:
                df_jogos = pd.DataFrame(games_list)
        except Exception:
            df_jogos = None

    mapa_live = {}
    try:
        from inplay_telemetry_engine import InPlayTelemetryEngine
        te = InPlayTelemetryEngine()
        mapa_live = te._mapa_live
    except Exception:
        mapa_live = {}

    return df_jogos, mapa_live


def executar_varredura():
    """Executa uma rodada completa de escaneamento in-play e salva novos sinais."""
    inicializar_log()
    chaves_vistas = carregar_chaves_registradas()
    hoje_str = date.today().strftime("%Y-%m-%d")

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Escaneando sinais in-play para {hoje_str}...")
    df_jogos, mapa_live = obter_dados_inplay(hoje_str)

    if df_jogos is None or df_jogos.empty:
        print("  - Nenhum jogo retornado no feed para esta data.")
        return 0

    oportunidades = escanear_oportunidades_trader(df_jogos, mapa_live)
    novos_sinais = 0

    with open(LOG_CSV, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for op in oportunidades:
            # Apenas jogos ao vivo ou em janela ativa
            if "AO VIVO" not in op.get("status_tempo", "") and "GATILHO" not in op.get("status_tempo", "") and "OPERAÇÃO" not in op.get("status_tempo", "") and "PRESSÃO" not in op.get("status_tempo", ""):
                continue

            data_jogo = str(op.get("data", hoje_str))[:10]
            chave = f"{data_jogo}_{_canon(op['home'])}_{_canon(op['away'])}_{op['id_metodo']}"

            if chave in chaves_vistas:
                continue

            ts_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            row = [
                ts_utc,
                data_jogo,
                op.get("hora", ""),
                op.get("home", ""),
                op.get("away", ""),
                op.get("liga", ""),
                op.get("id_metodo", ""),
                op.get("nome_metodo", ""),
                op.get("mercado", ""),
                op.get("lado", ""),
                op.get("runner", ""),
                op.get("minuto_atual", ""),
                op.get("placar_atual", ""),
                op.get("odd_atual", ""),
                op.get("status_odd", ""),
                op.get("faixa_odd_entrada", ""),
                op.get("take_profit", ""),
                op.get("stop_loss", ""),
                0.0,  # stake: 0.0 obrigatória GEMINI.md
                "OBSERVACAO_STAKE_ZERO",
                "PENDENTE",  # status
                "", "", "", 0.0, 0.0, 0.0, "EM_ABERTO", ""
            ]
            writer.writerow(row)
            chaves_vistas.add(chave)
            novos_sinais += 1
            print(f"  🔥 SINAL REGISTRADO: [{op['nome_metodo']}] {op['jogo']} @ {op.get('minuto_atual')} ({op.get('placar_atual')}) Odd: {op.get('odd_atual')}")

    print(f"  -> Total de novos sinais registrados: {novos_sinais}")
    return novos_sinais


def liquidar_sinais():
    """Liquida sinais pendentes com placares consolidados."""
    if not LOG_CSV.exists():
        print("Log inexistente para liquidar.")
        return

    df = pd.read_csv(LOG_CSV)
    if df.empty or "status" not in df.columns:
        return

    pendentes = df[df["status"] == "PENDENTE"]
    if pendentes.empty:
        print("Nenhum sinal pendente para liquidar.")
        return

    print(f"Verificando liquidação de {len(pendentes)} sinais pendentes...")
    # Telemetria para placares finais
    mapa_live = {}
    try:
        from inplay_telemetry_engine import InPlayTelemetryEngine
        te = InPlayTelemetryEngine()
        mapa_live = te._mapa_live
    except Exception:
        pass

    atualizados = 0
    for idx, r in pendentes.iterrows():
        dt = str(r["data"])[:10]
        h = str(r["home"])
        a = str(r["away"])
        m_key = f"{dt}_{_canon(h)}_{_canon(a)}"
        tele = mapa_live.get(m_key)

        if tele and tele.get("placar_final") and tele.get("placar_final") != "N/A":
            pf = tele["placar_final"]
            metodo = r["id_metodo"]

            # Exemplo de settlement básico
            df.at[idx, "placar_saida"] = pf
            df.at[idx, "status"] = "LIQUIDADO"
            atualizados += 1

    if atualizados > 0:
        df.to_csv(LOG_CSV, index=False, encoding="utf-8")
        print(f"✅ {atualizados} sinais liquidados com sucesso!")
    else:
        print("Partidas ainda em andamento ou aguardando apito final.")


def main():
    parser = argparse.ArgumentParser(description="Daemon Coletor In-Play dos 4 Métodos Trader ARKAD")
    parser.add_argument("--once", action="store_true", help="Executa uma única varredura e sai")
    parser.add_argument("--loop", type=int, default=0, help="Executa em loop a cada N segundos")
    parser.add_argument("--settle", action="store_true", help="Liquida posições pendentes")
    args = parser.parse_args()

    if args.settle:
        liquidar_sinais()
        return

    if args.once or args.loop == 0:
        executar_varredura()
        return

    print(f"Iniciando Daemon In-Play (intervalo {args.loop}s)... Pressione Ctrl+C para encerrar.")
    try:
        while True:
            executar_varredura()
            time.sleep(args.loop)
    except KeyboardInterrupt:
        print("\nDaemon finalizado pelo operador.")


if __name__ == "__main__":
    main()
