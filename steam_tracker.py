# -*- coding: utf-8 -*-
"""
steam_tracker.py — Rastreador de Steam Moves e Fluxo Institucional da Betfair (Sharp Money)

Objetivo:
1. Capturar periodicamente snapshots das odds da Betfair ao longo do dia.
2. Identificar quedas bruscas de linha (Steam Moves >= 5% ou >= 10%) entre a abertura do mercado e a linha atual.
3. Rastrear o lado em que o dinheiro institucional (sindicatos europeus) está entrando com liquidez.
4. Exportar relatórios e alimentar a interface Streamlit em tempo real.
"""

import os
import sys
import time
from datetime import datetime, date
from pathlib import Path
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parent
STEAM_DIR = ROOT / "steam_data"
STEAM_DIR.mkdir(exist_ok=True)

try:
    from futpythontrader_client import get_daily_dataframe
except ImportError:
    sys.path.insert(0, str(ROOT))
    from futpythontrader_client import get_daily_dataframe


def get_snapshot_filepath(date_str: str) -> Path:
    clean_date = str(date_str).strip().replace("-", "")
    return STEAM_DIR / f"snapshots_{clean_date}.csv"


def capturar_snapshot(date_str: str = None) -> pd.DataFrame:
    """
    Captura um snapshot instantâneo das odds da Betfair para o dia informado (default=hoje).
    Armazena em steam_data/snapshots_YYYYMMDD.csv com timestamp ISO.
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")

    print(f"[*] [Steam Tracker] Capturando snapshot Betfair para {date_str}...", flush=True)
    df_raw = get_daily_dataframe("betfair", date_str)
    if df_raw is None or df_raw.empty:
        print(f"[!] [Steam Tracker] Nenhum jogo retornado pela API para {date_str}.", flush=True)
        return pd.DataFrame()

    now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Colunas essenciais de identificação e odds
    cols_meta = ["ID_Evento", "Date", "Time", "League", "Home", "Away"]
    cols_odds = [
        "Odd_H_Back", "Odd_D_Back", "Odd_A_Back",
        "Odd_H_Lay", "Odd_D_Lay", "Odd_A_Lay",
        "Odd_Over25_FT_Back", "Odd_Under25_FT_Back",
        "Odd_BTTS_Yes_Back", "Odd_BTTS_No_Back"
    ]
    
    # Selecionar colunas existentes
    cols_to_keep = [c for c in cols_meta if c in df_raw.columns]
    for c in cols_odds:
        if c in df_raw.columns:
            cols_to_keep.append(c)

    df_snap = df_raw[cols_to_keep].copy()
    df_snap["captured_at"] = now_ts

    # Converter numéricos
    for c in cols_odds:
        if c in df_snap.columns:
            df_snap[c] = pd.to_numeric(df_snap[c], errors="coerce")

    # Salvar / anexar no CSV diário
    csv_path = get_snapshot_filepath(date_str)
    if csv_path.exists():
        df_snap.to_csv(csv_path, mode="a", header=False, index=False, encoding="utf-8")
    else:
        df_snap.to_csv(csv_path, index=False, encoding="utf-8")

    print(f"[+] [Steam Tracker] Snapshot salvo com {len(df_snap)} jogos às {now_ts} em {csv_path.name}", flush=True)
    return df_snap


def detectar_steam_moves(date_str: str = None, min_drop_pct: float = 5.0) -> pd.DataFrame:
    """
    Analisa os snapshots gravados para a data e identifica variações de cotação.
    Retorna DataFrame consolidado com:
    - Odd Abertura vs Odd Atual
    - Variação percentual (Drop %)
    - Classificação de intensidade (Mega Steam, Moderado, Drift)
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")

    csv_path = get_snapshot_filepath(date_str)
    if not csv_path.exists():
        print(f"[!] [Steam Tracker] Nenhum histórico de snapshots encontrado para {date_str}.")
        return pd.DataFrame()

    df_all = pd.read_csv(csv_path, low_memory=False)
    if df_all.empty or "captured_at" not in df_all.columns:
        return pd.DataFrame()

    df_all["captured_at"] = pd.to_datetime(df_all["captured_at"], errors="coerce")
    df_all = df_all.sort_values("captured_at")

    # Identificar jogos e timestamps
    timestamps = sorted(df_all["captured_at"].unique())
    num_snaps = len(timestamps)

    # Agrupar por ID_Evento ou (Home, Away)
    id_col = "ID_Evento" if "ID_Evento" in df_all.columns and df_all["ID_Evento"].notna().mean() > 0.5 else "Home"

    results = []
    for group_key, group in df_all.groupby(id_col):
        if len(group) < 1:
            continue

        first_row = group.iloc[0]
        last_row = group.iloc[-1]

        t_first = first_row["captured_at"]
        t_last = last_row["captured_at"]

        # Avaliar mercados principais: Home, Away, Draw, Over 2.5
        markets = [
            ("Home", "Odd_H_Back"),
            ("Away", "Odd_A_Back"),
            ("Draw", "Odd_D_Back"),
            ("Over25", "Odd_Over25_FT_Back")
        ]

        for m_label, m_col in markets:
            if m_col not in first_row or m_col not in last_row:
                continue

            odd_open = float(first_row[m_col]) if pd.notna(first_row[m_col]) else np.nan
            odd_now = float(last_row[m_col]) if pd.notna(last_row[m_col]) else np.nan

            if pd.isna(odd_open) or pd.isna(odd_now) or odd_open <= 1.01 or odd_now <= 1.01:
                continue

            # Drop % positivo = a odd caiu (o time virou mais favorito / entrou dinheiro)
            # drop_pct = (odd_open - odd_now) / odd_open * 100
            drop_pct = ((odd_open - odd_now) / odd_open) * 100.0

            # Classificação
            if drop_pct >= 10.0:
                intensidade = "🔴 MEGA STEAM (Sharp)"
                status = "STEAM_STRONG"
            elif drop_pct >= 5.0:
                intensidade = "🟡 STEAM MODERADO"
                status = "STEAM_MODERATE"
            elif drop_pct <= -8.0:
                intensidade = "🔵 DRIFT ACENTUADO"
                status = "DRIFT"
            else:
                intensidade = "⚪ ESTÁVEL"
                status = "STABLE"

            results.append({
                "ID_Evento": first_row.get("ID_Evento", ""),
                "Date": str(first_row.get("Date", ""))[:10],
                "Time": str(first_row.get("Time", "")),
                "League": first_row.get("League", ""),
                "Home": first_row.get("Home", ""),
                "Away": first_row.get("Away", ""),
                "Mercado": m_label,
                "Odd_Abertura": round(odd_open, 2),
                "Odd_Atual": round(odd_now, 2),
                "Delta_Odd": round(odd_now - odd_open, 2),
                "Drop_Pct": round(drop_pct, 2),
                "Intensidade": intensidade,
                "Status": status,
                "Snapshots_Qtd": len(group),
                "Primeira_Captura": str(t_first)[11:16],
                "Ultima_Captura": str(t_last)[11:16]
            })

    if not results:
        return pd.DataFrame()

    df_res = pd.DataFrame(results)
    # Filtrar movimentos relevantes ou ordenar por maior Drop_Pct
    df_res = df_res.sort_values("Drop_Pct", ascending=False).reset_index(drop=True)
    return df_res


def enviar_alertas_telegram(df_moves: pd.DataFrame, min_drop_pct: float = 8.0) -> int:
    """
    Envia alertas no Telegram para partidas que apresentarem Steam Move relevante (drop >= min_drop_pct).
    Utiliza cache local steam_data/alertas_enviados.json para não duplicar envios no mesmo dia.
    """
    if df_moves is None or df_moves.empty:
        return 0

    import json
    try:
        from telegram_notifier import enviar_mensagem_telegram
    except ImportError:
        sys.path.insert(0, str(ROOT))
        from telegram_notifier import enviar_mensagem_telegram

    cache_file = STEAM_DIR / "alertas_enviados.json"
    enviados = set()
    if cache_file.exists():
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                enviados = set(json.load(f))
        except Exception:
            enviados = set()

    candidatos = df_moves[df_moves["Drop_Pct"] >= min_drop_pct].copy()
    if candidatos.empty:
        return 0

    novos_enviados = 0
    for _, row in candidatos.iterrows():
        # Chave única por evento, mercado e faixa de queda arredondada para 2%
        faixa = int(row.get("Drop_Pct", 0) // 2) * 2
        key = f"{row.get('Date')}_{row.get('Home')}_{row.get('Away')}_{row.get('Mercado')}_{faixa}"
        if key in enviados:
            continue

        drop_str = f"-{abs(row['Drop_Pct']):.1f}%"
        intensidade = row.get("Intensidade", "🔴 STEAM MOVE")
        
        texto = (
            f"🚨 *ARKAD — ALERTA DE SHARP MONEY* 🚨\n\n"
            f"⚽ *{row.get('Home')} vs {row.get('Away')}*\n"
            f"🏆 *Liga:* {row.get('League', 'N/A')} | ⏰ *Hora:* {row.get('Time', 'N/A')}\n"
            f"🎯 *Mercado:* Back {row.get('Mercado')}\n"
            f"📉 *Odd Abertura:* `{row.get('Odd_Abertura'):.2f}` ➡️ *Odd Atual:* `{row.get('Odd_Atual'):.2f}` ({drop_str})\n"
            f"🔥 *Intensidade:* {intensidade}\n\n"
            f"⚡ *Sindicatos injetando volume pesado na Betfair.* Entrar a favor antes da linha fechar!"
        )

        ok, msg = enviar_mensagem_telegram(texto)
        if ok:
            enviados.add(key)
            novos_enviados += 1
            print(f"[+] Alerta Telegram enviado para: {row.get('Home')} vs {row.get('Away')}")

    if novos_enviados > 0:
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(list(enviados), f, indent=2)

    return novos_enviados


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Steam Tracker Betfair")
    parser.add_argument("--capture", action="store_true", help="Captura um novo snapshot hoje")
    parser.add_argument("--notify", action="store_true", help="Envia alertas no Telegram para Steam Moves detectados")
    parser.add_argument("--date", type=str, default=None, help="Data YYYY-MM-DD")
    parser.add_argument("--min_drop", type=float, default=5.0, help="Drop % mínimo para exibição")
    parser.add_argument("--notify_drop", type=float, default=8.0, help="Drop % mínimo para alerta Telegram")
    args = parser.parse_args()

    target_d = args.date or datetime.now().strftime("%Y-%m-%d")

    if args.capture:
        capturar_snapshot(target_d)

    df_moves = detectar_steam_moves(target_d, min_drop_pct=args.min_drop)
    if not df_moves.empty:
        print(f"\n[*] Movimentações encontradas ({len(df_moves)} registros):")
        steams = df_moves[df_moves["Status"].str.contains("STEAM")]
        print(f"[+] Total com Steam Move >= 5%: {len(steams)}")
        if not steams.empty:
            print(steams[["Time", "Home", "Away", "Mercado", "Odd_Abertura", "Odd_Atual", "Drop_Pct", "Intensidade"]].head(10))
            
        if args.notify:
            n_sent = enviar_alertas_telegram(df_moves, min_drop_pct=args.notify_drop)
            print(f"[+] {n_sent} novos alertas disparados no Telegram.")
    else:
        print("[i] Sem dados suficientes ou sem variações expressivas.")
