#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
radar_periodos_vps.py — Radar de Candidatos por Turno / Período no Telegram.

Envia antecipadamente os jogos candidatos nos métodos aprovados do ARKAD para
planejamento do trader nos 3 blocos do dia:
  - Bloco 1 (Manhã): 06:00 BRT  (cobre jogos das 06:00 às 10:30)
  - Bloco 2 (Tarde): 10:30 BRT  (cobre jogos das 10:30 às 16:30)
  - Bloco 3 (Noite): 16:30 BRT  (cobre jogos das 16:30 às 23:59)

A confirmação executável de cada jogo continua sendo estritamente enviada
15 minutos antes do início (KO-10) pelo serviço sinais_ko_vps.py.
"""
import os, sys, csv, argparse, urllib.parse, urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

AQUI = Path(__file__).resolve().parent
sys.path.insert(0, str(AQUI))
import sinais_ko_core as K

COLETOR = AQUI / "betfair_live_odds.csv"
ENVF = AQUI / "alerta.env"

BLOCOS = {
    1: {"nome": "Bloco Manhã", "inicio": "06:00", "fim": "10:30", "desc": "06:00 às 10:30"},
    2: {"nome": "Bloco Tarde / Europa", "inicio": "10:30", "fim": "16:30", "desc": "10:30 às 16:30"},
    3: {"nome": "Bloco Noite / Américas", "inicio": "16:30", "fim": "23:59", "desc": "16:30 às 23:59"},
}

def agora_brt():
    """Retorna datetime atual naive no fuso de Brasília (UTC-3)."""
    return datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=3)

def detectar_bloco_atual():
    hora = agora_brt().strftime("%H:%M")
    if hora < "10:30":
        return 1
    elif hora < "16:30":
        return 2
    else:
        return 3

def _f(x):
    try:
        v = float(x)
        return v if v > 1.0 else None
    except Exception:
        return None

def _esc(t):
    return str(t).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def enviar_telegram(msg, parse_mode="HTML"):
    tok = chat = None
    if ENVF.exists():
        for ln in open(ENVF, encoding="utf-8"):
            if ln.startswith("TG_TOKEN="):
                tok = ln.split("=", 1)[1].strip()
            elif ln.startswith("TG_CHAT="):
                chat = ln.split("=", 1)[1].strip()
    if not (tok and chat):
        print("[-] Telegram nao configurado em alerta.env", flush=True)
        return False
    try:
        data = urllib.parse.urlencode({"chat_id": chat, "text": msg, "parse_mode": parse_mode}).encode()
        req = urllib.request.Request(f"https://api.telegram.org/bot{tok}/sendMessage", data=data)
        urllib.request.urlopen(req, timeout=15)
        return True
    except Exception as e:
        print(f"[-] Telegram falhou: {e}", flush=True)
        return False

def extrair_candidatos_bloco(num_bloco, dia_alvo=None):
    if not COLETOR.exists():
        print("[-] betfair_live_odds.csv nao encontrado.")
        return []

    bloco_cfg = BLOCOS.get(num_bloco)
    if not bloco_cfg:
        print(f"[-] Bloco {num_bloco} invalido.")
        return []

    hoje_brt = agora_brt()
    agora_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    dia_ref = dia_alvo or hoje_brt.strftime("%Y-%m-%d")

    # Lê os últimos 40 MB do arquivo de odds para cobrir os ciclos recentes do catálogo
    tamanho = COLETOR.stat().st_size
    offset = max(0, tamanho - 40 * 1024 * 1024)

    games = {}

    with open(COLETOR, "r", encoding="utf-8", errors="replace") as f:
        f.seek(offset)
        f.readline()  # descarta linha truncada
        for line in f:
            p = line.rstrip("\n").split(",")
            if len(p) < 13:
                continue
            mtype = p[1]
            if mtype != "MATCH_ODDS":
                continue

            ts_str, comp, home, away, ko_str = p[0], p[2], p[3], p[4], p[5]
            runner = p[7]
            try:
                mtk = float(p[6])
            except Exception:
                continue

            # Converter KO UTC para horário de Brasília (UTC-3)
            try:
                ko_dt_brt = datetime.strptime(ko_str, "%Y-%m-%d %H:%M") - timedelta(hours=3)
            except Exception:
                continue

            # Para rodada em tempo real (sem --dia forçado), o jogo DEVE ser futuro
            if dia_alvo is None and ko_dt_brt <= hoje_brt:
                continue

            dia_jogo = ko_dt_brt.strftime("%Y-%m-%d")
            hora_jogo = ko_dt_brt.strftime("%H:%M")

            # Filtrar para a data e janela do bloco
            if dia_jogo != dia_ref:
                continue
            if not (bloco_cfg["inicio"] <= hora_jogo <= bloco_cfg["fim"]):
                continue

            # Validar que a cotação foi gravada recentemente (últimas 2 horas)
            try:
                ts_dt = datetime.strptime(ts_str[:19], "%Y-%m-%d %H:%M:%S")
                if dia_alvo is None and (agora_utc - ts_dt).total_seconds() > 7200:
                    continue
            except Exception:
                pass

            k = (hora_jogo, home, away, comp)
            if k not in games:
                games[k] = {}

            b_p, b_s = _f(p[9]), _f(p[10]) or 0.0
            l_p, l_s = _f(p[11]), _f(p[12]) or 0.0
            
            # Sobrescreve com a cotação mais recente do arquivo
            if mtype not in games[k]:
                games[k][mtype] = {}
            games[k][mtype][runner] = (b_p, b_s, l_p, l_s)

    # Avaliar os candidatos usando o motor oficial
    candidatos_qualificados = []
    for k, mk in sorted(games.items()):
        hora_jogo, home, away, comp = k
        if K.eh_jogo_ignorado(home, away, comp):
            continue
        
        sinais = K.avaliar(mk, home, away, comp, {})
        if sinais:
            candidatos_qualificados.append({
                "hora": hora_jogo,
                "home": home,
                "away": away,
                "comp": comp,
                "sinais": sinais
            })

    return candidatos_qualificados

def montar_mensagem_bloco(num_bloco, candidatos, dia_alvo=None):
    bloco_cfg = BLOCOS[num_bloco]
    dia_ref = dia_alvo or agora_brt().strftime("%Y-%m-%d")
    data_fmt = datetime.strptime(dia_ref, "%Y-%m-%d").strftime("%d/%m/%Y")

    linhas = [
        f"📋 <b>ARKAD — RADAR DE CANDIDATOS</b>",
        f"⚡ <b>{bloco_cfg['nome']} ({bloco_cfg['desc']})</b>",
        f"📅 Data: <b>{data_fmt}</b>\n"
    ]

    if not candidatos:
        linhas.append(f"<i>Nenhum jogo pré-qualificado nos métodos aprovados para este período.</i>\n")
        linhas.append("<i>Novos jogos serão monitorados no próximo bloco.</i>")
        return "\n".join(linhas)

    linhas.append(f"🔍 <b>{len(candidatos)} jogos no radar para este período:</b>\n")
    linhas.append("━━━━━━━━━━━━━━━━━━━━━━━")

    for c in candidatos:
        linhas.append(f"⏰ <b>{c['hora']}</b> · <i>{_esc(c['comp'])}</i>")
        linhas.append(f"⚽ <b>{_esc(c['home'])} x {_esc(c['away'])}</b>")
        for metodo, odd, liq, fav in c["sinais"]:
            fav_txt = f" · fav {fav:.2f}" if fav else ""
            linhas.append(f"📌 <b>{_esc(metodo)}</b> — lay ref @<b>{odd:.2f}</b> (liq {liq:.0f}){fav_txt}")
        linhas.append("───────────────────────")

    linhas.append("\n⚠️ <b>AVISO DE EXECUÇÃO:</b>")
    linhas.append("<i>Esta é uma lista prévia de atenção. A confirmação oficial de entrada com a odd de Lay em tempo real será enviada 15 minutos antes de cada pontapé inicial (KO−10).</i>")

    return "\n".join(linhas)

def main():
    parser = argparse.ArgumentParser(description="Radar de Candidatos por Bloco (Telegram)")
    parser.add_argument("--bloco", type=int, choices=[1, 2, 3], default=None, help="1=Manhã, 2=Tarde, 3=Noite")
    parser.add_argument("--dia", type=str, default=None, help="Data formato YYYY-MM-DD")
    parser.add_argument("--enviar-tg", action="store_true", help="Dispara a mensagem no Telegram")
    parser.add_argument("--so-se-tiver-jogos", action="store_true", help="Não envia no Telegram se a lista estiver vazia")
    args = parser.parse_args()

    num_bloco = args.bloco if args.bloco else detectar_bloco_atual()
    print(f"[*] Processando {BLOCOS[num_bloco]['nome']} (Data: {args.dia or agora_brt().strftime('%Y-%m-%d')})...")

    candidatos = extrair_candidatos_bloco(num_bloco, dia_alvo=args.dia)
    print(f"[+] Jogos qualificados encontrados: {len(candidatos)}")

    msg = montar_mensagem_bloco(num_bloco, candidatos, dia_alvo=args.dia)
    print("\n--- MENSAGEM GERADA ---")
    print(msg)
    print("------------------------\n")

    if args.enviar_tg:
        if args.so_se_tiver_jogos and not candidatos:
            print("[*] 0 candidatos e flag --so-se-tiver-jogos ativa. Envio ao Telegram suprimido.")
        else:
            ok = enviar_telegram(msg)
            print(f"[+] Envio Telegram: {'SUCESSO' if ok else 'FALHOU'}")
    else:
        print("[i] Modo de teste: Telegram nao disparado (use --enviar-tg para enviar).")

if __name__ == "__main__":
    main()
