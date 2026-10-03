# -*- coding: utf-8 -*-
"""
executar_radar_5_blocos.py — Execução em 5 Horários Estratégicos (Odds Reais por Bloco)

Por que 5 horários:
As odds da Betfair sofrem drásticas oscilações e ganham liquidez real à medida que o jogo se aproxima.
Avaliar jogos das 16:00 às 06:00 da manhã distorce a precificação.
Esta rotina divide o dia em 5 janelas de entrada com odds frescas de mercado:
  - Bloco 1 (06:00): Jogos matinais (06:30 - 10:30)
  - Bloco 2 (10:30): Jogos Europa 1 (11:00 - 13:00 - Premier League, Championship, Escócia)
  - Bloco 3 (13:00): Jogos Europa 2 (13:30 - 15:30 - Bundesliga, Serie A, La Liga)
  - Bloco 4 (15:30): Jogos Europa 3 / América 1 (16:00 - 18:00 - Grandes jogos tarde)
  - Bloco 5 (18:30): Jogos Noturnos Américas (19:00 - 23:30 - Brasileirão, Argentina, MLS)
"""
import os, sys, argparse
from datetime import datetime
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from sinais_dia_coletor import gerar_planilha_do_coletor, sincronizar_ledger_vps
from telegram_notifier import enviar_mensagem_telegram, enviar_documento_telegram

BLOCOS_INFO = {
    1: {"nome": "Bloco 1 (06:00) — Matinal", "inicio": "06:00", "fim": "10:30"},
    2: {"nome": "Bloco 2 (10:30) — Europa Principal 1", "inicio": "10:30", "fim": "13:00"},
    3: {"nome": "Bloco 3 (13:00) — Europa Principal 2", "inicio": "13:00", "fim": "15:30"},
    4: {"nome": "Bloco 4 (15:30) — Tarde / Clássicos", "inicio": "15:30", "fim": "18:00"},
    5: {"nome": "Bloco 5 (18:30) — Noite Américas", "inicio": "18:00", "fim": "23:59"},
}

def detectar_bloco_atual():
    agora = datetime.now().strftime("%H:%M")
    if agora < "10:30":
        return 1
    elif agora < "13:00":
        return 2
    elif agora < "15:30":
        return 3
    elif agora < "18:30":
        return 4
    else:
        return 5

def executar_bloco(num_bloco=None, data_str=None, banca=4000.0, risco_pct=0.05, enviar_telegram=True):
    if not data_str:
        data_str = datetime.now().strftime("%Y-%m-%d")
    if not num_bloco:
        num_bloco = detectar_bloco_atual()
        
    info = BLOCOS_INFO.get(num_bloco, BLOCOS_INFO[1])
    liability_fixa = banca * risco_pct
    
    print(f"\n[+] ⏰ EXECUTANDO RADAR EM BLOCOS: {info['nome']} — Data: {data_str}")
    print(f"[*] Sincronizando odds frescas diretamente da VPS (Betfair Exchange)...")
    
    # 1. Atualiza e puxa o ledger da VPS
    df_dia = gerar_planilha_do_coletor(data_str, sync_vps=True)
    if df_dia.empty:
        print(f"[-] Nenhum sinal registrado para hoje até o momento.")
        return df_dia
        
    # 2. Filtra jogos pertencentes a esta janela ou pendentes
    df_dia["Hora_Str"] = df_dia["Hora"].astype(str).str[:5]
    df_bloco = df_dia[
        (df_dia["Hora_Str"] >= info["inicio"]) & 
        (df_dia["Hora_Str"] <= info["fim"])
    ].copy()
    
    print(f"[+] Total geral do dia: {len(df_dia)} jogos | Neste Bloco ({info['inicio']} às {info['fim']}): {len(df_bloco)} jogos")
    
    # 3. Calcula stakes e lucros sugeridos
    for df_target in [df_dia, df_bloco]:
        if "Stake_Sugerida_R$" not in df_target.columns and not df_target.empty:
            df_target["Odd_Entrada"] = pd.to_numeric(df_target["Odd_Entrada"], errors="coerce")
            df_target["Risco_Red_R$"] = liability_fixa
            df_target["Stake_Sugerida_R$"] = (liability_fixa / (df_target["Odd_Entrada"] - 1.0)).round(2)
            df_target["Lucro_Green_R$"] = (df_target["Stake_Sugerida_R$"] * 0.955).round(2)
            
    # Salva a planilha consolidada do dia
    excel_path = ROOT / "metodos_aprovados" / f"Sinais_Metodos_Aprovados_{data_str}.xlsx"
    df_dia.to_excel(excel_path, index=False)
    print(f"[+] Planilha consolidada atualizada: {excel_path.name}")
    
    # 4. Envia o boletim deste bloco no Telegram
    if enviar_telegram and not df_bloco.empty:
        msg_linhas = [
            f"🎯 *ARKAD — RADAR DE ODDS FRESCAS ({data_str})*",
            f"⚡ *{info['nome']}* (Jogos das `{info['inicio']}` às `{info['fim']}`)",
            f"💰 *Banca:* R$ {banca:,.2f} | 🛡️ *Risco Máx por Jogo:* R$ {liability_fixa:.2f} ({risco_pct*100:.1f}%)",
            f"📊 *Entradas deste Bloco:* {len(df_bloco)} jogos\n",
            "━━━━━━━━━━━━━━━━━━━━━━━"
        ]
        for _, s in df_bloco.iterrows():
            odd_e = float(s.get("Odd_Entrada") or 0.0)
            stk = float(s.get("Stake_Sugerida_R$") or 0.0)
            lucro = float(s.get("Lucro_Green_R$") or 0.0)
            status = s.get("Status", "⏳ PENDENTE")
            msg_linhas.append(
                f"⏰ `{s.get('Hora', '')}` | 🏆 *{s.get('Liga', '')}*\n"
                f"⚽ *{s.get('Jogo', '')}*\n"
                f"📌 *{s.get('Método', '')}* (Odd Lay: `{odd_e:.2f}`)\n"
                f"💵 *Stake:* `R$ {stk:.2f}` ➔ *Lucro:* `+R$ {lucro:.2f}`\n"
                f"Status: {status}\n"
                "───────────────────────"
            )
        texto = "\n".join(msg_linhas)
        ok_m, res_m = enviar_mensagem_telegram(texto, force=True)
        print(f"[+] Envio Telegram ({info['nome']}): {ok_m} ({res_m})")
    elif enviar_telegram and df_bloco.empty:
        print(f"[*] Nenhum jogo neste bloco ({info['inicio']} às {info['fim']}). Sem envio de mensagem redundante.")
        
    return df_bloco

def main():
    parser = argparse.ArgumentParser(description="Radar de Sinais em 5 Blocos de Horários")
    parser.add_argument("--bloco", type=int, choices=[1, 2, 3, 4, 5], default=None, help="Número do bloco (1 a 5)")
    parser.add_argument("--data", type=str, default=None, help="Data formato YYYY-MM-DD")
    parser.add_argument("--banca", type=float, default=4000.0, help="Valor da banca em R$")
    parser.add_argument("--risco", type=float, default=0.05, help="Risco por aposta (ex: 0.05)")
    parser.add_argument("--sem-telegram", action="store_true", help="Não dispara no Telegram")
    
    args = parser.parse_args()
    executar_bloco(
        num_bloco=args.bloco,
        data_str=args.data,
        banca=args.banca,
        risco_pct=args.risco,
        enviar_telegram=(not args.sem_telegram)
    )

if __name__ == "__main__":
    main()
