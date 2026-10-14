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
from datetime import datetime, timedelta
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
    5: {"nome": "Bloco 5 (18:30) — Noite Américas 1", "inicio": "18:00", "fim": "21:00"},
    6: {"nome": "Bloco 6 (21:00) — Noite Américas 2", "inicio": "21:00", "fim": "22:30"},
    7: {"nome": "Bloco 7 (22:30) — América do Norte & Tardios", "inicio": "22:30", "fim": "23:59"},
    8: {"nome": "Bloco 8 (00:30) — Fechamento Geral & Liquidação Definitiva", "inicio": "00:00", "fim": "23:59"},
}

def detectar_bloco_atual():
    agora = datetime.now().strftime("%H:%M")
    if agora < "04:00":
        return 8  # Madrugada (00h00 às 04h00): fechamento e liquidação do dia anterior
    elif agora < "10:30":
        return 1
    elif agora < "13:00":
        return 2
    elif agora < "15:30":
        return 3
    elif agora < "18:30":
        return 4
    elif agora < "21:00":
        return 5
    elif agora < "22:30":
        return 6
    else:
        return 7  # 22h30 às 23h59: Bloco 7 América do Norte

def executar_bloco(num_bloco=None, data_str=None, banca=4000.0, risco_pct=0.05, enviar_telegram=True):
    if not num_bloco:
        num_bloco = detectar_bloco_atual()
        
    if not data_str:
        if num_bloco == 8 and datetime.now().hour < 5:
            # No bloco da madrugada, liquida e fecha o dia que acabou de encerrar
            data_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            data_str = datetime.now().strftime("%Y-%m-%d")
        
    info = BLOCOS_INFO.get(num_bloco, BLOCOS_INFO[1])
    liability_fixa = banca * risco_pct
    
    print(f"\n[+] ⏰ EXECUTANDO RADAR EM BLOCOS: {info['nome']} — Data: {data_str}")
    print(f"[*] Sincronizando odds frescas diretamente da VPS (Betfair Exchange)...")
    
    # 1. Atualiza e puxa o ledger da VPS
    df_dia = gerar_planilha_do_coletor(data_str, universo="feed", min_liq=200.0, sync_vps=True)
    if df_dia.empty:
        print(f"[-] Nenhum sinal registrado para hoje até o momento.")
        return df_dia
        
    # Liquidação automática dos jogos que já terminaram
    try:
        from liquidar_sinais_dia import liquidar_planilha_dia
        liquidar_planilha_dia(data_str)
        excel_path = ROOT / "metodos_aprovados" / f"Sinais_Metodos_Aprovados_{data_str}.xlsx"
        if excel_path.exists():
            df_dia = pd.read_excel(excel_path)
    except Exception as e:
        print(f"[-] Aviso ao liquidar jogos: {e}")
        
    # Tetos estritos causais homologados e travas operacionais (GEMINI.md)
    if "Odd_Entrada" in df_dia.columns:
        odd_e_col = pd.to_numeric(df_dia["Odd_Entrada"], errors="coerce").fillna(999.0)
        is_2x2 = df_dia["Método"].astype(str).str.contains("2x2", case=False, na=False)
        is_0x3 = df_dia["Método"].astype(str).str.contains("0x3", case=False, na=False)
        is_3x0 = df_dia["Método"].astype(str).str.contains("3x0", case=False, na=False)
        is_home = df_dia["Método"].astype(str).str.contains("Home", case=False, na=False)
        is_draw = df_dia["Método"].astype(str).str.contains("Draw", case=False, na=False)
        is_o45 = df_dia["Método"].astype(str).str.contains("4.5", case=False, na=False)
        df_dia = df_dia[~(is_2x2 & (odd_e_col > 14.0))].copy()
        df_dia = df_dia[~(is_0x3 & (odd_e_col > 30.0))].copy()
        df_dia = df_dia[~(is_3x0 & (odd_e_col > 30.0))].copy()
        df_dia = df_dia[~(is_home & (odd_e_col > 8.00))].copy()
        df_dia = df_dia[~(is_draw & (odd_e_col > 7.00))].copy()
        df_dia = df_dia[~(is_o45 & (odd_e_col > 20.00))].copy()
        
    if "Liquidez_Lay" in df_dia.columns:
        df_dia = df_dia[pd.to_numeric(df_dia["Liquidez_Lay"], errors="coerce").fillna(0) >= 200.0].copy()
        
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
    
    # Sincronização automática com GitHub / Streamlit Cloud
    try:
        import subprocess
        subprocess.run(["git", "add", "metodos_aprovados/"], cwd=str(ROOT), capture_output=True, timeout=15)
        subprocess.run(["git", "commit", "-m", f"chore(radar): auto-sync sinais {data_str} bloco {num_bloco}"], cwd=str(ROOT), capture_output=True, timeout=15)
        subprocess.run(["git", "push", "origin", "main"], cwd=str(ROOT), capture_output=True, timeout=30)
        print(f"[+] Sinais sincronizados com GitHub / Streamlit Cloud.")
    except Exception as e:
        print(f"[-] Aviso ao sincronizar com git: {e}")
    
    # 4. Envia o boletim deste bloco no Telegram
    if enviar_telegram:
        if num_bloco == 8:
            greens = len(df_dia[df_dia["Resultado"] == "GREEN"])
            reds = len(df_dia[df_dia["Resultado"] == "RED"])
            pendentes = len(df_dia[df_dia["Resultado"].astype(str).str.contains("PENDENTE", case=False, na=False)])
            total_resolvidos = greens + reds
            wr = (greens / total_resolvidos * 100) if total_resolvidos > 0 else 0.0

            pl_total_rs = 0.0
            for _, row in df_dia.iterrows():
                res = str(row.get("Resultado", "")).upper()
                lucro = float(row.get("Lucro_Green_R$") or 0.0)
                risco = float(row.get("Risco_Red_R$") or liability_fixa)
                if res == "GREEN":
                    pl_total_rs += lucro
                elif res == "RED":
                    pl_total_rs -= risco

            sinal_pl = "+" if pl_total_rs >= 0 else "-"
            msg_fechamento = [
                f"🏁 *ARKAD — FECHAMENTO GERAL DO DIA ({data_str})*",
                f"⚡ *Bloco 8 (00:30)* — Liquidação Oficial & Balanço Definitivo",
                f"💰 *Banca:* R$ {banca:,.2f} | 🛡️ *Risco Máx:* R$ {liability_fixa:.2f} ({risco_pct*100:.1f}%)",
                "━━━━━━━━━━━━━━━━━━━━━━━",
                f"📊 *Total de Entradas:* {len(df_dia)} jogos",
                f"🟢 *Greens:* {greens} | 🔴 *Reds:* {reds} | ⏳ *Pendentes:* {pendentes}",
                f"🎯 *Taxa de Acerto:* {wr:.1f}%",
                f"💵 *Resultado Líquido:* `{sinal_pl}R$ {abs(pl_total_rs):,.2f}`",
                "━━━━━━━━━━━━━━━━━━━━━━━",
                "✅ Planilha oficial consolidada e sincronizada no Streamlit Cloud."
            ]
            texto = "\n".join(msg_fechamento)
            ok_m, res_m = enviar_mensagem_telegram(texto, force=True)
            print(f"[+] Envio Telegram (Fechamento Geral Bloco 8): {ok_m} ({res_m})")
        elif not df_bloco.empty:
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
        else:
            print(f"[*] Nenhum jogo neste bloco ({info['inicio']} às {info['fim']}). Sem envio de mensagem redundante.")
        
    return df_bloco

def main():
    parser = argparse.ArgumentParser(description="Radar de Sinais em 8 Blocos de Horários")
    parser.add_argument("--bloco", type=int, choices=[1, 2, 3, 4, 5, 6, 7, 8], default=None, help="Número do bloco (1 a 8)")
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
