import sys
sys.stdout.reconfigure(encoding='utf-8')
import os, time, argparse, json, numpy as np, pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import b365_data_utils
try:
    from telegram_notifier import enviar_mensagem_telegram, enviar_documento_telegram
except ImportError:
    enviar_mensagem_telegram = lambda *a, **k: (False, "Módulo Telegram ausente")
    enviar_documento_telegram = lambda *a, **k: (False, "Módulo Telegram ausente")

def _get_series(df, col_names, default=99.0):
    for c in col_names:
        if c in df.columns:
            return pd.to_numeric(df[c], errors="coerce").fillna(default)
    return pd.Series(default, index=df.index)

def gerar_sinais_manha(data_str=None, banca=4000.0, risco_pct=0.05, enviar_telegram=True):
    """Executa a rotina matinal: consulta a API da Betfair, aplica os métodos oficiais, dimensiona as stakes e salva o Excel."""
    if data_str is None:
        data_str = datetime.now().strftime("%Y-%m-%d")
        
    print(f"\n[+] 🌅 INICIANDO ROTINA MATINAL — DATA: {data_str} (Banca: R$ {banca:,.2f} | Risco: {risco_pct*100:.1f}%)")
    
    games = b365_data_utils.fetch_betfair_daily(data_str)
    if isinstance(games, list):
        df_games = pd.DataFrame(games)
    elif isinstance(games, pd.DataFrame):
        df_games = games
    else:
        df_games = pd.DataFrame()
        
    if df_games.empty:
        print(f"[-] Nenhum jogo retornado pela API da Betfair para {data_str}.")
        return pd.DataFrame()
        
    print(f"[+] Total de jogos brutos na grade da Betfair: {len(df_games)}")
    
    oh = _get_series(df_games, ["Odd_H_Back", "Odd_H_FT", "Odd_H"])
    oa = _get_series(df_games, ["Odd_A_Back", "Odd_A_FT", "Odd_A"])
    od = _get_series(df_games, ["Odd_D_Back", "Odd_D_FT", "Odd_D"])
    ou05 = _get_series(df_games, ["Odd_Under05_Back", "Odd_Under05_FT", "Odd_Under05"])
    ou25 = _get_series(df_games, ["Odd_Under25_Back", "Odd_Under25_FT", "Odd_Under25"])
    ou45_lay = _get_series(df_games, ["Odd_Over45_FT_Lay", "Odd_Over45_Lay"])
    l01 = _get_series(df_games, ["Odd_CS_0x1_Lay"])
    
    liability_fixa = banca * risco_pct
    sinais = []
    
    for idx in range(len(df_games)):
        r = df_games.iloc[idx]
        h, a = str(r.get("Home", "Casa")), str(r.get("Away", "Fora"))
        jogo = f"{h} x {a}"
        hora = str(r.get("Time", r.get("Hora", "15:00")))[:5]
        liga = str(r.get("League", r.get("Liga", "N/A")))
        
        # 1. Lay 0x1 Super Fav Mandante (Odd_H <= 1.90 | Lay 5-15)
        if oh.iloc[idx] <= 1.90 and 5.0 <= l01.iloc[idx] <= 15.0:
            odd_e = round(float(l01.iloc[idx]), 2)
            stake_sug = round(liability_fixa / (odd_e - 1.0), 2)
            sinais.append({
                "Data": data_str, "Hora": hora, "Liga": liga, "Jogo": jogo,
                "Método": "Lay 0x1 Super Fav", "Mercado": "CS (0x1)", "Lado": "LAY",
                "Odd_Entrada": odd_e, "Odd_Fav": round(float(oh.iloc[idx]), 2),
                "Stake_Sugerida_R$": stake_sug, "Lucro_Green_R$": round(stake_sug * 0.955, 2),
                "Risco_Red_R$": liability_fixa, "Resultado": "PENDENTE"
            })
            
        # 2. Lay Under 0.5 FT em Super Fav (Odd <= 1.60 | Odd_U05 <= 15.0)
        fav_odd = min(oh.iloc[idx], oa.iloc[idx])
        if fav_odd <= 1.60 and 5.0 <= ou05.iloc[idx] * 1.05 <= 15.0:
            odd_e = round(float(ou05.iloc[idx] * 1.05), 2)
            stake_sug = round(liability_fixa / (odd_e - 1.0), 2)
            sinais.append({
                "Data": data_str, "Hora": hora, "Liga": liga, "Jogo": jogo,
                "Método": "Lay Under 0.5 FT", "Mercado": "Under 0.5", "Lado": "LAY",
                "Odd_Entrada": odd_e, "Odd_Fav": round(float(fav_odd), 2),
                "Stake_Sugerida_R$": stake_sug, "Lucro_Green_R$": round(stake_sug * 0.955, 2),
                "Risco_Red_R$": liability_fixa, "Resultado": "PENDENTE"
            })
            
        # 3. Lay Draw em Super Fav Mandante (Odd_H <= 1.40 | Odd_D 4.5 a 10.0)
        if oh.iloc[idx] <= 1.40 and 4.5 <= od.iloc[idx] * 1.03 <= 10.0:
            odd_e = round(float(od.iloc[idx] * 1.03), 2)
            stake_sug = round(liability_fixa / (odd_e - 1.0), 2)
            sinais.append({
                "Data": data_str, "Hora": hora, "Liga": liga, "Jogo": jogo,
                "Método": "Lay Draw Super Fav", "Mercado": "Draw", "Lado": "LAY",
                "Odd_Entrada": odd_e, "Odd_Fav": round(float(oh.iloc[idx]), 2),
                "Stake_Sugerida_R$": stake_sug, "Lucro_Green_R$": round(stake_sug * 0.955, 2),
                "Risco_Red_R$": liability_fixa, "Resultado": "PENDENTE"
            })

        # 4. Lay Over 4.5 FT em Jogos Under (Odd_U25 <= 1.50 | 4.0 <= Odd_Lay_O45 <= 20.0)
        if ou25.iloc[idx] <= 1.50 and 4.0 <= ou45_lay.iloc[idx] <= 20.0:
            odd_e = round(float(ou45_lay.iloc[idx]), 2)
            stake_sug = round(liability_fixa / (odd_e - 1.0), 2)
            sinais.append({
                "Data": data_str, "Hora": hora, "Liga": liga, "Jogo": jogo,
                "Método": "Lay Over 4.5 FT", "Mercado": "Over 4.5", "Lado": "LAY",
                "Odd_Entrada": odd_e, "Odd_Fav": round(float(ou25.iloc[idx]), 2),
                "Stake_Sugerida_R$": stake_sug, "Lucro_Green_R$": round(stake_sug * 0.955, 2),
                "Risco_Red_R$": liability_fixa, "Resultado": "PENDENTE"
            })

        # 5. Lay Away / Dupla Chance 1X em Super Fav Mandante (Odd_H <= 1.45 | 2.0 <= Lay_A <= 15.0)
        oa_lay = oa.iloc[idx] * 1.03
        if oh.iloc[idx] <= 1.45 and 2.0 <= oa_lay <= 15.0:
            odd_e = round(float(oa_lay), 2)
            stake_sug = round(liability_fixa / (odd_e - 1.0), 2)
            sinais.append({
                "Data": data_str, "Hora": hora, "Liga": liga, "Jogo": jogo,
                "Método": "Lay Away Super Fav", "Mercado": "Match Odds (Away)", "Lado": "LAY",
                "Odd_Entrada": odd_e, "Odd_Fav": round(float(oh.iloc[idx]), 2),
                "Stake_Sugerida_R$": stake_sug, "Lucro_Green_R$": round(stake_sug * 0.955, 2),
                "Risco_Red_R$": liability_fixa, "Resultado": "PENDENTE"
            })

        # 6. Lay 0x2 / 2x0 Zebra (Super Fav Mandante H <= 1.45 ou Super Fav Visitante A <= 1.45)
        l02 = _get_series(df_games, ["Odd_CS_0x2_Lay"])
        l20 = _get_series(df_games, ["Odd_CS_2x0_Lay"])
        if oh.iloc[idx] <= 1.45 and 5.0 <= l02.iloc[idx] <= 25.0:
            odd_e = round(float(l02.iloc[idx]), 2)
            stake_sug = round(liability_fixa / (odd_e - 1.0), 2)
            sinais.append({
                "Data": data_str, "Hora": hora, "Liga": liga, "Jogo": jogo,
                "Método": "Lay 0x2 Zebra", "Mercado": "CS (0x2)", "Lado": "LAY",
                "Odd_Entrada": odd_e, "Odd_Fav": round(float(oh.iloc[idx]), 2),
                "Stake_Sugerida_R$": stake_sug, "Lucro_Green_R$": round(stake_sug * 0.955, 2),
                "Risco_Red_R$": liability_fixa, "Resultado": "PENDENTE"
            })
        elif oa.iloc[idx] <= 1.45 and 5.0 <= l20.iloc[idx] <= 25.0:
            odd_e = round(float(l20.iloc[idx]), 2)
            stake_sug = round(liability_fixa / (odd_e - 1.0), 2)
            sinais.append({
                "Data": data_str, "Hora": hora, "Liga": liga, "Jogo": jogo,
                "Método": "Lay 2x0 Zebra", "Mercado": "CS (2x0)", "Lado": "LAY",
                "Odd_Entrada": odd_e, "Odd_Fav": round(float(oa.iloc[idx]), 2),
                "Stake_Sugerida_R$": stake_sug, "Lucro_Green_R$": round(stake_sug * 0.955, 2),
                "Risco_Red_R$": liability_fixa, "Resultado": "PENDENTE"
            })

        # 7. Lay Home / Dupla Chance X2 em Fav Visitante (Odd_A <= 1.65 | 2.0 <= Lay_H <= 10.0)
        oh_lay = oh.iloc[idx] * 1.03
        if oa.iloc[idx] <= 1.65 and 2.0 <= oh_lay <= 10.0:
            odd_e = round(float(oh_lay), 2)
            stake_sug = round(liability_fixa / (odd_e - 1.0), 2)
            sinais.append({
                "Data": data_str, "Hora": hora, "Liga": liga, "Jogo": jogo,
                "Método": "Lay Home Fav Visitante", "Mercado": "Match Odds (Home)", "Lado": "LAY",
                "Odd_Entrada": odd_e, "Odd_Fav": round(float(oa.iloc[idx]), 2),
                "Stake_Sugerida_R$": stake_sug, "Lucro_Green_R$": round(stake_sug * 0.955, 2),
                "Risco_Red_R$": liability_fixa, "Resultado": "PENDENTE"
            })
            
    df_sinais = pd.DataFrame(sinais)
    print(f"[+] Total de sinais oficiais qualificados: {len(df_sinais)}")
    
    if not df_sinais.empty:
        df_sinais = df_sinais.sort_values(["Data", "Hora"]).reset_index(drop=True)
        out_excel = ROOT / "metodos_aprovados" / f"Sinais_Metodos_Aprovados_{data_str}.xlsx"
        df_sinais.to_excel(out_excel, index=False)
        print(f"[+] Planilha matinal salva em: {out_excel.name}")
        
        if enviar_telegram:
            msg_linhas = [
                f"🎯 *ARKAD — RADAR DE SINAIS DO DIA ({data_str})*",
                f"💰 *Banca:* R$ {banca:,.2f} | 🛡️ *Risco Máx por Jogo:* R$ {liability_fixa:.2f} ({risco_pct*100:.1f}%)",
                f"📊 *Total de Entradas Qualificadas:* {len(df_sinais)} jogos\n",
                "━━━━━━━━━━━━━━━━━━━━━━━"
            ]
            for _, s in df_sinais.iterrows():
                msg_linhas.append(
                    f"⏰ `{s['Hora']}` | 🏆 *{s['Liga']}*\n"
                    f"⚽ *{s['Jogo']}*\n"
                    f"📌 *{s['Método']}* (Odd Lay: `{s['Odd_Entrada']:.2f}`)\n"
                    f"💵 *Stake:* `R$ {s['Stake_Sugerida_R$']:.2f}` ➔ *Lucro Green:* `+R$ {s['Lucro_Green_R$']:.2f}`\n"
                    "───────────────────────"
                )
            texto_telegram = "\n".join(msg_linhas)
            enviar_mensagem_telegram(texto_telegram)
            enviar_documento_telegram(out_excel, legenda=f"📥 Planilha de Sinais — {data_str}")
            print("[+] Mensagem e planilha enviadas com sucesso no Telegram!")
            
    return df_sinais

def liquidar_resultados_noite(data_str=None, enviar_telegram=True):
    """Executa a rotina noturna: lê a planilha do dia, busca placares, liquida greens/reds e notifica o fechamento."""
    if data_str is None:
        data_str = datetime.now().strftime("%Y-%m-%d")
        
    print(f"\n[+] 🌙 INICIANDO ROTINA NOTURNA DE LIQUIDAÇÃO — DATA: {data_str}")
    
    excel_path = ROOT / "metodos_aprovados" / f"Sinais_Metodos_Aprovados_{data_str}.xlsx"
    if not excel_path.exists():
        print(f"[-] Planilha {excel_path.name} não encontrada para liquidação.")
        return None
        
    df_dia = pd.read_excel(excel_path)
    if df_dia.empty:
        return None
        
    print(f"[+] Total de jogos a liquidar: {len(df_dia)}")
    
    # Mapa de placares
    mapa_placares = {}
    cache_file = ROOT / "_placares_coletor_cache.csv"
    if cache_file.exists():
        try:
            df_c = pd.read_csv(cache_file)
            for _, rc in df_c.iterrows():
                k = f"{str(rc.get('Home')).strip().lower()}_{str(rc.get('Away')).strip().lower()}"
                mapa_placares[k] = (rc.get('Goals_H_FT'), rc.get('Goals_A_FT'))
        except Exception:
            pass
            
    COMM = 0.045
    for idx, r in df_dia.iterrows():
        # Se já estiver liquidado, mantém
        if r.get("Resultado") in ["GREEN", "RED"]:
            continue
            
        jogo = str(r["Jogo"])
        partes = jogo.split(" x ")
        h, a = partes[0].strip(), partes[1].strip()
        k = f"{h.lower()}_{a.lower()}"
        
        gh, ga = mapa_placares.get(k, (r.get("Goals_H"), r.get("Goals_A")))
        metodo = str(r["Método"])
        odd = float(r["Odd_Entrada"])
        stake_r = float(r.get("Stake_Sugerida_R$", 10.0))
        liab_r = float(r.get("Risco_Red_R$", 50.0))
        
        if pd.notna(gh) and pd.notna(ga) and str(gh) != "nan":
            gh, ga = int(gh), int(ga)
            df_dia.at[idx, "Placar"] = f"{gh}x{ga}"
            
            if "0x1" in metodo:
                res = "RED" if (gh == 0 and ga == 1) else "GREEN"
            elif "Under 0.5" in metodo:
                res = "RED" if (gh == 0 and ga == 0) else "GREEN"
            elif "Draw" in metodo:
                res = "RED" if (gh == ga) else "GREEN"
            elif "Over 4.5" in metodo:
                res = "RED" if (gh + ga >= 5) else "GREEN"
            elif "Away" in metodo:
                res = "RED" if (ga > gh) else "GREEN"
            elif "Home" in metodo:
                res = "RED" if (gh > ga) else "GREEN"
            elif "0x2" in metodo:
                res = "RED" if (gh == 0 and ga == 2) else "GREEN"
            elif "2x0" in metodo:
                res = "RED" if (gh == 2 and ga == 0) else "GREEN"
            elif "Under 1.5" in metodo:
                res = "RED" if (gh + ga < 2) else "GREEN"
            else:
                res = "GREEN"
                
            df_dia.at[idx, "Resultado"] = res
            df_dia.at[idx, "1/0"] = 1 if res == "GREEN" else 0
            df_dia.at[idx, "PnL_u"] = 0.955 if res == "GREEN" else -(odd - 1.0)
            df_dia.at[idx, "PnL_R$"] = round(stake_r * (1.0 - COMM), 2) if res == "GREEN" else -liab_r
            
    df_dia.to_excel(excel_path, index=False)
    print(f"[+] Planilha liquidada e atualizada em: {excel_path.name}")
    
    # Resumo
    n = len(df_dia)
    greens = (df_dia["Resultado"] == "GREEN").sum()
    reds = (df_dia["Resultado"] == "RED").sum()
    pend = n - greens - reds
    lucro_u = df_dia["PnL_u"].sum() if "PnL_u" in df_dia.columns else 0.0
    lucro_r = df_dia["PnL_R$"].sum() if "PnL_R$" in df_dia.columns else 0.0
    
    print(f"\n📊 FECHAMENTO DO DIA: {greens} Greens | {reds} Reds | {pend} Pendentes")
    print(f"💰 Lucro Líquido: {lucro_u:+.3f} unidades | R$ {lucro_r:+,.2f}")
    
    if enviar_telegram and (greens > 0 or reds > 0):
        badge = "🎉 GREEN DAY" if lucro_r >= 0 else "⚠️ DIA NEGATIVO"
        msg_fechamento = (
            f"🌙 *ARKAD — FECHAMENTO DO DIA ({data_str})*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎮 *Jogos Operados:* {n}\n"
            f"🟢 *Greens:* {greens} ({greens/(greens+reds)*100:.1f}% WR)\n"
            f"🔴 *Reds:* {reds}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 *Lucro em Unidades:* `{lucro_u:+.2f} u`\n"
            f"💵 *Resultado Financeiro:* `R$ {lucro_r:+,.2f}` ({badge})\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🚀 *Status:* Resultados registrados no Streamlit!"
        )
        enviar_mensagem_telegram(msg_fechamento)
        print("[+] Notificação de fechamento enviada no Telegram!")
        
    return df_dia

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ARKAD — Robô de Automação Diária")
    parser.add_argument("--manha", action="store_true", help="Executar rotina matinal de geração de sinais")
    parser.add_argument("--noite", action="store_true", help="Executar rotina noturna de liquidação")
    parser.add_argument("--data", type=str, default=None, help="Data específica no formato YYYY-MM-DD")
    parser.add_argument("--banca", type=float, default=4000.0, help="Valor total da banca (R$)")
    parser.add_argument("--risco", type=float, default=0.05, help="Percentual de risco por operação (default: 0.05)")
    args = parser.parse_args()
    
    if args.manha:
        gerar_sinais_manha(args.data, banca=args.banca, risco_pct=args.risco)
    elif args.noite:
        liquidar_resultados_noite(args.data)
    else:
        print("[*] Executando rotina padrão de teste de geração...")
        gerar_sinais_manha(args.data, banca=args.banca, risco_pct=args.risco)
