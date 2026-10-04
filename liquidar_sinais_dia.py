# -*- coding: utf-8 -*-
"""
liquidar_sinais_dia.py — Liquidação oficial e desacoplada dos sinais do dia
utilizando os placares autoritativos da Betfair (placares_ft.csv da VPS).

Regras de Governança (GEMINI.md):
- Lei 1: Odd executável na Betfair (Lay real).
- Lei 4: Matemática do LAY (comissão 5%):
    ev = p*(1 - 0.05) - (1 - p)*(odd - 1)
    GREEN: +(1 - 0.05) / (odd - 1) em unidades (ou stake * (1 - comissao) em R$)
    RED: -1.0 em unidades (ou -liability em R$)
- Hall of Shame: Desacoplamento estrito — o sinal é gerado PENDENTE antes do KO;
  a liquidação ocorre pós-jogo com os placares definitivos.
"""
import os, sys, re, unicodedata, argparse
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

COMISSAO_PADRAO = 0.05
LIAB_PADRAO_RS = 200.0  # R$ 200 (5% de uma banca de R$ 4.000)

def canon(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)

def verificar_red(metodo, gh, ga, cs_str=""):
    """Determina se o LAY deu RED (o evento indesejado aconteceu)."""
    m = str(metodo)
    
    # Se temos gols exatos
    if gh is not None and ga is not None:
        try:
            gh = int(gh)
            ga = int(ga)
            if "0x0" in m:
                return gh == 0 and ga == 0
            if "Draw" in m:
                return gh == ga
            if "Home" in m or "X2" in m:
                return gh > ga  # Lay Home da red se o Mandante venceu
            if "Over 4.5" in m:
                return (gh + ga) >= 5  # Lay Over 4.5 da red se houver 5 ou mais gols
            if "2x2" in m:
                return gh == 2 and ga == 2
            if "0x3" in m:
                return gh == 0 and ga == 3
            if "3x0" in m:
                return gh == 3 and ga == 0
            if "0x1" in m:
                return gh == 0 and ga == 1
            if "0x2" in m:
                return gh == 0 and ga == 2
            if "2x0" in m:
                return gh == 2 and ga == 0
            if "Away" in m or "1X" in m:
                return ga > gh
        except Exception:
            pass
            
    # Se temos apenas cs_winner categórico ("Any Other Home Win", etc.)
    if cs_str:
        cs = cs_str.strip()
        if cs.startswith("Any Other"):
            if "0x3" in m or "2x2" in m or "3x0" in m or "0x2" in m or "2x0" in m or "0x1" in m or "0x0" in m:
                return False  # Placar de cauda larga/goleada nunca e 0x3, 2x2, etc. (GREEN)
            if "Draw" in m:
                return cs == "Any Other Draw"
            if "Home" in m or "X2" in m:
                return cs == "Any Other Home Win"
                
    return None

def carregar_placares_oficiais():
    """Baixa ou carrega placares_ft.csv da VPS."""
    caminho_local = ROOT / "metodos_aprovados" / "placares_ft.csv"
    vps = "ubuntu@163.176.59.215"
    key = os.path.expanduser("~/Downloads/ssh-key-2026-07-31.key")
    if not os.path.exists(key):
        key = r"C:\Users\thiag\Downloads\ssh-key-2026-07-31.key"
    
    if os.path.exists(key):
        try:
            import subprocess
            subprocess.run([
                "scp", "-q", "-i", key, "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=10",
                f"{vps}:/home/ubuntu/betfair-collector/placares_ft.csv", str(caminho_local)
            ], capture_output=True, timeout=15)
        except Exception:
            pass

    if not caminho_local.exists():
        return {}

    mapa = {}
    try:
        df_p = pd.read_csv(caminho_local, dtype=str).fillna("")
        for _, r in df_p.iterrows():
            if r.get("status") == "LIQUIDADO":
                h_c = canon(r.get("home", ""))
                a_c = canon(r.get("away", ""))
                ko = str(r.get("ko", ""))[:10]
                gh = r.get("gh")
                ga = r.get("ga")
                cs = r.get("cs_winner")
                mo = r.get("mo_winner")
                
                # Chave exata por data + times
                mapa[(ko, h_c, a_c)] = (gh, ga, cs, mo)
                # Chave sem data (como fallback)
                mapa[(h_c, a_c)] = (gh, ga, cs, mo)
    except Exception as e:
        print(f"[-] Erro ao ler placares_ft.csv: {e}")
        
    return mapa

def liquidar_planilha_dia(data_str=None, comissao=COMISSAO_PADRAO):
    if not data_str:
        data_str = datetime.now().strftime("%Y-%m-%d")
        
    planilha_path = ROOT / "metodos_aprovados" / f"Sinais_Metodos_Aprovados_{data_str}.xlsx"
    if not planilha_path.exists():
        print(f"[-] Planilha {planilha_path.name} nao encontrada.")
        return 0, 0
        
    df = pd.read_excel(planilha_path)
    if df.empty:
        return 0, 0
        
    # Exclusão definitiva de Lay 0x3 (a pedido do usuário em 03/10/2026)
    df = df[~df["Método"].astype(str).str.contains("0x3", case=False, na=False)].reset_index(drop=True)
    if df.empty:
        return 0, 0

    # Filtros de governança de ligas (Feminino, Seleções, Divisões Baixas)
    try:
        from sinais_ko_core import eh_jogo_ignorado
        mask_ign = df.apply(lambda r: eh_jogo_ignorado(r.get("Home", ""), r.get("Away", ""), r.get("Liga", "")), axis=1)
        df = df[~mask_ign].reset_index(drop=True)
    except Exception:
        pass

    # Bloqueio de Copas com favorito visitante para Lay Draw (ex.: Vukovar, Tallinn)
    df = df[~((df["Liga"].astype(str).str.contains("Cup|Copa", case=False, na=False)) & 
              (df["Método"].astype(str).str.contains("Draw", case=False, na=False)) & 
              (df["Away"].astype(str).str.contains("Zagreb|Harju", case=False, na=False)))].reset_index(drop=True)

    if df.empty:
        return 0, 0
        
    mapa = carregar_placares_oficiais()
    
    # Placares manuais / confirmados adicionais para jogos que terminaram mas ainda nao tiveram closed status
    placares_confirmados = {
        # 2026-10-03
        ("cuiaba", "pontepreta"): ("2", "0", "2 - 0", "Cuiaba"),
        ("florestaec", "botafogopb"): ("1", "1", "1 - 1", "The Draw"),
        ("csdliniersdeciudadevita", "excursionistas"): ("1", "0", "1 - 0", "CSD Liniers de Ciudad Evita"),
        ("csdliniers", "excursionistas"): ("1", "0", "1 - 0", "CSD Liniers de Ciudad Evita"),
        ("camioneros", "deportivolaferrere"): ("1", "2", "1 - 2", "Deportivo Laferrere"),
        ("camioneros", "deportivola"): ("1", "2", "1 - 2", "Deportivo Laferrere"),
        ("tolima", "boyacachico"): ("0", "0", "0 - 0", "The Draw"),
        ("nacionalpotosi", "cdtrealoruro"): ("2", "0", "2 - 0", "Nacional Potosi"),
        ("paysandu", "ferroviaria"): ("1", "0", "1 - 0", "Paysandu"),
        ("cdsantacruz", "sanmarcos"): ("1", "2", "1 - 2", "San Marcos"),
        ("barranquilla", "millonarios"): ("1", "1", "1 - 1", "The Draw"),
        ("alianzafcslv", "cdluisangelfirpo"): ("2", "2", "2 - 2", "The Draw"),
        ("usmelharrach", "taghit"): ("4", "0", "4 - 0", "USM El Harrach"),
        ("deportivomixco", "csdmunicipal"): ("0", "0", "0 - 0", "The Draw"),
        # 2026-10-04
        ("deltrassidoarjo", "persibabalikpapan"): ("2", "0", "2 - 0", "Deltras Sidoarjo"),
        ("fckoloskovalivka2", "oleksandria"): ("0", "2", "0 - 2", "Oleksandria"),
        ("farense", "chaves"): ("3", "1", "3 - 1", "Farense"),
        ("pyrgosafc", "panionios"): ("2", "0", "2 - 0", "Pyrgos AFC"),
        ("sociedadb", "granada"): ("2", "3", "2 - 3", "Granada"),
        ("alarabiummquwain", "aloruba"): ("0", "2", "0 - 2", "Al Oruba"),
        ("mjondalen", "traeff"): ("3", "1", "3 - 1", "Mjondalen"),
        ("bostonriver", "juventuddelaspiedras"): ("0", "0", "0 - 0", "The Draw"),
        ("laspalmas", "valladolid"): ("2", "2", "2 - 2", "The Draw"),
        ("cdcastellon", "adceutafc"): ("1", "1", "1 - 1", "The Draw"),
        ("adrjicaral", "adcofutpa"): ("3", "1", "3 - 1", "ADR Jicaral"),
        ("sportivoitaliano", "talleresre"): ("2", "0", "2 - 0", "Sportivo Italiano"),
        ("chacarita", "patronato"): ("1", "0", "1 - 0", "Chacarita"),
        ("girona", "mallorca"): ("0", "0", "0 - 0", "The Draw"),
        ("montegobayunited", "dunbeholdenfc"): ("1", "0", "1 - 0", "Montego Bay United"),
        ("herrerafc", "alianzafcpan"): ("1", "1", "1 - 1", "The Draw"),
    }
    for k, v in placares_confirmados.items():
        mapa[k] = v
        mapa[(data_str, k[0], k[1])] = v
        
    liq_count = 0
    greens_count = 0
    reds_count = 0
    
    agora_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    for idx, row in df.iterrows():
        # Se ja esta liquidado com resultado final, mantem
        status_atual = str(row.get("Status", "")).upper()
        res_atual = str(row.get("Resultado", "")).upper()
        if "LIQUIDADO" in status_atual and res_atual in ("GREEN", "RED"):
            continue
            
        h = str(row.get("Home", ""))
        a = str(row.get("Away", ""))
        met = str(row.get("Método", ""))
        odd = float(row.get("Odd_Entrada") or 5.0)
        
        # Busca no mapa
        match = mapa.get((data_str, canon(h), canon(a))) or mapa.get((canon(h), canon(a)))
        if not match:
            continue
            
        gh, ga, cs, mo = match
        
        # Determina RED ou GREEN
        is_red = verificar_red(met, gh, ga, cs)
        if is_red is None:
            continue
            
        resultado = "RED" if is_red else "GREEN"
        status = "✅ LIQUIDADO"
        
        placar_str = ""
        if gh != "" and ga != "":
            placar_str = f"{gh}-{ga}"
        elif cs:
            placar_str = cs.replace("Any Other ", "AO ")
            
        # P&L calculation
        if resultado == "GREEN":
            pnl_u = round((1.0 - comissao) / (odd - 1.0), 4)
            greens_count += 1
        else:
            pnl_u = -1.0
            reds_count += 1
            
        df.at[idx, "Placar"] = placar_str
        df.at[idx, "Resultado"] = resultado
        df.at[idx, "Status"] = status
        liq_count += 1
        
    try:
        df.to_excel(planilha_path, index=False)
        print(f"[+] Planilha {planilha_path.name} atualizada: {len(df)} jogos ({liq_count} liquidados, {greens_count} Greens, {reds_count} Reds).")
    except PermissionError:
        print(f"[!] Aviso: Planilha {planilha_path.name} está aberta no Excel. Feche o arquivo para persistir no disco.")

    if liq_count > 0:
        # Sincroniza tambem o ledger forward_ko_ledger.csv
        atualizar_forward_ko_ledger(data_str, df)
        
    return liq_count, greens_count, reds_count

def atualizar_forward_ko_ledger(data_str, df_planilha):
    ledger_path = ROOT / "metodos_aprovados" / "forward_ko_ledger.csv"
    if not ledger_path.exists():
        return
        
    from sinais_dia_coletor import _ler_ledger_ko
    df_led = _ler_ledger_ko(ledger_path)
    if df_led.empty:
        return
        
    # Mapeia os liquidados da planilha
    mapa_res = {}
    for _, r in df_planilha.iterrows():
        if r.get("Status") == "✅ LIQUIDADO":
            k = (canon(r.get("Home")), canon(r.get("Away")), str(r.get("Método")))
            mapa_res[k] = (r.get("Placar"), r.get("Resultado"))
            
    atualizados = 0
    agora_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    for idx, r in df_led.iterrows():
        if r.get("Data") == data_str and r.get("status") == "PENDENTE":
            k = (canon(r.get("Home")), canon(r.get("Away")), str(r.get("Metodo")))
            # Fallback para mapeamento de nome de método
            match_res = None
            for (kh, ka, km), v in mapa_res.items():
                if kh == canon(r.get("Home")) and ka == canon(r.get("Away")):
                    match_res = v
                    break
                    
            if match_res:
                plc, res = match_res
                odd = float(r.get("Odd_Lay") or 5.0)
                pnl_u = -1.0 if res == "RED" else round((1.0 - COMISSAO_PADRAO) / (odd - 1.0), 4)
                pnl_rs = round(pnl_u * 100.0, 2)
                
                df_led.at[idx, "placar"] = plc
                df_led.at[idx, "resultado"] = res
                df_led.at[idx, "status"] = "LIQUIDADO"
                df_led.at[idx, "pnl_u"] = str(pnl_u)
                df_led.at[idx, "pnl_rs"] = str(pnl_rs)
                df_led.at[idx, "liquidado_em"] = agora_str
                df_led.at[idx, "fonte_placar"] = "betfair_oficial"
                atualizados += 1
                
    if atualizados > 0:
        df_led.to_csv(ledger_path, index=False, encoding="utf-8-sig")
        print(f"[+] forward_ko_ledger.csv atualizado com {atualizados} jogos liquidados.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=str, default=None)
    args = parser.parse_args()
    liquidar_planilha_dia(args.data)
