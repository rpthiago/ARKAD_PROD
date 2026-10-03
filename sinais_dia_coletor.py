# -*- coding: utf-8 -*-
"""
sinais_dia_coletor.py — monta a PLANILHA DO DIA a partir do ledger do KO-10 (coletor da VPS),
para quando o feed "jogos-do-dia" da API estiver vazio.

Por que existe: desde 21/09/2026 o endpoint jogos-do-dia da apicomunidade passou a devolver uma
fracao do dia (em 03/10, 6 jogos de betfair contra 208 que o coletor enxerga). Os metodos escaneiam
esse feed, entao a pagina 01 fecha com "nenhum sinal qualificado" e a planilha diaria deixa de ser
gerada — o forward para de acumular amostra justamente nos fins de semana. O servico `sinais-ko` na
VPS, que le o coletor e avalia as MESMAS regras congeladas na primeira captura 4-16 min antes do KO,
continua produzindo normalmente (241 sinais nos ultimos 7 dias). Este script transforma esse registro
na planilha do dia, no formato que a pagina 02 e o stop diario ja leem.

BASE DIFERENTE, E ISSO FICA MARCADO: a coluna `Origem` sai como "coletor (KO-10)" em toda linha, e o
nome do arquivo ganha o sufixo _coletor quando --separado. Nao misture estes sinais com o forward da
API ao medir metodo (regra: uma base por metodo).

  python sinais_dia_coletor.py [YYYY-MM-DD] [--separado] [--universo feed|todos]
"""
import os, sys, re, argparse, csv, warnings
warnings.filterwarnings("ignore")
ROOT = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, ROOT)
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass
import pandas as pd
from datetime import datetime

KO_LEDGER = os.path.join(ROOT, "metodos_aprovados", "forward_ko_ledger.csv")
SAIDA_DIR = os.path.join(ROOT, "metodos_aprovados")

def _ler_ledger_ko(caminho=KO_LEDGER) -> pd.DataFrame:
    """Lê o forward_ko_ledger.csv de forma tolerante a linhas com números variáveis de colunas."""
    if not os.path.exists(caminho):
        return pd.DataFrame()
    cols_24 = [
        "Data", "Metodo", "Liga", "Home", "Away", "Hora", "Odd_Lay", "Odd_Fav", "liq_lay",
        "min_to_ko", "ts_captura", "origem", "status", "gols_H", "gols_A", "placar",
        "resultado", "pnl_u", "pnl_rs", "break_even", "liquidado_em", "fonte_placar",
        "universo", "no_0600"
    ]
    rows = []
    with open(caminho, encoding="utf-8-sig", errors="replace") as f:
        r = csv.reader(f)
        try:
            next(r)
        except StopIteration:
            return pd.DataFrame()
        for row in r:
            if not row:
                continue
            if len(row) < 24:
                row.extend([""] * (24 - len(row)))
            rows.append(row[:24])
    return pd.DataFrame(rows, columns=cols_24)
# nomes como a pagina 02 e o stop diario esperam
NOMES = {
    "Lay 0x3 Top 3": "Lay 0x3 Top 3 (Aprovado)",
    "Lay 2x2 Top 3": "Lay 2x2 Top 3 (Aprovado)",
    "Lay Draw (Fav<=1.40)": "Lay Draw (Fav <= 1.40)",
    "Lay Home/DC X2 (FavVis<=1.65)": "Lay Home/DC X2 (FavVis <= 1.65)",
    "Lay Over 4.5 (Under Pesado)": "Lay Over 4.5 FT (Under Pesado)",
    "Lay 0x3 (Regra Ampla)": "Lay 0x3 (Regra Ampla)",
}
COLS = ["Data", "Hora", "Liga", "Jogo", "Home", "Away", "Método", "Mercado", "Lado",
        "Odd_Entrada", "Odd_Fav", "Placar", "Resultado", "Status", "Origem", "Liquidez_Lay"]


import subprocess

def sincronizar_ledger_vps() -> bool:
    """Baixa o forward_ko_ledger.csv mais recente da VPS para a pasta metodos_aprovados."""
    vps = "ubuntu@163.176.59.215"
    key = os.path.expanduser("~/Downloads/ssh-key-2026-07-31.key")
    if not os.path.exists(key):
        key = r"C:\Users\thiag\Downloads\ssh-key-2026-07-31.key"
    if not os.path.exists(key):
        return False
    remoto = f"{vps}:/home/ubuntu/betfair-collector/forward_ko_ledger.csv"
    local_destino = os.path.join(ROOT, "metodos_aprovados", "forward_ko_ledger.csv")
    try:
        cmd = ["scp", "-i", key, "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=10", remoto, local_destino]
        res = subprocess.run(cmd, capture_output=True, timeout=20)
        return res.returncode == 0
    except Exception:
        return False


def gerar_planilha_do_coletor(dia=None, separado=False, universo="todos", min_liq=0.0, sync_vps=True) -> pd.DataFrame:
    """Gera a planilha Sinais_Metodos_Aprovados_YYYY-MM-DD.xlsx a partir do ledger da VPS."""
    if dia is None:
        dia = datetime.now().strftime("%Y-%m-%d")
        
    if sync_vps:
        sincronizar_ledger_vps()
        
    if not os.path.exists(KO_LEDGER):
        return pd.DataFrame()
        
    k = _ler_ledger_ko(KO_LEDGER)
    d = k[k.Data == dia].copy()
    if d.empty:
        return pd.DataFrame()

    if universo == "feed":
        d = d[d.universo.fillna("") != "fora"]
    if min_liq > 0:
        d = d[pd.to_numeric(d.liq_lay, errors="coerce").fillna(0) >= min_liq]
    if d.empty:
        return pd.DataFrame()

    out = pd.DataFrame({
        "Data": d.Data,
        "Hora": d.Hora,
        "Liga": d.Liga,
        "Jogo": d.Home.astype(str) + " x " + d.Away.astype(str),
        "Home": d.Home,
        "Away": d.Away,
        "Método": d.Metodo.map(lambda m: NOMES.get(m, m)),
        "Mercado": None,
        "Lado": "LAY",
        "Odd_Entrada": pd.to_numeric(d.Odd_Lay, errors="coerce"),
        "Odd_Fav": pd.to_numeric(d.Odd_Fav, errors="coerce"),
        "Placar": d.placar.fillna(""),
        "Resultado": d.resultado.fillna("PENDENTE").replace("", "PENDENTE"),
        "Status": d.status.map(lambda s: "✅ LIQUIDADO" if s == "LIQUIDADO" else "⏳ PENDENTE"),
        "Origem": "coletor (KO-10)",
        "Liquidez_Lay": pd.to_numeric(d.liq_lay, errors="coerce"),
    })[COLS].sort_values(["Hora", "Método"])

    nome = "Sinais_Metodos_Aprovados_%s%s.xlsx" % (dia, "_coletor" if separado else "")
    destino = os.path.join(SAIDA_DIR, nome)
    if os.path.exists(destino) and not separado:
        bak = destino + ".bak_" + datetime.now().strftime("%Y%m%d_%H%M")
        try:
            os.replace(destino, bak)
        except Exception:
            pass
    out.to_excel(destino, index=False)
    return out


def main():
    ap = argparse.ArgumentParser()
    datas = [a for a in sys.argv[1:] if re.fullmatch(r"\d{4}-\d{2}-\d{2}", a)]
    ap.add_argument("--separado", action="store_true",
                    help="grava como _coletor.xlsx em vez de sobrescrever a planilha do dia")
    ap.add_argument("--universo", default="todos", choices=["feed", "todos"],
                    help="todos (padrao) = tudo que o coletor viu; feed = so jogos que a API tambem enxergava "
                         "(era o recorte comparavel, mas com o feed quebrado ele zera o dia)")
    ap.add_argument("--min-liq", type=float, default=0.0, help="liquidez minima no lay")
    ap.add_argument("--no-sync", action="store_true", help="nao sincroniza ledger da VPS antes de gerar")
    a = ap.parse_args([x for x in sys.argv[1:] if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", x)])
    dia = datas[0] if datas else datetime.now().strftime("%Y-%m-%d")

    out = gerar_planilha_do_coletor(
        dia=dia,
        separado=a.separado,
        universo=a.universo,
        min_liq=a.min_liq,
        sync_vps=(not a.no_sync)
    )

    if out.empty:
        print("sem sinais do coletor para %s ou nada a gravar." % dia)
        sys.exit(0)

    nome = "Sinais_Metodos_Aprovados_%s%s.xlsx" % (dia, "_coletor" if a.separado else "")
    print("-> %s (%d linhas)" % (nome, len(out)))
    print("\npor metodo:")
    for m, g in out.groupby("Método"):
        liq = g.Liquidez_Lay.median()
        print("  %-36s %3d sinais | odd mediana %5.2f | liquidez mediana %7.0f"
              % (m[:36], len(g), g.Odd_Entrada.median(), liq if pd.notna(liq) else 0))
    pend = (out.Resultado == "PENDENTE").sum()
    print("\npendentes de liquidacao: %d de %d (o relatorio das 06:00 liquida pelo placar oficial)" % (pend, len(out)))


if __name__ == "__main__":
    main()
