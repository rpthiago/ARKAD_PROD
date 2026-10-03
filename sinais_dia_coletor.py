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
import os, sys, re, argparse, warnings
warnings.filterwarnings("ignore")
ROOT = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, ROOT)
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass
import pandas as pd
from datetime import datetime

KO_LEDGER = os.path.join(ROOT, "metodos_aprovados", "forward_ko_ledger.csv")
SAIDA_DIR = os.path.join(ROOT, "metodos_aprovados")
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


def main():
    ap = argparse.ArgumentParser()
    datas = [a for a in sys.argv[1:] if re.fullmatch(r"\d{4}-\d{2}-\d{2}", a)]
    ap.add_argument("--separado", action="store_true",
                    help="grava como _coletor.xlsx em vez de sobrescrever a planilha do dia")
    ap.add_argument("--universo", default="todos", choices=["feed", "todos"],
                    help="todos (padrao) = tudo que o coletor viu; feed = so jogos que a API tambem enxergava "
                         "(era o recorte comparavel, mas com o feed quebrado ele zera o dia)")
    ap.add_argument("--min-liq", type=float, default=0.0, help="liquidez minima no lay")
    a = ap.parse_args([x for x in sys.argv[1:] if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", x)])
    dia = datas[0] if datas else datetime.now().strftime("%Y-%m-%d")

    if not os.path.exists(KO_LEDGER):
        print("nao achei %s — rode o relatorio das 06:00 antes (ele traz o ledger da VPS)" % KO_LEDGER); sys.exit(1)
    k = pd.read_csv(KO_LEDGER, dtype=str, encoding="utf-8-sig")
    d = k[k.Data == dia].copy()
    if d.empty:
        print("sem sinais do coletor para %s no ledger (ultima data: %s)" % (dia, k.Data.max())); sys.exit(0)

    n0 = len(d)
    if a.universo == "feed":
        d = d[d.universo.fillna("") != "fora"]
    if a.min_liq > 0:
        d = d[pd.to_numeric(d.liq_lay, errors="coerce").fillna(0) >= a.min_liq]
    n_feed = int((k[k.Data == dia].universo.fillna("") != "fora").sum())
    print("sinais do coletor em %s: %d no ledger | %d tambem estavam no feed da API | %d apos filtros (universo=%s, liq>=%.0f)"
          % (dia, n0, n_feed, len(d), a.universo, a.min_liq))
    if a.universo == "todos" and n_feed < n0:
        print("  AVISO: %d destes NAO estavam no feed da API — nao sao comparaveis com o forward da API." % (n0 - n_feed))
    if d.empty:
        print("nada a gravar."); sys.exit(0)

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

    nome = "Sinais_Metodos_Aprovados_%s%s.xlsx" % (dia, "_coletor" if a.separado else "")
    destino = os.path.join(SAIDA_DIR, nome)
    if os.path.exists(destino) and not a.separado:
        bak = destino + ".bak_" + datetime.now().strftime("%Y%m%d_%H%M")
        os.replace(destino, bak); print("  planilha anterior preservada em %s" % os.path.basename(bak))
    out.to_excel(destino, index=False)
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
