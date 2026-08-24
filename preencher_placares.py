"""
preencher_placares.py — Preenche o PLACAR e o resultado das planilhas de sinais
================================================================================
Acaba com o trabalho manual de googlar jogo por jogo. Puxa o placar REAL do coletor
Betfair (VPS) — que grava ao vivo, então tem o resultado minutos apos o apito, inclusive
de ligas obscuras — e preenche cada planilha `sinais_*_YYYY-MM-DD.xlsx`.

Placar FT = runner de Correct Score com menor lay no fim do jogo (mais provavel = final).
Match por nome: exato (canon) -> prefixo (fuzzy). Games nao casados sao listados p/ conferir.

Uso:
  python preencher_placares.py                       # todas as planilhas dos ultimos 3 dias
  python preencher_placares.py caminho/sinais_x.xlsx # uma planilha especifica
"""
import os
import re
import sys
import glob
import subprocess
import unicodedata
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# ── VPS ───────────────────────────────────────────────────────────────────────
VPS = "ubuntu@163.176.59.215"
KEY = os.path.expanduser("~/Downloads/ssh-key-2026-07-31.key")
COLLECTOR_CSV = "/home/ubuntu/betfair-collector/betfair_live_odds.csv"
ROOT = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(ROOT, "_placares_coletor_cache.csv")
COMMISSION = 0.05

# metodo (do nome do arquivo) -> funcao green (o LAY ganha) a partir de (gh, ga)
def _green_rule(fname):
    f = fname.lower()
    m = re.search(r'(\d)x(\d)', f)
    if "draw" in f:                       return lambda gh, ga: gh != ga, "Lay Draw"
    if m:
        H, A = int(m.group(1)), int(m.group(2))
        return (lambda gh, ga: not (gh == H and ga == A)), f"Lay {H}x{A}"
    return None, None


def _canon(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)


def _pref(a, b):
    if not a or not b:
        return False
    s, l = (a, b) if len(a) <= len(b) else (b, a)
    return len(s) >= 5 and l.startswith(s)


def puxar_coletor(min_date):
    """Puxa do VPS as capturas de Correct Score no fim de jogo (min_to_ko<-70) a partir de min_date.
    Schema atual = 15 colunas (lay=$12). Devolve DataFrame ou None."""
    awk = (r'BEGIN{FS=","} $2=="CORRECT_SCORE" && ($7+0)<-70 && $6>="%s" '
           r'{print $6","$4","$5","$7","$8","$12}') % min_date
    cmd = ["ssh", "-i", KEY, "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=20",
           VPS, "awk '%s' %s" % (awk, COLLECTOR_CSV)]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        linhas = [l for l in out.stdout.splitlines() if l.strip()]
        if not linhas:
            print("  [aviso] coletor nao retornou linhas; tentando cache local")
            return pd.read_csv(CACHE) if os.path.exists(CACHE) else None
        df = pd.DataFrame([l.split(",") for l in linhas],
                          columns=["ko", "home", "away", "min_to_ko", "runner", "lay"])
        df.to_csv(CACHE, index=False)  # cache p/ rodar de novo sem VPS
        return df
    except Exception as e:
        print("  [aviso] falha no VPS (%s); usando cache local" % str(e)[:60])
        return pd.read_csv(CACHE) if os.path.exists(CACHE) else None


def settle_ft(cs):
    """Placar FT por jogo = runner CS de menor lay nas capturas do fim do jogo."""
    cs = cs[cs["runner"].astype(str).str.match(r"^\d+ - \d+$", na=False)].copy()
    cs["lay"] = pd.to_numeric(cs["lay"], errors="coerce")
    cs["min_to_ko"] = pd.to_numeric(cs["min_to_ko"], errors="coerce")
    cs["ko"] = pd.to_datetime(cs["ko"], errors="coerce")
    cs["d"] = cs["ko"].dt.strftime("%Y-%m-%d")
    cs["ch"] = cs["home"].map(_canon)
    cs["ca"] = cs["away"].map(_canon)
    cs = cs.dropna(subset=["lay"])
    end = cs.sort_values("min_to_ko").groupby(["d", "ch", "ca"]).head(60)
    idx = end.groupby(["d", "ch", "ca"])["lay"].idxmin()
    ft = end.loc[idx, ["d", "ch", "ca", "runner"]].copy()
    ft["gh"] = ft["runner"].str.split(" - ").str[0].astype(int)
    ft["ga"] = ft["runner"].str.split(" - ").str[1].astype(int)
    return ft


def achar_placar(ft, d, ch, ca):
    exato = ft[(ft["d"] == d) & (ft["ch"] == ch) & (ft["ca"] == ca)]
    if len(exato):
        r = exato.iloc[0]; return int(r["gh"]), int(r["ga"])
    cand = ft[ft["d"] == d]
    for _, r in cand.iterrows():
        if _pref(ch, r["ch"]) and _pref(ca, r["ca"]):
            return int(r["gh"]), int(r["ga"])
    return None, None


def preencher(path, ft):
    fname = os.path.basename(path)
    green_fn, metodo = _green_rule(fname)
    if green_fn is None:
        print("  [pular] %s: metodo nao reconhecido no nome" % fname); return
    # data do jogo = do nome do arquivo (YYYY-MM-DD)
    md = re.search(r"(\d{4}-\d{2}-\d{2})", fname)
    d = md.group(1) if md else None
    df = pd.read_excel(path)
    cconf = [c for c in df.columns if "confront" in c.lower() or c.lower() in ("jogo", "partida")]
    if not cconf:
        print("  [pular] %s: sem coluna Confronto" % fname); return
    cc = cconf[0]
    codd = [c for c in df.columns if "odd" in c.lower()]
    cresp = [c for c in df.columns if "respons" in c.lower()]
    clucro = [c for c in df.columns if "lucro" in c.lower() and "estim" in c.lower()]
    gols_m, gols_v, res, pnl, casou = [], [], [], [], 0
    for _, row in df.iterrows():
        conf = str(row[cc]).split(" x ")
        if len(conf) < 2:
            gols_m.append(""); gols_v.append(""); res.append(""); pnl.append(""); continue
        ch, ca = _canon(conf[0]), _canon(conf[1])
        gh, ga = achar_placar(ft, d, ch, ca)
        if gh is None:
            gols_m.append(""); gols_v.append(""); res.append("SEM_PLACAR"); pnl.append(""); continue
        casou += 1
        g = bool(green_fn(gh, ga))
        odd = pd.to_numeric(row[codd[0]], errors="coerce") if codd else np.nan
        stake = 100.0
        if g:
            p = float(row[clucro[0]]) if clucro and pd.notna(row[clucro[0]]) else stake * (1 - COMMISSION)
        else:
            p = -float(row[cresp[0]]) if cresp and pd.notna(row[cresp[0]]) else (-(odd - 1) * stake if pd.notna(odd) else np.nan)
        gols_m.append(gh); gols_v.append(ga); res.append("GREEN" if g else "RED"); pnl.append(round(p, 2))
    df["Gols_Mandante"] = gols_m
    df["Gols_Visitante"] = gols_v
    df["Resultado"] = res
    df["Lucro_Real_R$"] = pnl
    out = path.replace(".xlsx", "_PREENCHIDO.xlsx")
    df.to_excel(out, index=False)
    tot = len(df); sem = res.count("SEM_PLACAR")
    grn = res.count("GREEN"); red = res.count("RED")
    lucro = sum(x for x in pnl if isinstance(x, (int, float)))
    faltando = [str(df.iloc[i][cc]) for i, r in enumerate(res) if r == "SEM_PLACAR"]
    print("  %s [%s]: %d/%d com placar | GREEN %d RED %d | Lucro R$ %+.0f" % (fname, metodo, casou, tot, grn, red, lucro))
    if faltando:
        print("     conferir manualmente (%d): %s" % (len(faltando), " | ".join(faltando)))
    print("     -> salvo: %s" % os.path.basename(out))


def main():
    hoje = datetime.now().date()
    min_date = (hoje - timedelta(days=3)).strftime("%Y-%m-%d")
    if len(sys.argv) > 1:
        alvos = [sys.argv[1]]
    else:
        alvos = [p for p in glob.glob(os.path.join(ROOT, "**", "sinais_*.xlsx"), recursive=True)
                 if (re.search(r"(\d{4}-\d{2}-\d{2})", p) or [""])[0] >= min_date
                 and "_PREENCHIDO" not in p]
    if not alvos:
        print("Nenhuma planilha sinais_*.xlsx dos ultimos 3 dias encontrada."); return
    print("Puxando placares do coletor Betfair (VPS) desde %s..." % min_date)
    cs = puxar_coletor(min_date)
    if cs is None or cs.empty:
        print("Sem dados do coletor (VPS off e sem cache)."); return
    ft = settle_ft(cs)
    print("Placares FT disponiveis: %d jogos\n" % len(ft))
    for p in alvos:
        preencher(p, ft)


if __name__ == "__main__":
    main()
