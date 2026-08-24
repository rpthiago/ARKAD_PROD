"""
consolidar_sinais.py — Uniformiza TODAS as planilhas de sinais + preenche o placar
==================================================================================
Varre as planilhas `sinais_*_YYYY-MM-DD.xlsx` (formatos diferentes por metodo),
normaliza pra UM schema, puxa o placar real do coletor Betfair (VPS) e escreve um
unico `paper_consolidado.csv`. A pagina de Resultados le so esse arquivo.

Nao precisa baixar planilha por planilha nem copiar placar do Google.

Uso:
  python consolidar_sinais.py            # ultimos 3 dias, todas as planilhas
  python consolidar_sinais.py --dias 7   # ultimos 7 dias
"""
import os
import re
import sys
import glob
import unicodedata
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import preencher_placares as PP   # reusa coletor: puxar_coletor, settle_ft, achar_placar, _canon, _green_rule

ROOT = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(ROOT, "paper_consolidado.csv")
SCHEMA = ["Data", "Metodo", "Liga", "Mandante", "Visitante", "Odd",
          "Stake_R", "Resp_R", "Lucro_Est_R", "Gols_M", "Gols_V", "Resultado", "Lucro_Real_R"]

# familias legadas/duplicadas a IGNORAR (versoes antigas do mesmo metodo)
IGNORAR = ("_gold", "_migrado", "_realista", "_legado", "_consolidado",
           "_PREENCHIDO", "Arsenal_Completo", "Preencher_Placares", "Exclusivo",
           "saldo",   # Saldo Menor = back no handicap EH+3, nao e CS -> coletor nao liquida (fora por ora)
           "over05")  # Over 0.5 tambem nao e CS lay


def _find(cols, *keys):
    for k in keys:
        for c in cols:
            if k in str(c).lower():
                return c
    return None


def _metodo_do_nome(fname):
    f = fname.lower()
    if "saldo" in f: return "Saldo Menor"
    if "draw" in f:  return "Lay Draw"
    if "over05" in f: return "Over 0.5"
    m = re.search(r'(\d)x(\d)', f)
    return "Lay %sx%s" % (m.group(1), m.group(2)) if m else fname


def normalizar(path):
    """Le uma planilha em qualquer formato e devolve linhas no schema uniforme (sem placar ainda)."""
    fname = os.path.basename(path)
    df = pd.read_excel(path)
    cols = list(df.columns)
    md = re.search(r"(\d{4}-\d{2}-\d{2})", fname)
    data_file = md.group(1) if md else ""
    c_data = _find(cols, "data")
    c_conf = _find(cols, "confront", "jogo", "partida")
    c_mand = _find(cols, "mandante", "home")
    c_vis  = _find(cols, "visitante", "away")
    c_liga = _find(cols, "liga", "league")
    c_odd  = _find(cols, "odd lay", "odd betfair", "odd favorito", "odd")
    c_stk  = _find(cols, "stake")
    c_resp = _find(cols, "respons")
    c_lest = next((c for c in cols if "lucro" in str(c).lower() and "estim" in str(c).lower()), None)
    c_met  = _find(cols, "metodo", "método")   # NAO usa "Estrategia" (vinha "XGBoost (Lay 0x0)")
    out = []
    for _, r in df.iterrows():
        if c_conf and pd.notna(r.get(c_conf)):
            partes = str(r[c_conf]).split(" x ")
            mand, vis = (partes + ["", ""])[:2]
        else:
            mand, vis = str(r.get(c_mand, "")), str(r.get(c_vis, ""))
        if not str(mand).strip() or not str(vis).strip():
            continue
        metodo = str(r[c_met]) if (c_met and pd.notna(r.get(c_met)) and str(r[c_met]).strip()) else _metodo_do_nome(fname)
        out.append({
            "Data": (str(r[c_data])[:10] if c_data and pd.notna(r.get(c_data)) else data_file),
            "Metodo": metodo, "Liga": str(r.get(c_liga, "")) if c_liga else "",
            "Mandante": str(mand).strip(), "Visitante": str(vis).strip(),
            "Odd": pd.to_numeric(r.get(c_odd), errors="coerce") if c_odd else np.nan,
            "Stake_R": pd.to_numeric(r.get(c_stk), errors="coerce") if c_stk else np.nan,
            "Resp_R": pd.to_numeric(r.get(c_resp), errors="coerce") if c_resp else np.nan,
            "Lucro_Est_R": pd.to_numeric(r.get(c_lest), errors="coerce") if c_lest else np.nan,
        })
    return out, fname


def main():
    dias = 3
    if "--dias" in sys.argv:
        try: dias = int(sys.argv[sys.argv.index("--dias") + 1])
        except Exception: pass
    min_date = (datetime.now().date() - timedelta(days=dias)).strftime("%Y-%m-%d")
    todos = glob.glob(os.path.join(ROOT, "**", "sinais_*.xlsx"), recursive=True)
    alvos = [p for p in todos
             if not any(ig.lower() in os.path.basename(p).lower() for ig in IGNORAR)
             and (re.search(r"(\d{4}-\d{2}-\d{2})", p) or [""])[0] >= min_date]
    if not alvos:
        print("Nenhuma planilha sinais_*.xlsx dos ultimos %d dias." % dias); return
    print("Consolidando %d planilhas (desde %s)..." % (len(alvos), min_date))
    linhas = []
    for p in sorted(alvos):
        try:
            rows, fn = normalizar(p); linhas += rows
            print("  + %-45s %3d sinais" % (fn, len(rows)))
        except Exception as e:
            print("  ! %s ERRO %s" % (os.path.basename(p), str(e)[:50]))
    cons = pd.DataFrame(linhas, columns=[c for c in SCHEMA if c not in ("Gols_M","Gols_V","Resultado","Lucro_Real_R")])
    cons = cons.drop_duplicates(subset=["Data", "Metodo", "Mandante", "Visitante"]).reset_index(drop=True)
    print("\nTotal sinais uniformizados: %d | metodos: %s" % (len(cons), sorted(cons["Metodo"].unique())))

    # ── placar do coletor ──
    print("\nPuxando placares do coletor Betfair (VPS)...")
    cs = PP.puxar_coletor(min_date)
    ft = PP.settle_ft(cs) if cs is not None and not cs.empty else None
    gm, gv, res, pnl = [], [], [], []
    for _, r in cons.iterrows():
        gh = ga = None
        if ft is not None:
            gh, ga = PP.achar_placar(ft, str(r["Data"])[:10], PP._canon(r["Mandante"]), PP._canon(r["Visitante"]))
        if gh is None:
            gm.append(""); gv.append(""); res.append("SEM_PLACAR"); pnl.append(np.nan); continue
        gfn, _ = PP._green_rule(r["Metodo"].lower().replace(" ", "").replace("lay", "lay"))
        if gfn is None:  # metodos nao-CS (Saldo/Over): sem regra CS -> deixa placar, resultado manual
            gm.append(gh); gv.append(ga); res.append("PLACAR_OK"); pnl.append(np.nan); continue
        g = bool(gfn(gh, ga))
        stake = 100.0
        if g:
            p = float(r["Lucro_Est_R"]) if pd.notna(r["Lucro_Est_R"]) else stake * 0.95
        else:
            p = -float(r["Resp_R"]) if pd.notna(r["Resp_R"]) else (-(r["Odd"] - 1) * stake if pd.notna(r["Odd"]) else np.nan)
        gm.append(gh); gv.append(ga); res.append("GREEN" if g else "RED"); pnl.append(round(p, 2) if pd.notna(p) else np.nan)
    cons["Gols_M"], cons["Gols_V"], cons["Resultado"], cons["Lucro_Real_R"] = gm, gv, res, pnl
    cons = cons[SCHEMA]
    cons.to_csv(OUT, index=False, encoding="utf-8-sig")
    comp = (cons["Resultado"] != "SEM_PLACAR").sum()
    print("\n=== RESUMO ===")
    print("consolidado: %d sinais | com placar: %d (%.0f%%) | salvo: %s" % (len(cons), comp, comp/len(cons)*100, os.path.basename(OUT)))
    r = cons[cons["Resultado"].isin(["GREEN", "RED"])]
    if len(r):
        g = r.groupby("Metodo").agg(sinais=("Resultado", "size"),
                                    green=("Resultado", lambda s: (s == "GREEN").sum()),
                                    lucro=("Lucro_Real_R", "sum"))
        g["WR%"] = (g["green"] / g["sinais"] * 100).round(0)
        print(g[["sinais", "WR%", "lucro"]].to_string())
        print("LUCRO REAL TOTAL: R$ %+.0f" % r["Lucro_Real_R"].sum())
    faltam = cons[cons["Resultado"] == "SEM_PLACAR"]
    if len(faltam):
        print("\nsem placar (conferir): %d" % len(faltam))


if __name__ == "__main__":
    main()
