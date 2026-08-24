"""
gerar_sinais_local.py — Gera os sinais dos 8 metodos LOCALMENTE (sem Streamlit Cloud)
====================================================================================
Roda os 6 metodos ML (predict_and_evaluate_live) + 2 de regra (0x3, 2x2), reproduzindo
o MESMO fluxo das paginas, e salva TODOS num unico `sinais_gerados/sinais_gerados_<data>.xlsx`.
Depois o consolidar_sinais.py preenche o placar. Feito p/ rodar no Agendador de Tarefas.

Uso:
  python gerar_sinais_local.py            # jogos de HOJE
  python gerar_sinais_local.py 2026-08-24 # data especifica
"""
import os
import sys
import warnings
from datetime import datetime
import pandas as pd
import numpy as np
warnings.filterwarnings("ignore")

ROOT = os.path.dirname(os.path.abspath(__file__))
OUTDIR = os.path.join(ROOT, "sinais_gerados")
os.makedirs(OUTDIR, exist_ok=True)
RESP_PADRAO = 200.0  # responsabilidade por aposta (p/ dimensionar stake/lucro estimado)

# metodos ML: (nome, modulo da estrategia, chave da odd no dict de sinal)
ML = [
    ("Lay 0x0", "lay_0x0_rf_v2_strategy",  "Odd_CS_0x0_Lay"),
    ("Lay 0x1", "lay_0x1_rf_v2_strategy",  "Odd_CS_0x1_Lay"),
    ("Lay 1x0", "lay_1x0_rf_v2_strategy",  "Odd_CS_1x0_Lay"),
    ("Lay 2x0", "lay_2x0_rf_v2_strategy",  "Odd_CS_2x0_Lay"),
    ("Lay 0x2", "lay_0x2_rf_v2_strategy",  "Odd_CS_0x2_Lay"),
    ("Lay Draw", "lay_draw_rf_v2_strategy", "Odd_D_FT"),
]


def _row(metodo, liga, mand, vis, odd):
    odd = pd.to_numeric(odd, errors="coerce")
    stake = round(RESP_PADRAO / (odd - 1.0), 2) if pd.notna(odd) and odd > 1.0 else np.nan
    lucro = round(stake * 0.95, 2) if pd.notna(stake) else np.nan
    return {"Data": None, "Metodo": metodo, "Liga": str(liga or ""),
            "Mandante": str(mand or "").strip(), "Visitante": str(vis or "").strip(),
            "Odd": round(float(odd), 2) if pd.notna(odd) else np.nan,
            "Stake_R": stake, "Resp_R": RESP_PADRAO, "Lucro_Est_R": lucro}


def gerar(date_str):
    import b365_data_utils as B
    import hist_rf_loader
    rows = []

    # ── payload do dia + historico (compartilhado pelos 6 ML) ──
    bf = B.fetch_betfair_daily(date_str)
    if bf is None or bf.empty:
        print("  [aviso] fetch_betfair_daily vazio p/", date_str)
        payload = []
    else:
        payload = bf.to_dict("records")
    try:
        hist = hist_rf_loader.load_hist_rf()
    except Exception as e:
        print("  [ERRO] hist_rf_loader:", str(e)[:80]); hist = None

    for nome, modname, oddkey in ML:
        if not payload or hist is None:
            print("  %-10s pulado (sem payload/hist)" % nome); continue
        try:
            mod = __import__(modname, fromlist=["predict_and_evaluate_live"])
            res = mod.predict_and_evaluate_live(payload, hist)
            ap = [g for g in (res or []) if g.get("Decision") == "APOSTA"]
            for g in ap:
                rows.append(_row(nome, g.get("League"), g.get("Home"), g.get("Away"), g.get(oddkey) or g.get("Odd_D_FT")))
            print("  %-10s %d sinais" % (nome, len(ap)))
        except Exception as e:
            print("  %-10s ERRO %s" % (nome, str(e)[:70]))

    # ── df do dia p/ as regras (mesma fonte das paginas: get_daily_dataframe) ──
    try:
        from futpythontrader_client import get_daily_dataframe
        df_day = get_daily_dataframe("betfair", date_str)
    except Exception:
        df_day = bf  # fallback: o proprio feed betfair
    if df_day is None or df_day.empty:
        df_day = bf if bf is not None else pd.DataFrame()

    def _ha(r):
        for h in ("Home", "Mandante", "HomeTeam"):
            if h in r and pd.notna(r.get(h)): mand = r.get(h); break
        else: mand = ""
        for a in ("Away", "Visitante", "AwayTeam"):
            if a in r and pd.notna(r.get(a)): vis = r.get(a); break
        else: vis = ""
        return mand, vis

    # 0x3 (regra)
    try:
        import lay_goleada_quant_strategy as G
        s = G.aplicar_lay_goleada(df_day)
        for _, r in s.iterrows():
            jogo = str(r.get("jogo", "")).split(" x ")
            mand, vis = (jogo + ["", ""])[:2]
            rows.append(_row("Lay 0x3", r.get("liga"), mand, vis, r.get("odd_execucao")))
        print("  %-10s %d sinais" % ("Lay 0x3", len(s)))
    except Exception as e:
        print("  Lay 0x3    ERRO %s" % str(e)[:70])

    # 2x2 (regra)
    try:
        import metodo_lay2x2_strategy as M2
        s = M2.filtrar_grade_lay2x2(df_day)
        for _, r in s.iterrows():
            mand, vis = _ha(r)
            rows.append(_row("Lay 2x2", r.get("League", r.get("Liga", "")), mand, vis, r.get("odd_execucao")))
        print("  %-10s %d sinais" % ("Lay 2x2", len(s)))
    except Exception as e:
        print("  Lay 2x2    ERRO %s" % str(e)[:70])

    # ── salva ──
    df = pd.DataFrame([r for r in rows if r["Mandante"] and r["Visitante"]])
    if df.empty:
        print("Nenhum sinal gerado p/", date_str); return
    df["Data"] = date_str
    out = os.path.join(OUTDIR, "sinais_gerados_%s.xlsx" % date_str)
    df.to_excel(out, index=False)
    print("\n%d sinais salvos: %s" % (len(df), out))
    print(df.groupby("Metodo").size().to_string())


if __name__ == "__main__":
    d = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    print("Gerando sinais locais p/", d, "...")
    gerar(d)
