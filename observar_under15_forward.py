"""
observar_under15_forward.py — Observacao STAKE-ZERO do Lay Under 1.5 FT (XGBoost)
=================================================================================
Candidato em watchlist (nao aposta). Forward HONESTO:

- **stake = 0** (observacao pura; nunca R$ real).
- **So registra jogo cuja data ainda NAO chegou** (`Date >= hoje`). O feed diario nao traz placar,
  entao a DATA e o unico guard confiavel contra logar jogo ja jogado como se fosse pendente
  (foi o furo do gerar_sinais_forward_diario do Gemini, que re-pontuava historico como "forward").
- Liquida os pendentes lendo a BASE DE RESULTADOS (FRESH), nao o feed do dia — porque o feed
  diario so tem os jogos daquele dia (sem placar). GREEN se sair >=2 gols; comissao 4,5%.
- SO o Lay Under 1.5 (EV>=5%), isolado das miragens (0x3/2x2/BTTS ficam de fora).

Dependencia: precisa de um FEED com jogos FUTUROS + odd Betfair + as features do modelo
(dataset_leak_free_features.parquet regenerado com os proximos jogos). Enquanto o feed so
tiver historico, o observador loga 0 (correto — nada forward ainda).

Uso: python observar_under15_forward.py [--feed CAMINHO]
"""
import sys, re, unicodedata, argparse
from datetime import date
from pathlib import Path
import numpy as np
import pandas as pd
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass

ROOT = Path(__file__).resolve().parent
LOG = ROOT / "observacao_under15_forward.csv"
FEED_DEFAULT = ROOT / "scratch" / "dataset_leak_free_features.parquet"
RESULTS_BASE = ROOT / "Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH.csv"  # fonte de placar p/ liquidar
COMM = 0.045
EV_MIN = 0.05

from estrategia_lay_under15 import avaliar_jogo_lay_under15


def _canon(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)


def settle_pendentes(log):
    """Liquida os pendentes cujo placar ja existe na base de resultados (nao no feed do dia)."""
    pend = log[log["status"] == "Pendente"]
    if len(pend) == 0 or not RESULTS_BASE.exists():
        return log, 0
    rb = pd.read_csv(RESULTS_BASE, low_memory=False,
                     usecols=lambda c: c in ["Date", "Home", "Away", "Goals_H_FT", "Goals_A_FT"])
    rb["Date"] = pd.to_datetime(rb["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    rb = rb.dropna(subset=["Goals_H_FT", "Goals_A_FT"])
    res = {}
    for _, r in rb.iterrows():
        res[(r["Date"], _canon(r["Home"]), _canon(r["Away"]))] = (r["Goals_H_FT"], r["Goals_A_FT"])
    n = 0
    for i in pend.index:
        d = str(log.loc[i, "data"]); h, a = str(log.loc[i, "jogo"]).split(" x ", 1) if " x " in str(log.loc[i, "jogo"]) else ("", "")
        hit = res.get((d, _canon(h), _canon(a)))
        if hit is None:
            continue
        gh, ga = hit; ol = float(log.loc[i, "odd_lay"]); green = (gh + ga) >= 2
        log.loc[i, "status"] = "Finalizado"
        log.loc[i, "resultado"] = "GREEN" if green else "RED"
        log.loc[i, "pnl_unidades"] = round((1 - COMM) if green else -(ol - 1), 4)
        n += 1
    return log, n


def _col(row, *names, default=None):
    for n in names:
        v = row.get(n)
        if v is not None and not (isinstance(v, float) and pd.isna(v)):
            return v
    return default


def load_feed(path):
    p = Path(path)
    if not p.exists():
        print(f"[erro] feed nao encontrado: {p}"); return None
    df = pd.read_parquet(p) if p.suffix == ".parquet" else pd.read_csv(p, low_memory=False)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    return df.dropna(subset=["Date"])


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--feed", default=str(FEED_DEFAULT))
    feed = ap.parse_args().feed
    df = load_feed(feed)
    if df is None or df.empty:
        print("feed vazio."); return
    hoje = date.today()

    log = pd.read_csv(LOG) if LOG.exists() else pd.DataFrame(
        columns=["data", "jogo", "liga", "odd_lay", "prob_ml", "ev", "stake",
                 "primeiro_visto", "status", "resultado", "pnl_unidades"])
    vistos = set(log["jogo"] + "|" + log["data"].astype(str)) if len(log) else set()

    novos, ignorados_passado = 0, 0
    for _, row in df.iterrows():
        d = row["Date"].strftime("%Y-%m-%d")
        home = str(_col(row, "Home_Team", "Home", "Mandante", default="?"))
        away = str(_col(row, "Away_Team", "Away", "Visitante", default="?"))
        jogo = f"{home} x {away}"; key = f"{jogo}|{d}"
        if key in vistos:                       # ja registrado -> liquidacao e feita a parte
            continue
        # GUARD FORWARD: so registra jogo que ainda NAO comecou (data >= hoje).
        # O feed diario nao traz placar, entao "data no passado" e o unico guard confiavel
        # contra logar jogo ja jogado como se fosse pendente.
        if row["Date"].date() < hoje:
            ignorados_passado += 1; continue
        ev = avaliar_jogo_lay_under15(row.to_dict(), ev_threshold=EV_MIN)
        if not ev.get("aplica"):
            continue
        log = pd.concat([log, pd.DataFrame([{
            "data": d, "jogo": jogo, "liga": str(_col(row, "League", "Liga", default="?")),
            "odd_lay": round(ev["odd_lay"], 2), "prob_ml": round(ev["prob_estimada"], 4),
            "ev": round(ev["ev"], 4), "stake": 0, "primeiro_visto": hoje.isoformat(),
            "status": "Pendente", "resultado": "Pendente", "pnl_unidades": 0.0}])], ignore_index=True)
        novos += 1

    # liquida pendentes cujo placar ja saiu (da base de resultados, NAO do feed do dia)
    log, liquidados = settle_pendentes(log)

    log.to_csv(LOG, index=False, encoding="utf-8-sig")
    print(f"novos pendentes (forward): {novos} | liquidados hoje: {liquidados} | "
          f"ignorados (jogo no passado, NAO forward): {ignorados_passado}")

    fin = log[log["status"] == "Finalizado"]
    print(f"\n=== OBSERVACAO Under 1.5 FORWARD (stake-zero) ===")
    print(f"total no log: {len(log)} | pendentes: {(log['status']=='Pendente').sum()} | liquidados: {len(fin)}")
    if len(fin) >= 1:
        wr = (fin["resultado"] == "GREEN").mean() * 100
        roi = fin["pnl_unidades"].mean() * 100
        be = ((fin["odd_lay"] - 1) / (fin["odd_lay"] - COMM)).mean() * 100
        print(f"forward liquidado: N={len(fin)} WR={wr:.1f}% BE={be:.1f}% margem={wr-be:+.1f}% ROI={roi:+.1f}%")
        if len(fin) >= 100:
            print("  (N>=100: hora de rodar bootstrap+FDR e decidir promover ou matar)")
    else:
        print("(sem liquidados ainda — o forward comeca a valer quando o feed trouxer jogos futuros)")


if __name__ == "__main__":
    main()
