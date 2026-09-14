# -*- coding: utf-8 -*-
"""
auditar_top3_cs.py — auditoria do ranking TOP 3 por menor odd nos metodos Lay 2x2 e Lay 0x3.
Usa as PROPRIAS funcoes dos modulos (estrategia_lay_2x2 / estrategia_lay_0x3) dia a dia sobre a
base historica da Betfair (apicomunidade, odds de lay reais + Goals_*_FT), e o ledger do forward.
  - MANTIDOS  = o que a funcao devolve com top_n=3 (ranking + desempate por horario, codigo real)
  - QUALIFICADOS = o que a funcao devolve com top_n=None (todos que passam os filtros)
  - DESCARTADOS = qualificados - mantidos
P&L: LIABILITY = 1u, comissao 5% (Lei no 4). GREEN = 0,95/(odd-1); RED = -1.
Bootstrap bloco-dia 10.000x. Saida: tabelas + varredura_over/auditoria_top3_cs_<data>.csv
"""
import os, sys, warnings
from datetime import datetime
warnings.filterwarnings("ignore"); sys.stdout.reconfigure(encoding="utf-8")
ROOT = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, ROOT)
import pandas as pd, numpy as np
from estrategia_lay_2x2 import avaliar_jogos_lay_2x2_grade
from estrategia_lay_0x3 import avaliar_jogos_lay_0x3_grade

C = 0.05
BASE = os.path.join(ROOT, "metodos_aprovados", ".cache_base_betfair.csv")
B = 10000


def red(m, gh, ga):
    return (gh == 2 and ga == 2) if m == "2x2" else (gh == 0 and ga == 3)


def boot(v, blocos, seed=5):
    mm = {k: v[blocos == k] for k in pd.unique(blocos)}; ks = list(mm)
    if len(ks) < 6: return np.nan, np.nan, np.nan, len(ks)
    rng = np.random.default_rng(seed); r = np.empty(B)
    for i in range(B): r[i] = np.concatenate([mm[ks[j]] for j in rng.integers(0, len(ks), len(ks))]).mean()
    return np.percentile(r, 2.5), np.percentile(r, 97.5), float((r <= 0).mean()), len(ks)


def stats(df, tag):
    if len(df) == 0: return dict(grupo=tag, N=0)
    wr = df.green.mean(); be = df.be.mean(); roi = df.pnl.mean()
    lo, hi, p0, nk = boot(df.pnl.values, df.Date.values)
    return dict(grupo=tag, N=len(df), G=int(df.green.sum()), R=int((1 - df.green).sum()), WR=100 * wr, BE=100 * be,
                gap_pp=100 * (wr - be), ROI=100 * roi, PnL_u=df.pnl.sum(), odd_med=df.odd.median(),
                IC_lo=100 * lo if np.isfinite(lo) else np.nan, IC_hi=100 * hi if np.isfinite(hi) else np.nan,
                p_roi_le0=p0, dias=nk, greens_por_red=(1 / (0.95 / (df.odd.median() - 1))))


def rodar(base, m, desde=None):
    fn = avaliar_jogos_lay_2x2_grade if m == "2x2" else avaliar_jogos_lay_0x3_grade
    rows = []
    for d, g in base.groupby("Date"):
        if desde and d < desde: continue
        g = g.reset_index(drop=True)
        todos = fn(g, top_n=None); top3 = fn(g, top_n=3)
        kept = {(s["home"], s["away"]) for s in top3}
        rank = {(s["home"], s["away"]): i + 1 for i, s in enumerate(sorted(todos, key=lambda s: s["odd_lay"]))}
        sc = {(r.Home, r.Away): (r.gh, r.ga) for r in g.itertuples()}
        for s in todos:
            k = (s["home"], s["away"]); gh, ga = sc.get(k, (np.nan, np.nan))
            if not (np.isfinite(gh) and np.isfinite(ga)): continue
            odd = float(s["odd_lay"]); rd = red(m, int(gh), int(ga))
            rows.append(dict(Date=d, m=m, Home=k[0], Away=k[1], Hora=s["hora"], odd=odd, rank=rank[k], kept=k in kept,
                             green=0 if rd else 1, pnl=-1.0 if rd else (1 - C) / (odd - 1), be=(odd - 1) / (odd - C),
                             n_qualif_dia=len(todos)))
    return pd.DataFrame(rows)


def main():
    base = pd.read_csv(BASE, low_memory=False)
    base["Date"] = pd.to_datetime(base["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    base["gh"] = pd.to_numeric(base["Goals_H_FT"], errors="coerce"); base["ga"] = pd.to_numeric(base["Goals_A_FT"], errors="coerce")
    base = base.dropna(subset=["Date", "gh", "ga"])
    print("base Betfair: %d jogos com placar | %s -> %s" % (len(base), base.Date.min(), base.Date.max()))
    out = []; full = []
    for m in ("2x2", "0x3"):
        df = rodar(base, m); full.append(df)
        print("\n" + "=" * 100); print("LAY %s — base completa (%s -> %s): %d qualificados em %d dias | media %.1f qualificados/dia | mantidos (TOP3) %d"
              % (m, df.Date.min(), df.Date.max(), len(df), df.Date.nunique(), df.groupby("Date").size().mean(), df.kept.sum()))
        print("=" * 100)
        for periodo, sub in [("completo", df), ("2026", df[df.Date >= "2026-01-01"]), ("forward 01/08+", df[df.Date >= "2026-08-01"])]:
            for tag, s in [("QUALIFICADOS (sem ranking)", sub), ("MANTIDOS (rank 1-3)", sub[sub.kept]), ("DESCARTADOS (rank >3)", sub[~sub.kept])]:
                st = stats(s, tag); st.update(metodo=m, periodo=periodo); out.append(st)
                if st["N"]:
                    ic = "[%+.1f,%+.1f] p=%.3f" % (st["IC_lo"], st["IC_hi"], st["p_roi_le0"]) if np.isfinite(st["IC_lo"]) else "IC indef (%dd)" % st["dias"]
                    print("  %-15s %-27s N=%5d %4dG/%3dR WR=%5.1f%% BE=%5.1f%% gap=%+5.1fpp ROI=%+6.2f%% PnL=%+7.2fu odd_med=%5.2f  IC95 %s"
                          % (periodo, tag, st["N"], st["G"], st["R"], st["WR"], st["BE"], st["gap_pp"], st["ROI"], st["PnL_u"], st["odd_med"], ic))
        # ranking: por posicao
        print("  por posicao no ranking (base completa):")
        for r_, g in df.groupby(df["rank"].clip(upper=6)):
            print("    rank %s%s N=%5d WR=%5.1f%% BE=%5.1f%% gap=%+5.1fpp ROI=%+6.2f%% odd_med=%5.2f"
                  % (r_, "+" if r_ == 6 else " ", len(g), 100 * g.green.mean(), 100 * g.be.mean(), 100 * (g.green.mean() - g.be.mean()), 100 * g.pnl.mean(), g.odd.median()))
        # o paradoxo do preco: WR e break-even por faixa de odd
        print("  por faixa de odd (qualificados, base completa):")
        faixas = [(8, 11), (11, 14), (14, 18), (18, 22), (22, 27), (27, 36)]
        for lo_, hi_ in faixas:
            g = df[(df.odd >= lo_) & (df.odd < hi_)]
            if len(g) < 30: continue
            print("    odd %2d-%2d N=%5d P(placar)=%5.2f%% BE=%5.1f%% WR=%5.1f%% gap=%+5.1fpp ROI=%+6.2f%%"
                  % (lo_, hi_, len(g), 100 * (1 - g.green.mean()), 100 * g.be.mean(), 100 * g.green.mean(), 100 * (g.green.mean() - g.be.mean()), 100 * g.pnl.mean()))
        # greens para pagar 1 red
        print("  greens necessarios para pagar 1 red: odd %.0f -> %.1f | odd %.0f -> %.1f | odd mediana dos mantidos %.1f -> %.1f"
              % (df.odd.min(), (df.odd.min() - 1) / 0.95, df.odd.max(), (df.odd.max() - 1) / 0.95, df[df.kept].odd.median(), (df[df.kept].odd.median() - 1) / 0.95))
    T = pd.DataFrame(out); os.makedirs(os.path.join(ROOT, "varredura_over"), exist_ok=True)
    snap = os.path.join(ROOT, "varredura_over", "auditoria_top3_cs_%s.csv" % datetime.now().strftime("%Y-%m-%d"))
    T.to_csv(snap, index=False, encoding="utf-8-sig"); pd.concat(full).to_csv(snap.replace(".csv", "_apostas.csv"), index=False, encoding="utf-8-sig")

    # ---- FORWARD LEDGER (mantidos reais, odd do feed do dia, placar bases/oficial)
    print("\n" + "=" * 100); print("FORWARD LEDGER (metodos_aprovados/forward_5metodos_ledger.csv) — o que foi efetivamente sinalizado"); print("=" * 100)
    L = pd.read_csv(os.path.join(ROOT, "metodos_aprovados", "forward_5metodos_ledger.csv"), encoding="utf-8-sig")
    for m, nome in (("2x2", "Lay 2x2 Top 3"), ("0x3", "Lay 0x3 Top 3")):
        g = L[(L.Metodo == nome) & (L.status == "LIQUIDADO")].copy()
        g["odd"] = pd.to_numeric(g.Odd_Lay); g["pnl"] = pd.to_numeric(g.pnl_u); g["green"] = (g.resultado == "GREEN").astype(int); g["be"] = pd.to_numeric(g.break_even); g["Date"] = g.Data
        st = stats(g, nome)
        v = g.pnl.values
        print("  %-14s N=%3d %3dG/%2dR WR=%5.1f%% BE=%5.1f%% gap=%+5.1fpp ROI=%+6.2f%% PnL=%+6.2fu | greens somam %+.2fu, reds somam %+.2fu | odd med %.2f -> 1 red = %.1f greens"
              % (nome, st["N"], st["G"], st["R"], st["WR"], st["BE"], st["gap_pp"], st["ROI"], st["PnL_u"], v[v > 0].sum(), v[v < 0].sum(), st["odd_med"], st["greens_por_red"]))
        print("  %-14s pendentes=%d sem_placar=%d" % ("", (L[(L.Metodo == nome)].status == "PENDENTE").sum(), (L[(L.Metodo == nome)].status == "SEM_PLACAR").sum()))


if __name__ == "__main__":
    main()
