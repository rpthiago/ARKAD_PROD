import sys
sys.stdout.reconfigure(encoding='utf-8')
import warnings
warnings.filterwarnings('ignore')

import glob
import numpy as np
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent

# Carregar dados consolidados
dfs_bf = []
for fn in ["Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH3.csv", "metodos_aprovados/.cache_base_betfair.csv"]:
    p = ROOT / fn
    if p.exists():
        dfs_bf.append(pd.read_csv(p, low_memory=False))
feed_files = sorted(glob.glob(str(ROOT / "scratch" / "feed_arquivo" / "*.parquet")))
if feed_files:
    dfs_bf.append(pd.concat([pd.read_parquet(fp) for fp in feed_files], ignore_index=True))

df = pd.concat(dfs_bf, ignore_index=True)
df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
df["Home"] = df["Home"].astype(str).str.strip()
df["Away"] = df["Away"].astype(str).str.strip()
df["Goals_H_FT"] = pd.to_numeric(df["Goals_H_FT"], errors="coerce")
df["Goals_A_FT"] = pd.to_numeric(df["Goals_A_FT"], errors="coerce")
df["Goals_H_HT"] = pd.to_numeric(df.get("Goals_H_HT"), errors="coerce")
df["Goals_A_HT"] = pd.to_numeric(df.get("Goals_A_HT"), errors="coerce")

df = df.dropna(subset=["Date", "Home", "Away", "Goals_H_FT", "Goals_A_FT"]).copy()
df = df.drop_duplicates(subset=["Date", "Home", "Away"], keep="last").sort_values(["Date", "Time"]).reset_index(drop=True)

# Merge AH/EH da Bet365
p_b365 = ROOT / "Bases_de_Dados_API_FutPythonTrader_Bet365.csv"
if p_b365.exists():
    cols_b365 = pd.read_csv(p_b365, nrows=2, low_memory=False).columns.tolist()
    ah_eh_cols = [c for c in cols_b365 if c.startswith("AH_") or c.startswith("EH_") or c in ("Odd_DC_1X", "Odd_DC_12", "Odd_DC_X2")]
    df_b365 = pd.read_csv(p_b365, usecols=["Date", "Home", "Away"] + ah_eh_cols, low_memory=False)
    df_b365["Date"] = pd.to_datetime(df_b365["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df_b365["Home"] = df_b365["Home"].astype(str).str.strip()
    df_b365["Away"] = df_b365["Away"].astype(str).str.strip()
    df_b365 = df_b365.drop_duplicates(subset=["Date", "Home", "Away"], keep="last")
    df = df.merge(df_b365, on=["Date", "Home", "Away"], how="left")

for c in df.columns:
    if any(c.startswith(p) for p in ("Odd_", "AH_", "EH_")):
        df[c] = pd.to_numeric(df[c], errors="coerce")

df["Year"] = df["Date"].str.slice(0, 4)
df["Is_Jan_Apr_26"] = (df["Date"] >= "2026-01-01") & (df["Date"] <= "2026-04-30")
df["Is_Clean_OOS_26"] = (df["Date"] >= "2026-05-01")
df["Is_Aug_Sep_26"] = (df["Date"] >= "2026-08-01")

liga_up = df["League"].astype(str).str.upper().str.strip()
df["Is_Selecao"] = liga_up.apply(lambda x: any(t in x for t in ("WORLD", "NATIONS LEAGUE", "EUROCUP", "AMERICA CUP")))
df["Is_Copa"] = liga_up.apply(lambda x: any(t in x for t in (
    "CUP", "COPA", "CHAMPIONS", "EUROPA LEAGUE", "CONFERENCE",
    "LIBERTADORES", "SUDAMERICANA", "POKAL", "COUPE", "COPPA",
    "TAÇA", "TACA", "TROPHY", "SHIELD", "SUPERCUP", "SUPER CUP", "QUALIF", "PLAYOFF"
)))
df["Is_Liga_Nac"] = (~df["Is_Selecao"]) & (~df["Is_Copa"])

gh = df["Goals_H_FT"].values
ga = df["Goals_A_FT"].values
gtot = gh + ga
gh_ht = df["Goals_H_HT"].values
ga_ht = df["Goals_A_HT"].values
gtot_ht = gh_ht + ga_ht

oh_b, oa_b, od_b = df["Odd_H_Back"], df["Odd_A_Back"], df["Odd_D_Back"]
oh_l, oa_l, od_l = df["Odd_H_Lay"], df["Odd_A_Lay"], df["Odd_D_Lay"]
o25_b, u25_b = df["Odd_Over25_FT_Back"], df["Odd_Under25_FT_Back"]
o35_l, o45_l = df["Odd_Over35_FT_Lay"], df["Odd_Over45_FT_Lay"]
u05_l, u15_l = df["Odd_Under05_FT_Lay"], df["Odd_Under15_FT_Lay"]
o15_ht_l, o25_ht_l = df["Odd_Over15_HT_Lay"], df["Odd_Over25_HT_Lay"]

def eval_full(name, cat, mask, odd_col, win_arr, side="LAY", is_cs=False):
    valid = mask & odd_col.notna() & (odd_col > 1.02) & (odd_col < 100.0)
    m_clean = valid & (~df["Is_Jan_Apr_26"]) if is_cs else valid
    n3 = int(m_clean.sum())
    if n3 < 100:
        return None
    
    def _m(sub_m, comm=0.05):
        n = int(sub_m.sum())
        if n == 0:
            return 0, 0.0, 0.0, 0.0, 0.0, 0.0
        o = np.asarray(odd_col)[sub_m.values]
        w = np.asarray(win_arr)[sub_m.values].astype(bool)
        wr = float(w.mean() * 100.0)
        if side == "LAY":
            pnl = np.where(w, (1.0 - comm) / (o - 1.0), -1.0)
            be = float(np.mean((o - 1.0) / (o - comm)) * 100.0)
        else:
            pnl = np.where(w, (o - 1.0) * (1.0 - comm), -1.0)
            be = float(np.mean(1.0 / (1.0 + (o - 1.0) * (1.0 - comm))) * 100.0)
        pnl_sum = float(np.sum(pnl))
        roi = float((pnl_sum / n) * 100.0)
        se = float(np.std(pnl) / np.sqrt(n)) if n > 1 else 1.0
        z = (pnl_sum / n) / se if se > 0 else 0.0
        return n, wr, be, pnl_sum, roi, z

    n3, wr3, be3, pnl3_5, roi3_5, z3 = _m(m_clean, 0.05)
    _, _, _, pnl3_35, roi3_35, _ = _m(m_clean, 0.035)
    n24, _, _, _, roi24, _ = _m(m_clean & (df["Year"] == "2024"), 0.05)
    n25, _, _, _, roi25, _ = _m(m_clean & (df["Year"] == "2025"), 0.05)
    noos, wroos, _, pnloos, roioos, _ = _m(valid & df["Is_Clean_OOS_26"], 0.05)
    nas, wras, _, pnlas_5, roias_5, _ = _m(valid & df["Is_Aug_Sep_26"], 0.05)
    _, _, _, pnlas_35, roias_35, _ = _m(valid & df["Is_Aug_Sep_26"], 0.035)

    return {
        "Cat": cat, "Name": name,
        "N_3Y": n3, "WR_3Y": round(wr3, 2), "BE_3Y": round(be3, 2), "Marg": round(wr3 - be3, 2),
        "ROI_3Y_5%": round(roi3_5, 2), "ROI_3Y_3.5%": round(roi3_35, 2), "Z": round(z3, 2),
        "N24": n24, "ROI24": round(roi24, 2),
        "N25": n25, "ROI25": round(roi25, 2),
        "N_MaiSet26": noos, "ROI_MaiSet26": round(roioos, 2),
        "N_AgoSet26": nas, "WR_AgoSet26": round(wras, 2), "PnL_AS_5%": round(pnlas_5, 2),
        "ROI_AS_5%": round(roias_5, 2), "ROI_AS_3.5%": round(roias_35, 2)
    }

rows = []

# 1. Comparar os Métodos Atuais de Produção vs. Novos Descobertos na Mesma Régua
print("\n=== BLOCO A: DIAGNÓSTICO COMPLETO DOS MÉTODOS ATUAIS DE PRODUÇÃO VS NOVOS CANDIDATOS ===")

# Top-3 0x3 (Produção)
m_0x3_base = (u25_b <= 2.10) & ((oa_b >= 1.85) | oa_b.isna()) & (df["Odd_CS_0x3_Lay"] >= 14.0) & (df["Odd_CS_0x3_Lay"] <= 35.0)
rk_0x3 = df.loc[m_0x3_base, "Odd_CS_0x3_Lay"].groupby(df.loc[m_0x3_base, "Date"]).rank(method="first", ascending=True)
m_0x3_top3 = pd.Series(False, index=df.index); m_0x3_top3.loc[rk_0x3[rk_0x3 <= 3].index] = True
rows.append(eval_full("[PRODUÇÃO] Lay 0x3 Top 3 (U25<=2.10 | A>=1.85 | Lay 14-35)", "0_Producao", m_0x3_top3, df["Odd_CS_0x3_Lay"], ~((gh==0)&(ga==3)), "LAY", True))

# Top-3 2x2 (Produção)
m_2x2_base = (u25_b <= 1.95) & (np.minimum(oh_b, oa_b) >= 1.75) & (df["Odd_CS_2x2_Lay"] >= 14.0) & (df["Odd_CS_2x2_Lay"] <= 32.0)
rk_2x2 = df.loc[m_2x2_base, "Odd_CS_2x2_Lay"].groupby(df.loc[m_2x2_base, "Date"]).rank(method="first", ascending=True)
m_2x2_top3 = pd.Series(False, index=df.index); m_2x2_top3.loc[rk_2x2[rk_2x2 <= 3].index] = True
rows.append(eval_full("[PRODUÇÃO] Lay 2x2 Top 3 (U25<=1.95 | Fav>=1.75 | Lay 14-32)", "0_Producao", m_2x2_top3, df["Odd_CS_2x2_Lay"], ~((gh==2)&(ga==2)), "LAY", True))

# Novo Candidato: Lay 3x0 Top-K (Espelho Simétrico do 0x3 para o Mandante!)
for u25_cut in [1.75, 1.80, 1.85, 1.90]:
    for h_cut in [1.80, 1.90, 2.00]:
        for top_k in [2, 3]:
            m_3x0_b = (u25_b <= u25_cut) & (oh_b >= h_cut) & (df["Odd_CS_3x0_Lay"] >= 14.0) & (df["Odd_CS_3x0_Lay"] <= 35.0)
            rk_3x0 = df.loc[m_3x0_b, "Odd_CS_3x0_Lay"].groupby(df.loc[m_3x0_b, "Date"]).rank(method="first", ascending=True)
            m_3x0_k = pd.Series(False, index=df.index); m_3x0_k.loc[rk_3x0[rk_3x0 <= top_k].index] = True
            r = eval_full(f"[NOVO CS] Lay 3x0 Top-{top_k} (U25<={u25_cut} | H>={h_cut} | Lay 14-35)", "1_Novo_CS_TopK", m_3x0_k, df["Odd_CS_3x0_Lay"], ~((gh==3)&(ga==0)), "LAY", True)
            if r: rows.append(r)

# Novo Candidato: Lay Goleada_H / Lay Goleada_A Top-3 (Qualquer Outro Mandante 4+ / Visitante 4+)
for top_k in [2, 3]:
    m_gh_b = (u25_b <= 1.85) & (oh_b >= 1.85) & (df["Odd_CS_Goleada_H_Lay"] >= 12.0) & (df["Odd_CS_Goleada_H_Lay"] <= 35.0)
    rk_gh = df.loc[m_gh_b, "Odd_CS_Goleada_H_Lay"].groupby(df.loc[m_gh_b, "Date"]).rank(method="first", ascending=True)
    m_gh_k = pd.Series(False, index=df.index); m_gh_k.loc[rk_gh[rk_gh <= top_k].index] = True
    r = eval_full(f"[NOVO CS] Lay Goleada_H (4+ Mandante) Top-{top_k} (U25<=1.85 | H>=1.85)", "1_Novo_CS_TopK", m_gh_k, df["Odd_CS_Goleada_H_Lay"], ~((gh>=4)&(gh>ga)), "LAY", True)
    if r: rows.append(r)

    m_ga_b = (u25_b <= 1.95) & (oa_b >= 1.85) & (df["Odd_CS_Goleada_A_Lay"] >= 12.0) & (df["Odd_CS_Goleada_A_Lay"] <= 35.0)
    rk_ga = df.loc[m_ga_b, "Odd_CS_Goleada_A_Lay"].groupby(df.loc[m_ga_b, "Date"]).rank(method="first", ascending=True)
    m_ga_k = pd.Series(False, index=df.index); m_ga_k.loc[rk_ga[rk_ga <= top_k].index] = True
    r = eval_full(f"[NOVO CS] Lay Goleada_A (4+ Visitante) Top-{top_k} (U25<=1.95 | A>=1.85)", "1_Novo_CS_TopK", m_ga_k, df["Odd_CS_Goleada_A_Lay"], ~((ga>=4)&(ga>gh)), "LAY", True)
    if r: rows.append(r)

# Novo Candidato: Lay Over 4.5 FT Jogo Equilibrado vs Produção
rows.append(eval_full("[PRODUÇÃO] Lay Over 4.5 FT (U25<=1.50 | Lay 4-20)", "0_Producao", (u25_b <= 1.50) & (o45_l >= 4.0) & (o45_l <= 20.0), o45_l, (gtot <= 4), "LAY", False))
for u25_c in [1.55, 1.60, 1.65]:
    for fav_min in [1.85, 1.95, 2.00]:
        m_o45_eq = (u25_b <= u25_c) & (np.minimum(oh_b, oa_b) >= fav_min) & (o45_l >= 4.0) & (o45_l <= 18.0)
        r = eval_full(f"[NOVO OVER] Lay Over 4.5 FT Equilibrado (U25<={u25_c} | minFav>={fav_min} | Lay 4-18)", "2_Novo_Over_Under", m_o45_eq, o45_l, (gtot <= 4), "LAY", False)
        if r: rows.append(r)

# Novo Candidato: Lay Over 2.5 HT em Jogo Equilibrado Under
for u25_c in [1.55, 1.60, 1.65]:
    for fav_min in [1.90, 2.00]:
        m_o25ht = (u25_b <= u25_c) & (np.minimum(oh_b, oa_b) >= fav_min) & (o25_ht_l >= 5.0) & (o25_ht_l <= 18.0) & pd.notna(gtot_ht)
        r = eval_full(f"[NOVO HT] Lay Over 2.5 HT Equilibrado (U25_FT<={u25_c} | minFav>={fav_min} | Lay 5-18)", "2_Novo_Over_Under", m_o25ht, o25_ht_l, (gtot_ht <= 2), "LAY", False)
        if r: rows.append(r)

# Novo Candidato: Lay Home (Dupla Chance X2) em Favorito Visitante — diagnóstico completo
rows.append(eval_full("[PRODUÇÃO] Lay Home Fav Visitante (Fav_A<=1.65 | Lay 2-10)", "0_Producao", (oa_b <= 1.65) & (oh_l >= 2.0) & (oh_l <= 10.0), oh_l, (gh <= ga), "LAY", False))
for fa_c in [1.45, 1.55, 1.60, 1.65]:
    for extra_n, extra_m in [
        ("O25 >= 1.75 (Jogo Controlado)", o25_b >= 1.75),
        ("O25 <= 1.75 (Jogo Aberto)", o25_b <= 1.75),
        ("Apenas Ligas Nacionais", df["Is_Liga_Nac"]),
        ("Lay [3.0, 10.0]", (oh_l >= 3.0) & (oh_l <= 10.0)),
    ]:
        r = eval_full(f"[DIAG HOME] Lay Home X2 (Fav_A<={fa_c} | {extra_n})", "3_Diag_1X2", (oa_b <= fa_c) & extra_m & (oh_l >= 2.0) & (oh_l <= 10.0), oh_l, (gh <= ga), "LAY", False)
        if r: rows.append(r)

# Novo Candidato: Back 12 Exchange & Handicaps Asiáticos
for dc_n, dc_m, dc_col, dc_w in [
    ("[NOVO DC] Back 12 Exchange (Fav_H<=1.45 & O25>=1.75 & Spread<=12%)", (oh_b <= 1.45) & (o25_b >= 1.75) & (df["Odd_12_Lay"] <= df["Odd_12_Back"]*1.12), df["Odd_12_Back"], (gh != ga)),
    ("[NOVO DC] Back 1X Exchange (Fav_H<=1.50 & O25>=1.75 & Spread<=10%)", (oh_b <= 1.50) & (o25_b >= 1.75) & (df["Odd_1X_Lay"] <= df["Odd_1X_Back"]*1.10), df["Odd_1X_Back"], (gh >= ga)),
]:
    r = eval_full(dc_n, "4_DC_Handicap", dc_m, dc_col, dc_w, "BACK", False)
    if r: rows.append(r)

df_out = pd.DataFrame([r for r in rows if r is not None])
print(df_out.to_string(index=False))
df_out.to_csv(ROOT / "mineracao_profunda_fase2_resultados.csv", index=False)
