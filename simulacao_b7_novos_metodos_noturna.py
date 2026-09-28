import sys
sys.stdout.reconfigure(encoding='utf-8')
import warnings
warnings.filterwarnings('ignore')

import glob
import numpy as np
import pandas as pd
from pathlib import Path
from scipy.optimize import minimize_scalar

ROOT = Path(__file__).resolve().parent

print("="*105)
print("🔬 FASE 3: AUDITORIA BOOTSTRAP 10.000× + KELLY + SIMULAÇÃO CRONOLÓGICA CENÁRIO B7 (01/08 -> 24/09/2026)")
print("="*105)

# 1. Carregar Base Unificada Betfair Exchange
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
df["Time"] = df["Time"].astype(str).str.slice(0, 5)
df["Goals_H_FT"] = pd.to_numeric(df["Goals_H_FT"], errors="coerce")
df["Goals_A_FT"] = pd.to_numeric(df["Goals_A_FT"], errors="coerce")
df["Goals_H_HT"] = pd.to_numeric(df.get("Goals_H_HT"), errors="coerce")
df["Goals_A_HT"] = pd.to_numeric(df.get("Goals_A_HT"), errors="coerce")

df = df.dropna(subset=["Date", "Home", "Away", "Goals_H_FT", "Goals_A_FT"]).copy()
df = df.drop_duplicates(subset=["Date", "Home", "Away"], keep="last").sort_values(["Date", "Time"]).reset_index(drop=True)

for c in df.columns:
    if c.startswith("Odd_"):
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
o45_l = df["Odd_Over45_FT_Lay"]
o25_ht_l = df["Odd_Over25_HT_Lay"]

# Motor Exato de Seleção Top-3 com Desempate por Horário Distinto (idêntico à Página 16 / estrategia_lay_0x3.py)
def select_top_k_distinct_hours(df_sub, odd_col_name, top_n=3):
    selected_indices = []
    for dt, grp in df_sub.groupby("Date"):
        if len(grp) <= top_n:
            selected_indices.extend(grp.index.tolist())
            continue
        grp_sorted = grp.sort_values(odd_col_name, ascending=True)
        sel = []
        used_hours = set()
        for o_val in sorted(grp_sorted[odd_col_name].unique()):
            sub_o = grp_sorted[grp_sorted[odd_col_name] == o_val]
            for idx, r in sub_o.iterrows():
                if len(sel) == top_n: break
                h_str = str(r["Time"])[:5]
                if h_str not in used_hours:
                    sel.append(idx)
                    used_hours.add(h_str)
            if len(sel) == top_n: break
            for idx, r in sub_o.iterrows():
                if len(sel) == top_n: break
                if idx not in sel:
                    sel.append(idx)
        selected_indices.extend(sel)
    mask_out = pd.Series(False, index=df.index)
    mask_out.loc[selected_indices] = True
    return mask_out

# Construir máscaras exatas dos novos candidatos com desempate por horário distinto
# 1. Lay 3x0 Top-3 (U25 <= 1.80 | Odd_H_Back >= 1.80 | 14.0 <= Odd_CS_3x0_Lay <= 35.0)
m_3x0_cand_180 = (u25_b > 0) & (u25_b <= 1.80) & (oh_b >= 1.80) & (df["Odd_CS_3x0_Lay"] >= 14.0) & (df["Odd_CS_3x0_Lay"] <= 35.0)
mask_lay_3x0_top3_180 = select_top_k_distinct_hours(df[m_3x0_cand_180], "Odd_CS_3x0_Lay", top_n=3)

# 1b. Lay 3x0 Top-3 (U25 <= 1.75 | Odd_H_Back >= 1.80 | 14.0 <= Odd_CS_3x0_Lay <= 35.0)
m_3x0_cand_175 = (u25_b > 0) & (u25_b <= 1.75) & (oh_b >= 1.80) & (df["Odd_CS_3x0_Lay"] >= 14.0) & (df["Odd_CS_3x0_Lay"] <= 35.0)
mask_lay_3x0_top3_175 = select_top_k_distinct_hours(df[m_3x0_cand_175], "Odd_CS_3x0_Lay", top_n=3)

# 2. Lay Goleada_H (4+ Gols Mandante) Top-3 (U25 <= 1.85 | Odd_H_Back >= 1.85 | 12.0 <= Odd_CS_Goleada_H_Lay <= 35.0)
m_gh_cand = (u25_b > 0) & (u25_b <= 1.85) & (oh_b >= 1.85) & (df["Odd_CS_Goleada_H_Lay"] >= 12.0) & (df["Odd_CS_Goleada_H_Lay"] <= 35.0)
mask_lay_gh_top3 = select_top_k_distinct_hours(df[m_gh_cand], "Odd_CS_Goleada_H_Lay", top_n=3)

# 3. Lay Over 4.5 FT Equilibrado (U25 <= 1.60 | min(Odd_H, Odd_A) >= 2.00 | 4.0 <= Odd_Over45_Lay <= 18.0)
mask_o45_eq = (u25_b > 0) & (u25_b <= 1.60) & (np.minimum(oh_b, oa_b) >= 2.00) & (o45_l >= 4.0) & (o45_l <= 18.0)

# 4. Lay Over 2.5 HT Equilibrado (U25_FT <= 1.65 | min(Odd_H, Odd_A) >= 2.00 | 5.0 <= Odd_Over25_HT_Lay <= 16.0)
mask_o25_ht_eq = (u25_b > 0) & (u25_b <= 1.65) & (np.minimum(oh_b, oa_b) >= 2.00) & (o25_ht_l >= 5.0) & (o25_ht_l <= 16.0) & pd.notna(gtot_ht)

# 5. Lay Away Fortaleza 1X (Fav_H <= 1.40 | O25 >= 1.75 | 4.5 <= Odd_A_Lay <= 15.0)
mask_away_1x = (oh_b <= 1.40) & (o25_b >= 1.75) & (oa_l >= 4.5) & (oa_l <= 15.0)

# 6. Lay Home Fortaleza X2 (Fav_A <= 1.60 | O25 >= 1.75 | 2.0 <= Odd_H_Lay <= 10.0)
mask_home_x2_ctrl = (oa_b <= 1.60) & (o25_b >= 1.75) & (oh_l >= 2.0) & (oh_l <= 10.0)

# Bootstrap Block-Day 10.000x + Cálculo de Kelly Exato
def bootstrap_and_kelly(name, mask, odd_col, win_arr, is_cs=False):
    m_clean = mask & (~df["Is_Jan_Apr_26"]) if is_cs else mask
    sub = df[m_clean].copy()
    odds = odd_col[m_clean].values
    wins = np.asarray(win_arr)[m_clean.values].astype(bool)
    
    for comm in [0.05, 0.035]:
        pnl = np.where(wins, (1.0 - comm) / (odds - 1.0), -1.0)
        sub["pnl"] = pnl
        daily_pnl = sub.groupby("Date")["pnl"].agg(["sum", "count"])
        days = daily_pnl.index.values
        sums = daily_pnl["sum"].values
        cnts = daily_pnl["count"].values
        
        rng = np.random.default_rng(42)
        n_days = len(days)
        idx_boot = rng.integers(0, n_days, size=(10000, n_days))
        boot_rois = (sums[idx_boot].sum(axis=1) / cnts[idx_boot].sum(axis=1)) * 100.0
        ic_low, ic_high = np.percentile(boot_rois, [2.5, 97.5])
        p_val = float(np.mean(boot_rois <= 0.0))
        
        # Kelly Exato (max E[ln(1 + f * pnl)])
        def neg_log_growth(f):
            if f <= 0: return 0.0
            vals = 1.0 + f * pnl
            if np.any(vals <= 1e-6): return 1e9
            return -np.mean(np.log(vals))
        res_k = minimize_scalar(neg_log_growth, bounds=(0.0, 0.50), method="bounded")
        f_star = float(res_k.x) if res_k.success else 0.0
        
        if comm == 0.05:
            res_5 = (len(sub), float(wins.mean()*100), float(np.sum(pnl)), float(np.mean(pnl)*100), ic_low, ic_high, p_val, f_star*100)
        else:
            res_35 = (float(np.sum(pnl)), float(np.mean(pnl)*100), ic_low, ic_high, p_val, f_star*100)
            
    print(f"📌 {name}")
    print(f"   N={res_5[0]:,} | WR={res_5[1]:.2f}%")
    print(f"   [Comissão 5.0%] P&L: {res_5[2]:+.2f}u | ROI: {res_5[3]:+.2f}% | IC95%: [{res_5[4]:+.2f}%, {res_5[5]:+.2f}%] (p={res_5[6]:.4f}) | Full Kelly f*={res_5[7]:.1f}% (1/4 Kelly={res_5[7]/4:.1f}%)")
    print(f"   [Comissão 3.5%] P&L: {res_35[0]:+.2f}u | ROI: {res_35[1]:+.2f}% | IC95%: [{res_35[2]:+.2f}%, {res_35[3]:+.2f}%] (p={res_35[4]:.4f}) | Full Kelly f*={res_35[5]:.1f}% (1/4 Kelly={res_35[5]/4:.1f}%)\n")

print("\n=== 1. AUDITORIA ESTATÍSTICA (BOOTSTRAP BLOCO-DIA 10.000× & KELLY EXATO) ===\n")
bootstrap_and_kelly("Lay Away Fortaleza 1X (Fav_H <= 1.40 | O25 >= 1.75 | Lay 4.5-15.0)", mask_away_1x, oa_l, (gh >= ga), is_cs=False)
bootstrap_and_kelly("Lay 3x0 Top 3 Menor Odd (U25 <= 1.80 | H >= 1.80 | Lay 14-35 | Horário Distinto)", mask_lay_3x0_top3_180, df["Odd_CS_3x0_Lay"], ~((gh==3)&(ga==0)), is_cs=True)
bootstrap_and_kelly("Lay 3x0 Top 3 Menor Odd (U25 <= 1.75 | H >= 1.80 | Lay 14-35 | Horário Distinto)", mask_lay_3x0_top3_175, df["Odd_CS_3x0_Lay"], ~((gh==3)&(ga==0)), is_cs=True)
bootstrap_and_kelly("Lay Over 4.5 FT Jogo Equilibrado (U25 <= 1.60 | minFav >= 2.00 | Lay 4-18)", mask_o45_eq, o45_l, (gtot <= 4), is_cs=False)
bootstrap_and_kelly("Lay Over 2.5 HT Jogo Equilibrado (U25_FT <= 1.65 | minFav >= 2.00 | Lay 5-16)", mask_o25_ht_eq, o25_ht_l, (gtot_ht <= 2), is_cs=False)

# ==============================================================================
# 2. SIMULAÇÃO CRONOLÓGICA DO CENÁRIO B7 (01/08/2026 -> 24/09/2026) COM NOVOS MÉTODOS
# ==============================================================================
print("="*105)
print("💰 2. SIMULAÇÃO CRONOLÓGICA CENÁRIO B7 (01/08/2026 -> 24/09/2026 | Banca R$ 1.000 | Stop -10% / +10%)")
print("="*105)

# Carregar todos os sinais reais das planilhas metodos_aprovados/Sinais_Metodos_Aprovados_*.xlsx
excel_files = sorted(glob.glob(str(ROOT / "metodos_aprovados" / "Sinais_Metodos_Aprovados_*.xlsx")))
dfs_ex = []
for fp in excel_files:
    d_ex = pd.read_excel(fp)
    dfs_ex.append(d_ex)
df_portfolio_base = pd.concat(dfs_ex, ignore_index=True)
df_portfolio_base["Data"] = pd.to_datetime(df_portfolio_base["Data"], errors="coerce").dt.strftime("%Y-%m-%d")
df_portfolio_base = df_portfolio_base[df_portfolio_base["Resultado"].isin(["GREEN", "RED", "🟢 GREEN", "🔴 RED"])].copy()
df_portfolio_base["Is_Green"] = df_portfolio_base["Resultado"].astype(str).str.contains("GREEN")
df_portfolio_base = df_portfolio_base[~df_portfolio_base["Método"].astype(str).str.contains("Paralelo|Zebra|Micro-Liability", na=False)].copy()

# Preparar os sinais adicionais de Ago-Set/2026 (01/08 -> 24/09) na base oficial para os novos candidatos
df_as = df[df["Date"] >= "2026-08-01"].copy()

def build_extra_signals(sub_mask, metodo_nome, odd_col_name, win_cond_series):
    rows_ext = []
    sub_df = df[sub_mask & (df["Date"] >= "2026-08-01")].copy()
    for idx, r in sub_df.iterrows():
        rows_ext.append({
            "Data": r["Date"],
            "Hora": str(r["Time"])[:5],
            "Liga": r["League"],
            "Jogo": f"{r['Home']} x {r['Away']}",
            "Método": metodo_nome,
            "Odd_Entrada": float(r[odd_col_name]),
            "Is_Green": bool(win_cond_series.loc[idx])
        })
    return pd.DataFrame(rows_ext)

df_ext_3x0_180 = build_extra_signals(mask_lay_3x0_top3_180, "Lay 3x0 Top 3 (U25<=1.80 | H>=1.80)", "Odd_CS_3x0_Lay", pd.Series(~((gh==3)&(ga==0)), index=df.index))
df_ext_3x0_175 = build_extra_signals(mask_lay_3x0_top3_175, "Lay 3x0 Top 3 (U25<=1.75 | H>=1.80)", "Odd_CS_3x0_Lay", pd.Series(~((gh==3)&(ga==0)), index=df.index))
df_ext_o45_eq = build_extra_signals(mask_o45_eq & (~((u25_b <= 1.50) & (o45_l >= 4.0) & (o45_l <= 20.0))), "Lay Over 4.5 Equilibrado Extra", "Odd_Over45_FT_Lay", pd.Series(gtot <= 4, index=df.index))
df_ext_o25_ht = build_extra_signals(mask_o25_ht_eq, "Lay Over 2.5 HT Equilibrado", "Odd_Over25_HT_Lay", pd.Series(gtot_ht <= 2, index=df.index))

def run_b7_sim(df_sinais, extra_dfs, custom_weights, comm=0.045, stop_loss=-0.10, stop_win=0.10):
    base_cols = ["Data", "Hora", "Jogo", "Método", "Odd_Entrada", "Is_Green"]
    all_parts = [df_sinais[base_cols].copy()]
    for edf in extra_dfs:
        if not edf.empty:
            all_parts.append(edf[base_cols].copy())
    df_sim = pd.concat(all_parts, ignore_index=True)
    df_sim["Hora"] = df_sim["Hora"].fillna("15:00").astype(str).str.slice(0, 5)
    df_sim = df_sim.sort_values(["Data", "Hora", "Método"]).reset_index(drop=True)
    
    banca = 1000.0
    peak = 1000.0
    max_dd = 0.0
    total_trades = 0
    greens = 0
    reds = 0
    
    for dt, grp in df_sim.groupby("Data"):
        banca_ini_dia = banca
        pnl_dia = 0.0
        for _, row in grp.iterrows():
            # Checar Stop Diário (-10% / +10%)
            if pnl_dia <= stop_loss * banca_ini_dia or pnl_dia >= stop_win * banca_ini_dia:
                break
            met = str(row["Método"])
            w_pct = 0.05
            for k_str, val_w in custom_weights.items():
                if k_str in met:
                    w_pct = val_w
                    break
            if w_pct <= 0:
                continue
            liab = banca_ini_dia * w_pct
            odd_e = float(row["Odd_Entrada"])
            if odd_e <= 1.02:
                continue
            if row["Is_Green"]:
                lucro = liab * ((1.0 - comm) / (odd_e - 1.0))
                greens += 1
            else:
                lucro = -liab
                reds += 1
            pnl_dia += lucro
            banca += lucro
            total_trades += 1
            if banca > peak:
                peak = banca
            dd = (banca - peak) / peak * 100.0
            if dd < max_dd:
                max_dd = dd
    wr = (greens / total_trades * 100.0) if total_trades > 0 else 0.0
    return banca, banca - 1000.0, (banca / 1000.0 - 1.0)*100.0, max_dd, total_trades, greens, reds, wr

base_weights = {
    "Over 4.5": 0.15,
    "0x3": 0.10,
    "2x2": 0.10,
    "Lay Away Fortaleza 1X": 0.10,
    "Lay Draw": 0.05,
    "Lay Home": 0.05,
    "0x0": 0.05,
}

configs = [
    ("1. Portfólio B7 Atual (7 Métodos + Trava Lay Away 1X)", [], base_weights),
    ("2. B7 + Lay 3x0 Top 3 (U25<=1.80 | H>=1.80) @ 5% Liab", [df_ext_3x0_180], {**base_weights, "Lay 3x0 Top 3": 0.05}),
    ("3. B7 + Lay 3x0 Top 3 (U25<=1.80 | H>=1.80) @ 10% Liab", [df_ext_3x0_180], {**base_weights, "Lay 3x0 Top 3": 0.10}),
    ("4. B7 + Lay 3x0 Top 3 (U25<=1.75 | H>=1.80) @ 5% Liab", [df_ext_3x0_175], {**base_weights, "Lay 3x0 Top 3": 0.05}),
    ("5. B7 + Lay 3x0 Top 3 (U25<=1.75 | H>=1.80) @ 10% Liab", [df_ext_3x0_175], {**base_weights, "Lay 3x0 Top 3": 0.10}),
    ("6. B7 + Lay Over 4.5 Equilibrado Extra (U25<=1.60 & minFav>=2.00) @ 10%", [df_ext_o45_eq], {**base_weights, "Lay Over 4.5 Equilibrado Extra": 0.10}),
    ("7. B7 + Lay Over 2.5 HT Equilibrado (U25<=1.65 & minFav>=2.00) @ 5%", [df_ext_o25_ht], {**base_weights, "Lay Over 2.5 HT Equilibrado": 0.05}),
    ("8. B7 COMBO ELITE (B7 Atual + Lay 3x0 Top 3 [U25<=1.75] 10% + Over 4.5 Eq 10%)", [df_ext_3x0_175, df_ext_o45_eq], {**base_weights, "Lay 3x0 Top 3": 0.10, "Lay Over 4.5 Equilibrado Extra": 0.10}),
]

sim_rows = []
for cfg_name, ext_list, w_dict in configs:
    bf, luc, cresc, mdd, tr, g, r, wr = run_b7_sim(df_portfolio_base, ext_list, w_dict)
    sim_rows.append({
        "Configuração Cenário B7": cfg_name,
        "Trades": tr,
        "G / R": f"{g}G / {r}R",
        "WR (%)": f"{wr:.2f}%",
        "Banca Final (R$)": f"R$ {bf:,.2f}",
        "Lucro Líquido (R$)": f"+R$ {luc:,.2f}",
        "Crescimento (%)": f"+{cresc:.2f}%",
        "Max Drawdown (%)": f"{mdd:.2f}%"
    })

df_sim_res = pd.DataFrame(sim_rows)
print(df_sim_res.to_string(index=False))
