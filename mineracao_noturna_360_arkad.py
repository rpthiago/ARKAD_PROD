import sys
sys.stdout.reconfigure(encoding='utf-8')
import warnings
warnings.filterwarnings('ignore')

import glob
import numpy as np
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent

print("="*100)
print("🌙 MOTOR DE MINERAÇÃO NOTURNA 360° ARKAD — VARREDURA ESTRUTURAL MULTI-MERCADO (2024–2026)")
print("="*100)

# 1. Carregar Base Unificada Betfair Exchange (FRESH3 + .cache_base_betfair.csv + feeds diários)
dfs_bf = []
for fn in ["Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH3.csv", "metodos_aprovados/.cache_base_betfair.csv"]:
    p = ROOT / fn
    if p.exists():
        d = pd.read_csv(p, low_memory=False)
        dfs_bf.append(d)
        print(f"[+] Carregado {fn}: {len(d):,} linhas")

# Também carregar scratch/feed_arquivo/*.parquet se existir
feed_files = sorted(glob.glob(str(ROOT / "scratch" / "feed_arquivo" / "*.parquet")))
if feed_files:
    dfs_feed = [pd.read_parquet(fp) for fp in feed_files]
    df_feed_all = pd.concat(dfs_feed, ignore_index=True)
    print(f"[+] Carregado scratch/feed_arquivo ({len(feed_files)} arquivos): {len(df_feed_all):,} linhas")
    dfs_bf.append(df_feed_all)

df = pd.concat(dfs_bf, ignore_index=True)
df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
df["Home"] = df["Home"].astype(str).str.strip()
df["Away"] = df["Away"].astype(str).str.strip()
df["Goals_H_FT"] = pd.to_numeric(df["Goals_H_FT"], errors="coerce")
df["Goals_A_FT"] = pd.to_numeric(df["Goals_A_FT"], errors="coerce")
df["Goals_H_HT"] = pd.to_numeric(df.get("Goals_H_HT"), errors="coerce")
df["Goals_A_HT"] = pd.to_numeric(df.get("Goals_A_HT"), errors="coerce")

# Filtrar apenas jogos com placar FT válido e deduplicar mantendo o registro mais recente/completo
df = df.dropna(subset=["Date", "Home", "Away", "Goals_H_FT", "Goals_A_FT"]).copy()
df = df.drop_duplicates(subset=["Date", "Home", "Away"], keep="last").sort_values(["Date", "Time"]).reset_index(drop=True)

# Adicionar janelas temporais críticas (Regra Anti-Miragem Jan-Abr/2026)
df["Year"] = df["Date"].str.slice(0, 4)
df["YM"] = df["Date"].str.slice(0, 7)
df["Is_Jan_Apr_26"] = (df["Date"] >= "2026-01-01") & (df["Date"] <= "2026-04-30")
df["Is_Clean_OOS_26"] = (df["Date"] >= "2026-05-01")  # Mai-Set 2026 (livre de compressão CS)
df["Is_Aug_Sep_26"] = (df["Date"] >= "2026-08-01")    # Janela do B7 / Operação Recente

print(f"[+] Base Betfair Exchange Consolidada e Deduplicada: {len(df):,} jogos ({df['Date'].min()} -> {df['Date'].max()})")
print(f"    - 2024: {(df['Year']=='2024').sum():,} | 2025: {(df['Year']=='2025').sum():,} | Jan-Abr/26: {df['Is_Jan_Apr_26'].sum():,} | Mai-Set/26 (Clean OOS): {df['Is_Clean_OOS_26'].sum():,} (Ago-Set/26: {df['Is_Aug_Sep_26'].sum():,})")

# 2. Juntar colunas de Handicap Asiático (AH_*) e Europeu (EH_*) da Base Bet365 para testar Família 3
p_b365 = ROOT / "Bases_de_Dados_API_FutPythonTrader_Bet365.csv"
if p_b365.exists():
    cols_b365_check = pd.read_csv(p_b365, nrows=2, low_memory=False).columns.tolist()
    ah_eh_cols = [c for c in cols_b365_check if c.startswith("AH_") or c.startswith("EH_") or c in ("Odd_DC_1X", "Odd_DC_12", "Odd_DC_X2")]
    use_cols = ["Date", "Home", "Away"] + ah_eh_cols
    df_b365 = pd.read_csv(p_b365, usecols=[c for c in use_cols if c in cols_b365_check], low_memory=False)
    df_b365["Date"] = pd.to_datetime(df_b365["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df_b365["Home"] = df_b365["Home"].astype(str).str.strip()
    df_b365["Away"] = df_b365["Away"].astype(str).str.strip()
    df_b365 = df_b365.drop_duplicates(subset=["Date", "Home", "Away"], keep="last")
    df = df.merge(df_b365, on=["Date", "Home", "Away"], how="left")
    print(f"[+] Colunas AH/EH/DC mescladas da Bet365 ({len(ah_eh_cols)} colunas). Cobertura AH: {df['AH_H_neg_0_5'].notna().sum():,} jogos")

# Converter todas as colunas de Odd/AH/EH para float
for c in df.columns:
    if any(c.startswith(p) for p in ("Odd_", "AH_", "EH_")):
        df[c] = pd.to_numeric(df[c], errors="coerce")

# Variáveis de Placar
gh = df["Goals_H_FT"].values
ga = df["Goals_A_FT"].values
gtot = gh + ga
gh_ht = df["Goals_H_HT"].values
ga_ht = df["Goals_A_HT"].values
gtot_ht = gh_ht + ga_ht

# 3. Construir Features Rolling Leak-Free (shift(1) estrito por Mandante em Casa, Visitante Fora e Liga)
print("[+] Calculando Features Rolling Leak-Free (shift(1)) por Mandante, Visitante e Liga...")
df["H_Win"] = (gh > ga).astype(float)
df["Draw"] = (gh == ga).astype(float)
df["A_Win"] = (gh < ga).astype(float)
df["Over25"] = (gtot >= 3).astype(float)
df["Under25"] = (gtot <= 2).astype(float)
df["Over15"] = (gtot >= 2).astype(float)
df["BTTS"] = ((gh >= 1) & (ga >= 1)).astype(float)
df["H_CS"] = (ga == 0).astype(float)       # Mandante Clean Sheet (Visitante 0 gols)
df["A_CS"] = (gh == 0).astype(float)       # Visitante Clean Sheet (Mandante 0 gols)
df["CS_0x0"] = (gtot == 0).astype(float)

# Rolling Home em Casa (últimos 5 jogos em casa antes de hoje)
df["H_Roll_WinRate"] = df.groupby("Home")["H_Win"].transform(lambda s: s.shift(1).rolling(5, min_periods=3).mean())
df["H_Roll_CS_Rate"] = df.groupby("Home")["H_CS"].transform(lambda s: s.shift(1).rolling(5, min_periods=3).mean())
df["H_Roll_GF"] = df.groupby("Home")["Goals_H_FT"].transform(lambda s: s.shift(1).rolling(5, min_periods=3).mean())
df["H_Roll_GA"] = df.groupby("Home")["Goals_A_FT"].transform(lambda s: s.shift(1).rolling(5, min_periods=3).mean())
df["H_Roll_GoalsTot"] = df["H_Roll_GF"] + df["H_Roll_GA"]

# Rolling Away Fora (últimos 5 jogos fora antes de hoje)
df["A_Roll_WinRate"] = df.groupby("Away")["A_Win"].transform(lambda s: s.shift(1).rolling(5, min_periods=3).mean())
df["A_Roll_LossRate"] = df.groupby("Away")["H_Win"].transform(lambda s: s.shift(1).rolling(5, min_periods=3).mean())
df["A_Roll_FTS_Rate"] = df.groupby("Away")["H_CS"].transform(lambda s: s.shift(1).rolling(5, min_periods=3).mean()) # Fail to score away
df["A_Roll_GF"] = df.groupby("Away")["Goals_A_FT"].transform(lambda s: s.shift(1).rolling(5, min_periods=3).mean())
df["A_Roll_GA"] = df.groupby("Away")["Goals_H_FT"].transform(lambda s: s.shift(1).rolling(5, min_periods=3).mean())
df["A_Roll_GoalsTot"] = df["A_Roll_GF"] + df["A_Roll_GA"]

# Rolling Liga (últimos 50 jogos da liga antes do jogo atual)
df["Liga_Roll_DrawRate"] = df.groupby("League")["Draw"].transform(lambda s: s.shift(1).rolling(50, min_periods=20).mean())
df["Liga_Roll_GoalsAvg"] = df.groupby("League")["Goals_H_FT"].transform(lambda s: (s + df.loc[s.index, "Goals_A_FT"]).shift(1).rolling(50, min_periods=20).mean())
df["Liga_Roll_0x0Rate"] = df.groupby("League")["CS_0x0"].transform(lambda s: s.shift(1).rolling(50, min_periods=20).mean())

# Classificação de Competição (Liga Nacional vs Copa vs Seleção)
liga_up = df["League"].astype(str).str.upper().str.strip()
df["Is_Selecao"] = liga_up.apply(lambda x: any(t in x for t in ("WORLD", "NATIONS LEAGUE", "EUROCUP", "AMERICA CUP")))
df["Is_Copa"] = liga_up.apply(lambda x: any(t in x for t in (
    "CUP", "COPA", "CHAMPIONS", "EUROPA LEAGUE", "CONFERENCE",
    "LIBERTADORES", "SUDAMERICANA", "POKAL", "COUPE", "COPPA",
    "TAÇA", "TACA", "TROPHY", "SHIELD", "SUPERCUP", "SUPER CUP", "QUALIF", "PLAYOFF"
)))
df["Is_Liga_Nac"] = (~df["Is_Selecao"]) & (~df["Is_Copa"])

# Função de Avaliação Rigorosa (com quebra temporal obrigatória + Kelly + Bootstrap)
def evaluate_strategy(name, family, mask, odd_series, win_mask, side="LAY", is_cs=False, min_n_clean=150, min_n_aug_sep=10):
    # Validar odd executável
    valid_odd = odd_series.notna() & (odd_series > 1.02) & (odd_series < 100.0)
    full_mask = mask & valid_odd
    
    # Se for CS, exigir análise Sem Jan-Abr/2026 como métrica primária para não cair na miragem de gravação
    mask_clean_3y = full_mask & (~df["Is_Jan_Apr_26"]) if is_cs else full_mask
    
    n_clean = int(mask_clean_3y.sum())
    if n_clean < min_n_clean:
        return None
        
    # Subconjunto Ago-Set 2026 (Operação Recente / B7)
    mask_aug_sep = full_mask & df["Is_Aug_Sep_26"]
    n_aug_sep = int(mask_aug_sep.sum())
    if n_aug_sep < min_n_aug_sep:
        return None
        
    def calc_metrics(sub_mask, comm=0.05):
        n = int(sub_mask.sum())
        if n == 0:
            return 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0
        odds = np.asarray(odd_series)[sub_mask.values]
        wins = np.asarray(win_mask)[sub_mask.values].astype(bool)
        w_cnt = int(wins.sum())
        wr = w_cnt / n * 100.0
        if side == "LAY":
            # PnL por 1u de Liability (Capital em Risco)
            pnl_arr = np.where(wins, (1.0 - comm) / (odds - 1.0), -1.0)
            be_wr = np.mean((odds - 1.0) / (odds - comm)) * 100.0
        else:
            # BACK: PnL por 1u de Stake
            pnl_arr = np.where(wins, (odds - 1.0) * (1.0 - comm), -1.0)
            be_wr = np.mean(1.0 / (1.0 + (odds - 1.0) * (1.0 - comm))) * 100.0
        pnl_u = float(np.sum(pnl_arr))
        roi = (pnl_u / n) * 100.0
        # Bootstrap p-value rápido
        std_err = float(np.std(pnl_arr) / np.sqrt(n)) if n > 1 else 1.0
        z_stat = (pnl_u / n) / std_err if std_err > 0 else 0.0
        return n, w_cnt, wr, be_wr, pnl_u, roi, z_stat

    # Métricas na Base Limpa (Sem Jan-Abr/26 para CS; 3 Anos para demais) a 5.0% e 3.5% comissão
    n3, w3, wr3, be3, pnl3_5, roi3_5, z3_5 = calc_metrics(mask_clean_3y, comm=0.05)
    _, _, _, _, pnl3_35, roi3_35, z3_35 = calc_metrics(mask_clean_3y, comm=0.035)
    
    # Filtro primário: tem que ser positivo a 5.0% (ou >= +1.0% a 3.5%) na base histórica limpa
    if roi3_5 <= 0.5:
        return None
        
    # Quebras por Período: 2024, 2025, Mai-Set/26 (Clean OOS), Ago-Set/26 (Janela B7)
    n24, _, _, _, _, roi24, _ = calc_metrics(mask_clean_3y & (df["Year"] == "2024"), comm=0.05)
    n25, _, _, _, _, roi25, _ = calc_metrics(mask_clean_3y & (df["Year"] == "2025"), comm=0.05)
    n_oos, w_oos, wr_oos, _, pnl_oos, roi_oos, _ = calc_metrics(full_mask & df["Is_Clean_OOS_26"], comm=0.05)
    n_as, w_as, wr_as, _, pnl_as_5, roi_as_5, _ = calc_metrics(mask_aug_sep, comm=0.05)
    _, _, _, _, pnl_as_35, roi_as_35, _ = calc_metrics(mask_aug_sep, comm=0.035)
    
    # Coerência Temporal:
    # Exigir que Mai-Set/2026 (Clean OOS) E Ago-Set/2026 sejam POSITIVOS, e que pelo menos 2 dos 3 anos (2024, 2025, 2026_Clean) sejam positivos!
    pos_years = int(roi24 > 0) + int(roi25 > 0) + int(roi_oos > 0)
    if roi_oos <= 0 or roi_as_5 <= 0 or pos_years < 2:
        return None

    return {
        "Family": family,
        "Name": name,
        "Side": side,
        "N_3Y_Clean": n3,
        "WR_3Y": round(wr3, 2),
        "BE_3Y": round(be3, 2),
        "Margin_pp": round(wr3 - be3, 2),
        "PnL_3Y_5pct": round(pnl3_5, 2),
        "ROI_3Y_5pct": round(roi3_5, 2),
        "ROI_3Y_35pct": round(roi3_35, 2),
        "Z_Score": round(z3_5, 2),
        "ROI_2024": round(roi24, 2),
        "N_2024": n24,
        "ROI_2025": round(roi25, 2),
        "N_2025": n25,
        "ROI_MaiSet26": round(roi_oos, 2),
        "N_MaiSet26": n_oos,
        "ROI_AgoSet26_5pct": round(roi_as_5, 2),
        "ROI_AgoSet26_35pct": round(roi_as_35, 2),
        "PnL_AgoSet26_5pct": round(pnl_as_5, 2),
        "N_AgoSet26": n_as,
        "WR_AgoSet26": round(wr_as, 2),
        "Mask_AgoSet26": mask_aug_sep
    }

results = []

# ==============================================================================
# FAMÍLIA 1: ENGENHARIA DE REGIME CRUZADO (TENSÃO 1X2 × GOLS × MERCADO ALVO)
# ==============================================================================
print("\n[1/6] Escaneando Família 1: Tensões Cross-Market (1X2 × Over/Under × BTTS × HT/FT)...")

oh_b, oa_b, od_b = df["Odd_H_Back"], df["Odd_A_Back"], df["Odd_D_Back"]
oh_l, oa_l, od_l = df["Odd_H_Lay"], df["Odd_A_Lay"], df["Odd_D_Lay"]
o25_b, u25_b = df["Odd_Over25_FT_Back"], df["Odd_Under25_FT_Back"]
o15_b, u15_b = df["Odd_Over15_FT_Back"], df["Odd_Under15_FT_Back"]
o35_l, o45_l = df["Odd_Over35_FT_Lay"], df["Odd_Over45_FT_Lay"]
u05_l, u15_l, u25_l, u35_l = df["Odd_Under05_FT_Lay"], df["Odd_Under15_FT_Lay"], df["Odd_Under25_FT_Lay"], df["Odd_Under35_FT_Lay"]
o05_ht_l, o15_ht_l, o25_ht_l = df["Odd_Over05_HT_Lay"], df["Odd_Over15_HT_Lay"], df["Odd_Over25_HT_Lay"]
u05_ht_l, u15_ht_l = df["Odd_Under05_HT_Lay"], df["Odd_Under15_HT_Lay"]
btts_y_l, btts_n_l = df["Odd_BTTS_Yes_Lay"], df["Odd_BTTS_No_Lay"]
btts_y_b, btts_n_b = df["Odd_BTTS_Yes_Back"], df["Odd_BTTS_No_Back"]

# 1A. Lay Home (Dupla Chance X2) em Favorito Visitante + Filtros de Regime (O25 / U25 / Ligas / Odd Bands)
for fav_a_max in [1.40, 1.50, 1.60, 1.65, 1.75]:
    for o25_cond_name, o25_cond in [
        ("Qualquer O25", pd.Series(True, index=df.index)),
        ("O25 >= 1.70 (Jogo Controlado)", o25_b >= 1.70),
        ("O25 >= 1.80 (Jogo Amarrado)", o25_b >= 1.80),
        ("O25 <= 1.75 (Jogo Aberto)", o25_b <= 1.75),
        ("Apenas Ligas Nacionais", df["Is_Liga_Nac"]),
        ("Ligas Nacionais + O25 >= 1.70", df["Is_Liga_Nac"] & (o25_b >= 1.70)),
    ]:
        for h_lay_min, h_lay_max in [(2.2, 8.0), (2.5, 10.0), (3.0, 10.0), (3.5, 12.0)]:
            m = (oa_b <= fav_a_max) & o25_cond & (oh_l >= h_lay_min) & (oh_l <= h_lay_max)
            res = evaluate_strategy(
                f"Lay Home X2 (Fav_A <= {fav_a_max} | {o25_cond_name} | Lay [{h_lay_min}, {h_lay_max}])",
                "1_Cross_1X2_Regime", m, oh_l, (gh <= ga), side="LAY"
            )
            if res: results.append(res)

# 1B. Lay Away (Dupla Chance 1X) em Favorito Mandante + Variações Estruturais
for fav_h_max in [1.35, 1.40, 1.45, 1.50, 1.60]:
    for g_cond_name, g_cond in [
        ("O25 >= 1.70", o25_b >= 1.70),
        ("O25 >= 1.75", o25_b >= 1.75),
        ("O25 >= 1.80", o25_b >= 1.80),
        ("BTTS_No <= 1.80", btts_n_b <= 1.80),
        ("U25 <= 2.15", u25_b <= 2.15),
    ]:
        for a_lay_min, a_lay_max in [(4.0, 15.0), (4.5, 15.0), (5.0, 16.0)]:
            m = (oh_b <= fav_h_max) & g_cond & (oa_l >= a_lay_min) & (oa_l <= a_lay_max)
            res = evaluate_strategy(
                f"Lay Away 1X (Fav_H <= {fav_h_max} | {g_cond_name} | Lay [{a_lay_min}, {a_lay_max}])",
                "1_Cross_1X2_Regime", m, oa_l, (gh >= ga), side="LAY"
            )
            if res: results.append(res)

# 1C. Mercados de Gols FT e HT (Lay Over 3.5, Lay Over 4.5, Lay Over 1.5 HT, Lay Over 2.5 HT, Lay Under 1.5 FT, Lay Under 2.5 FT, Lay BTTS)
for u25_max in [1.45, 1.50, 1.55, 1.60, 1.65]:
    for eq_name, eq_cond in [
        ("Todos", pd.Series(True, index=df.index)),
        ("Jogo Equilibrado (min_Fav >= 1.80)", np.minimum(oh_b, oa_b) >= 1.80),
        ("Jogo Super Equilibrado (min_Fav >= 2.00)", np.minimum(oh_b, oa_b) >= 2.00),
        ("Apenas Ligas Nacionais", df["Is_Liga_Nac"]),
    ]:
        # Lay Over 3.5 FT
        for lmin, lmax in [(2.5, 8.0), (3.0, 9.0), (3.2, 10.0)]:
            m = (u25_b <= u25_max) & eq_cond & (o35_l >= lmin) & (o35_l <= lmax)
            res = evaluate_strategy(
                f"Lay Over 3.5 FT (U25 <= {u25_max} | {eq_name} | Lay [{lmin}, {lmax}])",
                "1_Cross_Goals_Under", m, o35_l, (gtot <= 3), side="LAY"
            )
            if res: results.append(res)
            
        # Lay Over 4.5 FT
        for lmin, lmax in [(4.0, 16.0), (4.5, 18.0), (5.0, 20.0)]:
            m = (u25_b <= u25_max) & eq_cond & (o45_l >= lmin) & (o45_l <= lmax)
            res = evaluate_strategy(
                f"Lay Over 4.5 FT (U25 <= {u25_max} | {eq_name} | Lay [{lmin}, {lmax}])",
                "1_Cross_Goals_Under", m, o45_l, (gtot <= 4), side="LAY"
            )
            if res: results.append(res)

        # Lay Over 1.5 HT / Lay Over 2.5 HT em jogos Under FT
        for lmin, lmax in [(2.8, 8.0), (3.2, 10.0)]:
            m15ht = (u25_b <= u25_max) & eq_cond & (o15_ht_l >= lmin) & (o15_ht_l <= lmax) & pd.notna(gtot_ht)
            res = evaluate_strategy(
                f"Lay Over 1.5 HT (U25_FT <= {u25_max} | {eq_name} | Lay [{lmin}, {lmax}])",
                "1_Cross_HT_Under", m15ht, o15_ht_l, (gtot_ht <= 1), side="LAY"
            )
            if res: results.append(res)

        for lmin, lmax in [(5.0, 16.0), (6.0, 20.0)]:
            m25ht = (u25_b <= u25_max) & eq_cond & (o25_ht_l >= lmin) & (o25_ht_l <= lmax) & pd.notna(gtot_ht)
            res = evaluate_strategy(
                f"Lay Over 2.5 HT (U25_FT <= {u25_max} | {eq_name} | Lay [{lmin}, {lmax}])",
                "1_Cross_HT_Under", m25ht, o25_ht_l, (gtot_ht <= 2), side="LAY"
            )
            if res: results.append(res)

        # Lay BTTS Yes em jogo Under + Desequilibrado ou Equilibrado
        for lmin, lmax in [(2.0, 4.5), (2.2, 5.0)]:
            mbtts = (u25_b <= u25_max) & eq_cond & (btts_y_l >= lmin) & (btts_y_l <= lmax)
            res = evaluate_strategy(
                f"Lay BTTS Yes (U25 <= {u25_max} | {eq_name} | Lay [{lmin}, {lmax}])",
                "1_Cross_BTTS", mbtts, btts_y_l, ((gh == 0) | (ga == 0)), side="LAY"
            )
            if res: results.append(res)

# 1D. Jogos Over (Super Favorito + Expectativa de Gols): Lay Under 0.5 FT, Lay Under 1.5 FT, Lay Under 0.5 HT, Lay BTTS No
for fav_max in [1.30, 1.35, 1.40, 1.45]:
    fav_odd = np.minimum(oh_b, oa_b)
    for o25_max in [1.45, 1.55, 1.65, 99.0]:
        # Lay Under 0.5 FT (Equivalente a Lay 0x0 via Mercado Under 0.5 com mais liquidez!)
        for lmin, lmax in [(7.0, 18.0), (8.0, 20.0), (9.0, 22.0)]:
            m_u05 = (fav_odd <= fav_max) & (o25_b <= o25_max) & (u05_l >= lmin) & (u05_l <= lmax)
            res = evaluate_strategy(
                f"Lay Under 0.5 FT (Fav <= {fav_max} | O25 <= {o25_max} | Lay [{lmin}, {lmax}])",
                "1_Cross_Over_Attack", m_u05, u05_l, (gtot >= 1), side="LAY"
            )
            if res: results.append(res)
            
        # Lay Under 1.5 FT
        for lmin, lmax in [(3.0, 7.5), (3.5, 8.5), (4.0, 10.0)]:
            m_u15 = (fav_odd <= fav_max) & (o25_b <= o25_max) & (u15_l >= lmin) & (u15_l <= lmax)
            res = evaluate_strategy(
                f"Lay Under 1.5 FT (Fav <= {fav_max} | O25 <= {o25_max} | Lay [{lmin}, {lmax}])",
                "1_Cross_Over_Attack", m_u15, u15_l, (gtot >= 2), side="LAY"
            )
            if res: results.append(res)

        # Lay Under 0.5 HT (Gol no 1º Tempo de Super Favorito)
        for lmin, lmax in [(2.6, 5.5), (3.0, 6.0)]:
            m_u05ht = (fav_odd <= fav_max) & (o25_b <= o25_max) & (u05_ht_l >= lmin) & (u05_ht_l <= lmax) & pd.notna(gtot_ht)
            res = evaluate_strategy(
                f"Lay Under 0.5 HT (Fav <= {fav_max} | O25 <= {o25_max} | Lay [{lmin}, {lmax}])",
                "1_Cross_Over_Attack", m_u05ht, u05_ht_l, (gtot_ht >= 1), side="LAY"
            )
            if res: results.append(res)

# ==============================================================================
# FAMÍLIA 2: ENGENHARIA TOP-K MENOR ODD DA GRADE DIÁRIA (COMO LAY 0X3 E LAY 2X2 TOP-3!)
# ==============================================================================
print("[2/6] Escaneando Família 2: Seleção Top-K Menor Odd da Grade Diária (CS, Over/Under, 1X2)...")

# Candidatos a Top-K da grade diária (Sem Jan-Abr/2026 para todos os CS!)
topk_candidates = [
    ("CS 0x1 (Fav_H <= 1.80 | U25 >= 1.75)", (oh_b <= 1.80) & (u25_b >= 1.75) & (df["Odd_CS_0x1_Lay"] >= 7.0) & (df["Odd_CS_0x1_Lay"] <= 18.0), df["Odd_CS_0x1_Lay"], ~((gh == 0) & (ga == 1)), True),
    ("CS 1x0 (Fav_A <= 1.80 | U25 >= 1.75)", (oa_b <= 1.80) & (u25_b >= 1.75) & (df["Odd_CS_1x0_Lay"] >= 7.0) & (df["Odd_CS_1x0_Lay"] <= 18.0), df["Odd_CS_1x0_Lay"], ~((gh == 1) & (ga == 0)), True),
    ("CS 1x1 (Fav <= 1.55 | O25 <= 1.85)", (np.minimum(oh_b, oa_b) <= 1.55) & (o25_b <= 1.85) & (df["Odd_CS_1x1_Lay"] >= 6.5) & (df["Odd_CS_1x1_Lay"] <= 14.0), df["Odd_CS_1x1_Lay"], ~((gh == 1) & (ga == 1)), True),
    ("CS 2x1 (U25 <= 1.70 | Fav_A <= 2.10)", (u25_b <= 1.70) & (oa_b <= 2.10) & (df["Odd_CS_2x1_Lay"] >= 10.0) & (df["Odd_CS_2x1_Lay"] <= 22.0), df["Odd_CS_2x1_Lay"], ~((gh == 2) & (ga == 1)), True),
    ("CS 1x2 (U25 <= 1.70 | Fav_H <= 2.10)", (u25_b <= 1.70) & (oh_b <= 2.10) & (df["Odd_CS_1x2_Lay"] >= 10.0) & (df["Odd_CS_1x2_Lay"] <= 22.0), df["Odd_CS_1x2_Lay"], ~((gh == 1) & (ga == 2)), True),
    ("CS 3x0 (U25 <= 1.85 | Fav_H >= 1.80)", (u25_b <= 1.85) & (oh_b >= 1.80) & (df["Odd_CS_3x0_Lay"] >= 14.0) & (df["Odd_CS_3x0_Lay"] <= 35.0), df["Odd_CS_3x0_Lay"], ~((gh == 3) & (ga == 0)), True),
    ("CS 3x1 (U25 <= 1.85 | Fav_H >= 1.75)", (u25_b <= 1.85) & (oh_b >= 1.75) & (df["Odd_CS_3x1_Lay"] >= 14.0) & (df["Odd_CS_3x1_Lay"] <= 35.0), df["Odd_CS_3x1_Lay"], ~((gh == 3) & (ga == 1)), True),
    ("CS 1x3 (U25 <= 1.85 | Fav_A >= 1.75)", (u25_b <= 1.85) & (oa_b >= 1.75) & (df["Odd_CS_1x3_Lay"] >= 14.0) & (df["Odd_CS_1x3_Lay"] <= 35.0), df["Odd_CS_1x3_Lay"], ~((gh == 1) & (ga == 3)), True),
    ("CS Goleada_H (U25 <= 1.85 | Fav_H >= 1.80)", (u25_b <= 1.85) & (oh_b >= 1.80) & (df["Odd_CS_Goleada_H_Lay"] >= 10.0) & (df["Odd_CS_Goleada_H_Lay"] <= 30.0), df["Odd_CS_Goleada_H_Lay"], ~((gh >= 4) & (gh > ga)), True),
    ("CS Goleada_A (U25 <= 1.85 | Fav_A >= 1.80)", (u25_b <= 1.85) & (oa_b >= 1.80) & (df["Odd_CS_Goleada_A_Lay"] >= 10.0) & (df["Odd_CS_Goleada_A_Lay"] <= 30.0), df["Odd_CS_Goleada_A_Lay"], ~((ga >= 4) & (ga > gh)), True),
    ("Over 4.5 FT (U25 <= 1.50 | Lay 4.0-16.0)", (u25_b <= 1.50) & (o45_l >= 4.0) & (o45_l <= 16.0), o45_l, (gtot <= 4), False),
    ("Over 3.5 FT (U25 <= 1.50 | Lay 2.5-8.0)", (u25_b <= 1.50) & (o35_l >= 2.5) & (o35_l <= 8.0), o35_l, (gtot <= 3), False),
    ("Under 0.5 FT (Fav <= 1.45 | Lay 7.0-18.0)", (np.minimum(oh_b, oa_b) <= 1.45) & (u05_l >= 7.0) & (u05_l <= 18.0), u05_l, (gtot >= 1), False),
]

for cand_name, base_mask, odd_col, win_col, is_cs_flag in topk_candidates:
    valid_m = base_mask & odd_col.notna() & (odd_col > 1.02)
    if valid_m.sum() < 200:
        continue
    # Rankear por Data pela menor odd_col (exatamente como o motor Top-3 0x3 e 2x2!)
    ranks = odd_col[valid_m].groupby(df.loc[valid_m, "Date"]).rank(method="first", ascending=True)
    for top_k in [1, 2, 3, 5]:
        idx_k = ranks[ranks <= top_k].index
        mask_k = pd.Series(False, index=df.index)
        mask_k.loc[idx_k] = True
        res = evaluate_strategy(
            f"Top-{top_k} Menor Odd do Dia: Lay {cand_name}",
            "2_TopK_Daily_Grade", mask_k, odd_col, win_col, side="LAY", is_cs=is_cs_flag, min_n_clean=150, min_n_aug_sep=8
        )
        if res: results.append(res)

# ==============================================================================
# FAMÍLIA 3: HANDICAP ASIÁTICO (AH_*) E EUROPEU (EH_*) + DOUBLE CHANCE EXCHANGE
# ==============================================================================
print("[3/6] Escaneando Família 3: Handicaps Asiáticos, Europeus e Dupla Chance Direta Exchange...")

# 3A. Double Chance Direto na Betfair Exchange (Back 1X, Back X2, Back 12 com Spread Curto)
for dc_col_b, dc_col_l, dc_win, dc_label in [
    ("Odd_1X_Back", "Odd_1X_Lay", (gh >= ga), "Back 1X Exchange"),
    ("Odd_X2_Back", "Odd_X2_Lay", (ga >= gh), "Back X2 Exchange"),
    ("Odd_12_Back", "Odd_12_Lay", (gh != ga), "Back 12 Exchange"),
]:
    if dc_col_b in df.columns and dc_col_l in df.columns:
        b_col = df[dc_col_b]
        l_col = df[dc_col_l]
        # Exigir livro líquido (Lay <= 1.15 * Back)
        liq_ok = b_col.notna() & l_col.notna() & (b_col >= 1.12) & (b_col <= 2.20) & (l_col <= b_col * 1.12)
        for cond_name, cond_m in [
            ("Fav_H <= 1.45 & O25 >= 1.75", (oh_b <= 1.45) & (o25_b >= 1.75)),
            ("Fav_A <= 1.65 & O25 >= 1.70", (oa_b <= 1.65) & (o25_b >= 1.70)),
            ("Fav <= 1.40 & O25 <= 1.70", (np.minimum(oh_b, oa_b) <= 1.40) & (o25_b <= 1.70)),
        ]:
            res = evaluate_strategy(
                f"{dc_label} ({cond_name})",
                "3_DoubleChance_Handicap", liq_ok & cond_m, b_col, dc_win, side="BACK", min_n_clean=120, min_n_aug_sep=5
            )
            if res: results.append(res)

# 3B. Handicaps Asiáticos (AH_H_neg_0_5, AH_H_neg_1_5, AH_A_pos_0_5, AH_A_pos_1_5, AH_H_pos_0_5)
if "AH_H_neg_1_5" in df.columns:
    ah_tests = [
        ("Back AH Home -1.5 (Super Fav H <= 1.35 & O25 <= 1.65)", (oh_b <= 1.35) & (o25_b <= 1.65) & (df["AH_H_neg_1_5"] >= 1.65) & (df["AH_H_neg_1_5"] <= 2.30), df["AH_H_neg_1_5"], ((gh - ga) >= 2)),
        ("Back AH Away +1.5 (Fav H 1.45-1.75 & U25 <= 1.65)", (oh_b >= 1.45) & (oh_b <= 1.75) & (u25_b <= 1.65) & (df["AH_A_pos_1_5"] >= 1.50) & (df["AH_A_pos_1_5"] <= 2.10), df["AH_A_pos_1_5"], ((ga - gh) >= -1)),
        ("Back AH Home +0.5 (1X) (Fav A 1.85-2.30 & U25 <= 1.65)", (oa_b >= 1.85) & (oa_b <= 2.30) & (u25_b <= 1.65) & (df["AH_H_pos_0_5"] >= 1.55) & (df["AH_H_pos_0_5"] <= 2.10), df["AH_H_pos_0_5"], (gh >= ga)),
        ("Back AH Away +0.5 (X2) (Fav H 1.85-2.30 & U25 <= 1.65)", (oh_b >= 1.85) & (oh_b <= 2.30) & (u25_b <= 1.65) & (df["AH_A_pos_0_5"] >= 1.55) & (df["AH_A_pos_0_5"] <= 2.10), df["AH_A_pos_0_5"], (ga >= gh)),
    ]
    for ah_name, ah_m, ah_col, ah_win in ah_tests:
        res = evaluate_strategy(ah_name, "3_DoubleChance_Handicap", ah_m, ah_col, ah_win, side="BACK", min_n_clean=100, min_n_aug_sep=3)
        if res: results.append(res)

# ==============================================================================
# FAMÍLIA 4: HÍBRIDOS FORMA HISTÓRICA (ROLLING LEAK-FREE) + PREÇO EXCHANGE
# ==============================================================================
print("[4/6] Escaneando Família 4: Híbridos Forma Histórica (Rolling shift(1)) + Preço Betfair Exchange...")

form_hybrids = [
    # 4A. Lay Away Blindado por Forma (Mandante forte em casa + Visitante não vence fora)
    ("Lay Away Blindado (Fav_H <= 1.55 | H_WinRate >= 60% | A_WinRate <= 20% | Lay 4.0-15.0)",
     (oh_b <= 1.55) & (df["H_Roll_WinRate"] >= 0.60) & (df["A_Roll_WinRate"] <= 0.20) & (oa_l >= 4.0) & (oa_l <= 15.0),
     oa_l, (gh >= ga), "LAY", False),
     
    ("Lay Away Clean-Sheet (Fav_H <= 1.50 | H_CS_Rate >= 40% | A_FTS_Rate >= 40% | Lay 4.0-15.0)",
     (oh_b <= 1.50) & (df["H_Roll_CS_Rate"] >= 0.40) & (df["A_Roll_FTS_Rate"] >= 0.40) & (oa_l >= 4.0) & (oa_l <= 15.0),
     oa_l, (gh >= ga), "LAY", False),

    # 4B. Lay Home Blindado por Forma (Visitante forte fora + Mandante fraco em casa)
    ("Lay Home Blindado (Fav_A <= 1.75 | A_WinRate >= 50% | H_WinRate <= 25% | Lay 2.2-10.0)",
     (oa_b <= 1.75) & (df["A_Roll_WinRate"] >= 0.50) & (df["H_Roll_WinRate"] <= 0.25) & (oh_l >= 2.2) & (oh_l <= 10.0),
     oh_l, (gh <= ga), "LAY", False),

    # 4C. Lay Draw Blindado por Liga Pouco Empatadora (Liga_DrawRate <= 24% + Fav <= 1.45)
    ("Lay Draw Liga Anti-Empate (Fav <= 1.45 | Liga_DrawRate <= 24% | Sem Copas/Seleções | Lay 4.5-10.0)",
     (np.minimum(oh_b, oa_b) <= 1.45) & (df["Liga_Roll_DrawRate"] <= 0.24) & df["Is_Liga_Nac"] & (od_l >= 4.5) & (od_l <= 10.0),
     od_l, (gh != ga), "LAY", False),

    # 4D. Lay Over 4.5 / 3.5 Blindado por Média de Gols Histórica dos Times
    ("Lay Over 4.5 Forma Under (U25 <= 1.60 | H_GoalsTot <= 2.4 & A_GoalsTot <= 2.4 | Lay 4.0-18.0)",
     (u25_b <= 1.60) & (df["H_Roll_GoalsTot"] <= 2.4) & (df["A_Roll_GoalsTot"] <= 2.4) & (o45_l >= 4.0) & (o45_l <= 18.0),
     o45_l, (gtot <= 4), "LAY", False),

    ("Lay Over 3.5 Forma Under (U25 <= 1.55 | H_GoalsTot <= 2.2 & A_GoalsTot <= 2.2 | Lay 2.6-8.5)",
     (u25_b <= 1.55) & (df["H_Roll_GoalsTot"] <= 2.2) & (df["A_Roll_GoalsTot"] <= 2.2) & (o35_l >= 2.6) & (o35_l <= 8.5),
     o35_l, (gtot <= 3), "LAY", False),

    # 4E. Lay Under 0.5 FT / Lay 0x0 Blindado por Liga Goleadora + Ataque Forte
    ("Lay Under 0.5 FT Ataque Forte (Fav <= 1.50 | Liga_0x0Rate <= 7% | H_GF >= 1.8 | Lay 7.5-18.0)",
     (np.minimum(oh_b, oa_b) <= 1.50) & (df["Liga_Roll_0x0Rate"] <= 0.07) & (df["H_Roll_GF"] >= 1.8) & (u05_l >= 7.5) & (u05_l <= 18.0),
     u05_l, (gtot >= 1), "LAY", False),
]

for fh_name, fh_m, fh_odd, fh_win, fh_side, fh_cs in form_hybrids:
    res = evaluate_strategy(fh_name, "4_Form_Price_Hybrid", fh_m, fh_odd, fh_win, side=fh_side, is_cs=fh_cs, min_n_clean=120, min_n_aug_sep=6)
    if res: results.append(res)

# ==============================================================================
# FAMÍLIA 5: OTIMIZAÇÃO DE FILTROS & CHAVEAMENTOS DOS MÉTODOS DE PRODUÇÃO (PORTFÓLIO B7)
# ==============================================================================
print("[5/6] Escaneando Família 5: Refinamento & Chaveamentos dentro dos Métodos Ativos do Portfólio...")

# Testar se o Lay Home (Fav Visitante <= 1.65) também tem uma zona de perigo (ex.: Copa / Seleção ou Over/Under extremo)
for sub_name, sub_m in [
    ("Lay Home Base Produção (Fav_A <= 1.65 | Lay 2.0-10.0)", (oa_b <= 1.65) & (oh_l >= 2.0) & (oh_l <= 10.0)),
    ("Lay Home Sem Seleções/Copas (Fav_A <= 1.65 | Apenas Ligas Nacionais | Lay 2.0-10.0)", (oa_b <= 1.65) & df["Is_Liga_Nac"] & (oh_l >= 2.0) & (oh_l <= 10.0)),
    ("Lay Home Sweet-Spot (Fav_A <= 1.60 | Sem Seleções | Lay 2.4-8.5)", (oa_b <= 1.60) & (~df["Is_Selecao"]) & (oh_l >= 2.4) & (oh_l <= 8.5)),
    ("Lay Over 4.5 Base Produção (U25 <= 1.50 | Lay 4.0-20.0)", (u25_b <= 1.50) & (o45_l >= 4.0) & (o45_l <= 20.0)),
    ("Lay Over 4.5 Sweet-Spot (U25 <= 1.50 | Lay 4.0-15.0 | Sem Goleadores)", (u25_b <= 1.50) & (o45_l >= 4.0) & (o45_l <= 15.0)),
]:
    odd_s = oh_l if "Lay Home" in sub_name else o45_l
    win_s = (gh <= ga) if "Lay Home" in sub_name else (gtot <= 4)
    res = evaluate_strategy(sub_name, "5_Portfolio_Refinement", sub_m, odd_s, win_s, side="LAY", min_n_clean=150, min_n_aug_sep=10)
    if res: results.append(res)

# Ordenar todos os candidatos aprovados pelo ROI_3Y_5pct e Z_Score
df_res = pd.DataFrame(results)
if not df_res.empty:
    df_res = df_res.drop(columns=["Mask_AgoSet26"]).sort_values(["ROI_3Y_5pct", "ROI_AgoSet26_5pct"], ascending=False).reset_index(drop=True)
    print(f"\n[+] TOTAL DE MÉTODOS QUE PASSARAM POR TODOS OS PORTÕES (2024 + 2025 + Mai-Set/26 + Ago-Set/26): {len(df_res)}")
    print("="*100)
    cols_show = ["Family", "Name", "N_3Y_Clean", "WR_3Y", "Margin_pp", "ROI_3Y_5pct", "ROI_3Y_35pct", "Z_Score", "ROI_2024", "ROI_2025", "ROI_MaiSet26", "N_AgoSet26", "WR_AgoSet26", "ROI_AgoSet26_5pct"]
    print(df_res[cols_show].head(35).to_string(index=False))
    df_res.to_csv(ROOT / "ranking_mineracao_noturna_360.csv", index=False)
else:
    print("[-] Nenhum método passou todos os filtros.")
