#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
testar_eixo2_regressao_media_xg.py — Execução do Estudo Eixo 2: Regressão à Média em Finalização (Luck Fade via xG)
Autoridade: PREREGISTRO_eixo2_regressao_media_xg.md + GEMINI.md
"""

import os, sys, glob, unicodedata, re, warnings
import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy import stats

warnings.filterwarnings("ignore")
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass

ROOT = os.path.dirname(os.path.abspath(__file__))
COMISSAO = 0.05

def canon(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)

def carregar_dados():
    print("1. Carregando estatisticas_jogos.csv (32.070 jogos de 2026)...")
    st_path = os.path.join(ROOT, "estatisticas_jogos.csv")
    df_st = pd.read_csv(st_path, low_memory=False)
    df_st["Data"] = pd.to_datetime(df_st["Data"]).dt.strftime("%Y-%m-%d")
    df_st["ch"] = df_st["Home"].apply(canon)
    df_st["ca"] = df_st["Away"].apply(canon)
    
    # Numéricos de gols e xG
    for c in ["gh", "ga", "xg_h", "xg_a"]:
        df_st[c] = pd.to_numeric(df_st[c], errors="coerce")
    
    print("2. Carregando odds reais Betfair pré-jogo (FRESH3 + feeds diários)...")
    odds_dict = {}
    
    # A) FRESH3
    bf_fresh = os.path.join(ROOT, "Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH3.csv")
    if os.path.exists(bf_fresh):
        df_bf = pd.read_csv(bf_fresh, low_memory=False)
        df_bf["Data"] = pd.to_datetime(df_bf["Date"]).dt.strftime("%Y-%m-%d")
        df_bf["ch"] = df_bf["Home"].apply(canon)
        df_bf["ca"] = df_bf["Away"].apply(canon)
        cols_bf = ["Odd_H_Lay", "Odd_A_Lay", "Odd_D_Lay", "Odd_Over25_FT_Lay", "Odd_H_Back", "Odd_A_Back", "Odd_Over25_FT_Back"]
        for _, r in df_bf.iterrows():
            k = (r["Data"], r["ch"], r["ca"])
            val = {c: pd.to_numeric(r.get(c), errors="coerce") for c in cols_bf}
            odds_dict[k] = val
        print(f"   -> {len(odds_dict)} jogos carregados da FRESH3")
        
    # B) Feeds diários parquets
    parquets = glob.glob(os.path.join(ROOT, "scratch", "feed_arquivo", "*.parquet"))
    n_pq = 0
    for p in parquets:
        try:
            pq = pd.read_parquet(p)
            d_str = os.path.basename(p).replace("feed_forward_diario_", "").replace(".parquet", "")
            pq["ch"] = pq["Home"].apply(canon)
            pq["ca"] = pq["Away"].apply(canon)
            for _, r in pq.iterrows():
                k = (d_str, r["ch"], r["ca"])
                val = {
                    "Odd_H_Lay": pd.to_numeric(r.get("Odd_H_Lay"), errors="coerce"),
                    "Odd_A_Lay": pd.to_numeric(r.get("Odd_A_Lay"), errors="coerce"),
                    "Odd_D_Lay": pd.to_numeric(r.get("Odd_D_Lay"), errors="coerce"),
                    "Odd_Over25_FT_Lay": pd.to_numeric(r.get("Odd_Over25_FT_Lay"), errors="coerce"),
                    "Odd_H_Back": pd.to_numeric(r.get("Odd_H_Back"), errors="coerce"),
                    "Odd_A_Back": pd.to_numeric(r.get("Odd_A_Back"), errors="coerce"),
                    "Odd_Over25_FT_Back": pd.to_numeric(r.get("Odd_Over25_FT_Back"), errors="coerce"),
                }
                odds_dict[k] = val
                n_pq += 1
        except Exception:
            pass
    print(f"   -> {n_pq} registros adicionados/atualizados via parquets diários")
    
    # Acoplar odds na base de stats
    odds_rows = []
    for _, r in df_st.iterrows():
        k = (r["Data"], r["ch"], r["ca"])
        o = odds_dict.get(k, {})
        odds_rows.append(o)
    df_odds = pd.DataFrame(odds_rows)
    for c in df_odds.columns:
        df_st[c] = df_odds[c]
        
    return df_st

def calcular_features_leak_free(df):
    print("\n3. Calculando features de Sorte/Azar em finalização com shift(1) estrito...")
    df = df.sort_values("Data").reset_index(drop=True)
    
    # Criar tabela de partidas por time
    team_matches = []
    for idx, r in df.iterrows():
        d = r["Data"]
        # Só consideramos partidas com xG válido para construir a métrica de finalização
        tem_xg = pd.notna(r["xg_h"]) and pd.notna(r["xg_a"]) and pd.notna(r["gh"]) and pd.notna(r["ga"])
        
        team_matches.append({
            "match_idx": idx, "data": d, "team": r["ch"], "opp": r["ca"], "venue": "H",
            "gf": r["gh"] if tem_xg else np.nan,
            "ga": r["ga"] if tem_xg else np.nan,
            "xgf": r["xg_h"] if tem_xg else np.nan,
            "xga": r["xg_a"] if tem_xg else np.nan,
            "tem_xg": tem_xg
        })
        team_matches.append({
            "match_idx": idx, "data": d, "team": r["ca"], "opp": r["ch"], "venue": "A",
            "gf": r["ga"] if tem_xg else np.nan,
            "ga": r["gh"] if tem_xg else np.nan,
            "xgf": r["xg_a"] if tem_xg else np.nan,
            "xga": r["xg_h"] if tem_xg else np.nan,
            "tem_xg": tem_xg
        })
        
    t_df = pd.DataFrame(team_matches)
    t_df = t_df.sort_values(["team", "data", "match_idx"]).reset_index(drop=True)
    
    # Resíduos de cada jogo
    t_df["res_off"] = t_df["gf"] - t_df["xgf"] # Gols marcados - xG gerado
    t_df["res_def"] = t_df["ga"] - t_df["xga"] # Gols sofridos - xGA cedido
    t_df["luck_net"] = t_df["res_off"] - t_df["res_def"]
    
    # Rolling K=5 com shift(1) obrigatório
    K = 5
    for col in ["res_off", "res_def", "luck_net"]:
        t_df[f"roll_{col}"] = (
            t_df.groupby("team")[col]
            .apply(lambda s: s.shift(1).rolling(K, min_periods=3).mean())
            .reset_index(level=0, drop=True)
        )
    # Contagem de jogos prévios válidos com xG
    t_df["roll_n_valid"] = (
        t_df.groupby("team")["tem_xg"]
        .apply(lambda s: s.shift(1).rolling(K, min_periods=3).sum())
        .reset_index(level=0, drop=True)
    )
    
    # Reintegrar ao DataFrame mestre
    h_side = t_df[t_df["venue"] == "H"].set_index("match_idx")
    a_side = t_df[t_df["venue"] == "A"].set_index("match_idx")
    
    df["H_luck_off"] = h_side["roll_res_off"]
    df["H_luck_net"] = h_side["roll_luck_net"]
    df["H_n_valid"] = h_side["roll_n_valid"]
    
    df["A_luck_off"] = a_side["roll_res_off"]
    df["A_luck_net"] = a_side["roll_luck_net"]
    df["A_n_valid"] = a_side["roll_n_valid"]
    
    df["delta_luck_net"] = df["H_luck_net"] - df["A_luck_net"]
    df["sum_luck_off"] = df["H_luck_off"] + df["A_luck_off"]
    
    return df

def bootstrap_bloco_dia(pnl_series, date_series, n_iter=1000, seed=42):
    np.random.seed(seed)
    df_boot = pd.DataFrame({"pnl": pnl_series, "data": date_series})
    dias = df_boot["data"].unique()
    if len(dias) < 6:
        return np.nan, np.nan, np.nan
    
    day_groups = [df_boot[df_boot["data"] == d]["pnl"].values for d in dias]
    n_days = len(dias)
    
    rois = []
    for _ in range(n_iter):
        sampled_day_idxs = np.random.choice(n_days, size=n_days, replace=True)
        sampled_pnls = np.concatenate([day_groups[i] for i in sampled_day_idxs])
        if len(sampled_pnls) > 0:
            rois.append(np.mean(sampled_pnls) * 100.0)
            
    ci_low = np.percentile(rois, 2.5)
    ci_high = np.percentile(rois, 97.5)
    p_neg = np.mean(np.array(rois) <= 0.0)
    return ci_low, ci_high, p_neg

def rodar_estudo(df):
    print("\n" + "=" * 80)
    print("4. RELATÓRIO DE COBERTURA E ELEGIBILIDADE")
    print("=" * 80)
    n_total = len(df)
    n_com_odds = df["Odd_H_Lay"].notna().sum()
    n_com_xg = (df["xg_h"].notna() & df["xg_a"].notna()).sum()
    n_qualificados = df["delta_luck_net"].notna().sum()
    n_completos = (df["delta_luck_net"].notna() & df["Odd_H_Lay"].notna()).sum()
    
    print(f"Total de jogos de 2026 na base: {n_total}")
    print(f"Jogos com xG do próprio jogo disponível: {n_com_xg} ({n_com_xg/n_total*100:.1f}%)")
    print(f"Jogos com odds de Lay reais da Betfair: {n_com_odds} ({n_com_odds/n_total*100:.1f}%)")
    print(f"Jogos com histórico prévio de xG (ambos os times com N>=3 jogos): {n_qualificados} ({n_qualificados/n_total*100:.1f}%)")
    print(f"Jogos de 2026 com Histórico xG + Odds Betfair completas: {n_completos} ({n_completos/n_total*100:.1f}%)")
    
    # Filtrar universo elegível para teste estatístico
    df_valid = df[df["delta_luck_net"].notna() & df["Odd_H_Lay"].notna() & df["gh"].notna() & df["ga"].notna()].copy()
    df_valid["home_win"] = (df_valid["gh"] > df_valid["ga"]).astype(int)
    df_valid["away_win"] = (df_valid["gh"] < df_valid["ga"]).astype(int)
    df_valid["over25"] = (df_valid["gh"] + df_valid["ga"] > 2.5).astype(int)
    
    # 5. TESTE ECONOMÉTRICO DA CLOSING LINE (Regressão Logística)
    print("\n" + "=" * 80)
    print("5. TESTE DE EFICIÊNCIA DE MERCADO (Logit: P(Vitória) ~ log(Odd) + Delta_Luck)")
    print("=" * 80)
    df_reg = df_valid[df_valid["Odd_H_Back"] > 1.0].copy()
    df_reg["log_odd_h"] = np.log(df_reg["Odd_H_Back"])
    
    X = sm.add_constant(df_reg[["log_odd_h", "delta_luck_net"]])
    y = df_reg["home_win"]
    try:
        logit_model = sm.Logit(y, X).fit(disp=False)
        print(logit_model.summary())
        coef_luck = logit_model.params["delta_luck_net"]
        p_luck = logit_model.pvalues["delta_luck_net"]
        print(f"\n-> Coeficiente Delta_Luck: {coef_luck:.4f} | p-valor: {p_luck:.4f}")
        if p_luck >= 0.05:
            print("❌ O p-valor é >= 0.05! Delta_Luck NÃO possui poder preditivo estatístico após controlar pelo preço da Betfair!")
        else:
            print("✅ Delta_Luck é estatisticamente significante contra o preço!")
    except Exception as e:
        print(f"Erro na regressão: {e}")

    # 6. SIMULAÇÃO PRÉ-REGISTRADA DAS HIPÓTESES H2-A, H2-B, H2-C
    print("\n" + "=" * 80)
    print("6. GRADE DE RESULTADOS DAS HIPÓTESES PRÉ-REGISTRADAS")
    print("=" * 80)
    
    resultados = []
    
    # --- H2-A: Lay Mandante Sortudo ---
    # Mandante com muita sorte líquida prévia, Visitante neutro/azarado
    for tau in [0.40, 0.60, 0.80]:
        cands = df_valid[
            (df_valid["H_luck_net"] >= tau) & 
            (df_valid["A_luck_net"] <= 0.0) &
            (df_valid["Odd_H_Lay"] >= 1.50) & 
            (df_valid["Odd_H_Lay"] <= 4.00)
        ].copy()
        
        n = len(cands)
        if n >= 10:
            # Lay Home ganha se o mandante NÃO venceu
            win_lay = (cands["home_win"] == 0).astype(int)
            wr = win_lay.mean()
            be = ((cands["Odd_H_Lay"] - 1.0) / (cands["Odd_H_Lay"] - COMISSAO)).mean()
            # P&L liability 1u: GREEN = (1-c)/(odd-1), RED = -1.0
            pnl_u = np.where(win_lay == 1, (1.0 - COMISSAO) / (cands["Odd_H_Lay"] - 1.0), -1.0)
            roi = np.mean(pnl_u) * 100.0
            tot_pnl = np.sum(pnl_u)
            ci_l, ci_h, p_neg = bootstrap_bloco_dia(pnl_u, cands["Data"].values)
            
            resultados.append({
                "Hipotese": f"H2-A Lay Home Sortudo (tau={tau:.2f})",
                "N": n, "Greens": win_lay.sum(), "Reds": n - win_lay.sum(),
                "WR_Real": f"{wr*100:.2f}%", "BE_Medio": f"{be*100:.2f}%",
                "Margem_pp": f"{(wr - be)*100:+.2f}pp",
                "PnL_1u": f"{tot_pnl:+.2f}u", "ROI_Liab": f"{roi:+.2f}%",
                "IC95_Boot": f"[{ci_l:+.2f}%, {ci_h:+.2f}%]", "P_Neg": f"{p_neg:.3f}"
            })
            
    # --- H2-B: Lay Visitante Sortudo ---
    # Visitante com muita sorte líquida prévia, Mandante neutro/azarado
    for tau in [0.40, 0.60, 0.80]:
        cands = df_valid[
            (df_valid["A_luck_net"] >= tau) & 
            (df_valid["H_luck_net"] <= 0.0) &
            (df_valid["Odd_A_Lay"] >= 1.50) & 
            (df_valid["Odd_A_Lay"] <= 4.00)
        ].copy()
        
        n = len(cands)
        if n >= 10:
            # Lay Away ganha se o visitante NÃO venceu
            win_lay = (cands["away_win"] == 0).astype(int)
            wr = win_lay.mean()
            be = ((cands["Odd_A_Lay"] - 1.0) / (cands["Odd_A_Lay"] - COMISSAO)).mean()
            pnl_u = np.where(win_lay == 1, (1.0 - COMISSAO) / (cands["Odd_A_Lay"] - 1.0), -1.0)
            roi = np.mean(pnl_u) * 100.0
            tot_pnl = np.sum(pnl_u)
            ci_l, ci_h, p_neg = bootstrap_bloco_dia(pnl_u, cands["Data"].values)
            
            resultados.append({
                "Hipotese": f"H2-B Lay Away Sortudo (tau={tau:.2f})",
                "N": n, "Greens": win_lay.sum(), "Reds": n - win_lay.sum(),
                "WR_Real": f"{wr*100:.2f}%", "BE_Medio": f"{be*100:.2f}%",
                "Margem_pp": f"{(wr - be)*100:+.2f}pp",
                "PnL_1u": f"{tot_pnl:+.2f}u", "ROI_Liab": f"{roi:+.2f}%",
                "IC95_Boot": f"[{ci_l:+.2f}%, {ci_h:+.2f}%]", "P_Neg": f"{p_neg:.3f}"
            })
            
    # --- H2-C: Lay Over 2.5 (Ambos com overperformance ofensiva) ---
    for tau_ov in [0.60, 0.80, 1.00]:
        cands = df_valid[
            (df_valid["sum_luck_off"] >= tau_ov) &
            df_valid["Odd_Over25_FT_Lay"].notna() &
            (df_valid["Odd_Over25_FT_Lay"] >= 1.60) &
            (df_valid["Odd_Over25_FT_Lay"] <= 2.50)
        ].copy()
        
        n = len(cands)
        if n >= 10:
            # Lay Over 2.5 ganha se saíram 2 gols ou menos (over25 == 0)
            win_lay = (cands["over25"] == 0).astype(int)
            wr = win_lay.mean()
            be = ((cands["Odd_Over25_FT_Lay"] - 1.0) / (cands["Odd_Over25_FT_Lay"] - COMISSAO)).mean()
            pnl_u = np.where(win_lay == 1, (1.0 - COMISSAO) / (cands["Odd_Over25_FT_Lay"] - 1.0), -1.0)
            roi = np.mean(pnl_u) * 100.0
            tot_pnl = np.sum(pnl_u)
            ci_l, ci_h, p_neg = bootstrap_bloco_dia(pnl_u, cands["Data"].values)
            
            resultados.append({
                "Hipotese": f"H2-C Lay Over 2.5 (tau_sum={tau_ov:.2f})",
                "N": n, "Greens": win_lay.sum(), "Reds": n - win_lay.sum(),
                "WR_Real": f"{wr*100:.2f}%", "BE_Medio": f"{be*100:.2f}%",
                "Margem_pp": f"{(wr - be)*100:+.2f}pp",
                "PnL_1u": f"{tot_pnl:+.2f}u", "ROI_Liab": f"{roi:+.2f}%",
                "IC95_Boot": f"[{ci_l:+.2f}%, {ci_h:+.2f}%]", "P_Neg": f"{p_neg:.3f}"
            })
            
    df_res = pd.DataFrame(resultados)
    print(df_res.to_string(index=False))
    df_res.to_csv(os.path.join(ROOT, "scratch", "resultados_eixo2_luck_fade.csv"), index=False)

def main():
    df = carregar_dados()
    df = calcular_features_leak_free(df)
    rodar_estudo(df)

if __name__ == "__main__":
    main()
