# -*- coding: utf-8 -*-
"""
auditar_filtro_spread.py
Executa a auditoria completa solicitada pelo Claude sobre a hipótese:
Filtro de Spread no Lay Draw (Odd_D_Lay / Odd_D_Back <= 1.15).

Responde rigorosamente aos 5 vetores de ataque:
1. Varredura de Limiar (Threshold Scan)
2. Spread da base vs Coletor KO-10 (Liquidez dinâmica)
3. Reconciliação dos números (Bet365 N=2013 vs Betfair N=843/552)
4. Sobrevivência em 2024-2025
5. Teste de Permutação (1.000 iterações estratificadas por dia)
"""
import sys, os, re
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass
import pandas as pd
import numpy as np

ROOT = os.path.dirname(os.path.abspath(__file__))
import sinais_ko_core as K

CACHE_BF = os.path.join(ROOT, "metodos_aprovados", ".cache_base_betfair.csv")
BASE_B365 = os.path.join(ROOT, "Bases_de_Dados_API_FutPythonTrader_Bet365.csv")

# ==============================================================================
# CARREGAMENTO E PREPARAÇÃO DA BASE BETFAIR
# ==============================================================================
print("Carregando .cache_base_betfair.csv...")
bf = pd.read_csv(CACHE_BF, low_memory=False)
bf["Date_raw"] = bf["Date"].astype(str)
bf["Date"] = pd.to_datetime(bf["Date"], errors="coerce")
bf = bf.dropna(subset=["Date"]).copy()
bf["Date_str"] = bf["Date"].dt.strftime("%Y-%m-%d")
bf["ano"] = bf["Date"].dt.year

for col in ["Odd_H_Back", "Odd_A_Back", "Odd_D_Back", "Odd_D_Lay", "Goals_H_FT", "Goals_A_FT"]:
    bf[col] = pd.to_numeric(bf.get(col), errors="coerce")

bf = bf.dropna(subset=["Odd_H_Back", "Odd_A_Back", "Odd_D_Back", "Odd_D_Lay", "Goals_H_FT", "Goals_A_FT"]).copy()

RE_COPA = re.compile(r"\b(CUP|COPA|CHAMPIONS|EUROPA LEAGUE|CONFERENCE|LIBERTADORES|SUDAMERICANA|POKAL|COUPE|COPPA|TACA|TROPHY|SHIELD|SUPERCUP|SUPER CUP|QUALIF|PLAYOFF)\b", re.I)
bf["copa"] = bf["League"].astype(str).apply(lambda x: bool(RE_COPA.search(x)))
bf["fav_back"] = bf[["Odd_H_Back", "Odd_A_Back"]].min(axis=1)
bf["ign"] = bf.apply(lambda r: K.eh_jogo_ignorado(r["Home"], r["Away"], r["League"]), axis=1)

ok_fav = np.where(bf["copa"], bf["Odd_H_Back"] <= 1.40, bf["fav_back"] <= 1.40)
bf["qualifica"] = ok_fav & ~bf["ign"]

bf["spread"] = bf["Odd_D_Lay"] / bf["Odd_D_Back"]
bf["draw"] = bf["Goals_H_FT"] == bf["Goals_A_FT"]

# PnL Liability (comissão 5% padrão Betfair)
COMM = 0.05
bf["pnl_5"] = np.where(bf["draw"], -1.0, (1.0 - COMM) / (bf["Odd_D_Lay"] - 1.0))
# PnL Liability (comissão 3% OrbitX)
COMM_3 = 0.03
bf["pnl_3"] = np.where(bf["draw"], -1.0, (1.0 - COMM_3) / (bf["Odd_D_Lay"] - 1.0))

# Break-even individual
bf["be_5"] = (bf["Odd_D_Lay"] - 1.0) / (bf["Odd_D_Lay"] - COMM)

print(f"Total registros limpos na base Betfair: {len(bf)} jogos.")

# ==============================================================================
# PARTE 1: RECONCILIAÇÃO DOS NÚMEROS (2.013 vs 843 vs 552)
# ==============================================================================
print("\n" + "="*80)
print("VETOR 3: RECONCILIAÇÃO DOS NÚMEROS (Bet365 N=2.013 vs Betfair N=843/552)")
print("="*80)

# Cohort 2026 na Betfair
bf_2026 = bf[(bf["ano"] == 2026) & bf["qualifica"]].copy()
bf_2026_w10 = bf_2026[bf_2026["Odd_D_Lay"].between(4.5, 10.0)].copy()
bf_2026_w7 = bf_2026[bf_2026["Odd_D_Lay"].between(4.5, 7.0)].copy()

print(f"Base Betfair 2026 com governanca + janela [4.5, 10.0]: N = {len(bf_2026_w10)}")
print(f"  Odd_D_Lay media = {bf_2026_w10['Odd_D_Lay'].mean():.2f} | mediana = {bf_2026_w10['Odd_D_Lay'].median():.2f}")
print(f"  WR = {(1-bf_2026_w10['draw'].mean())*100:.2f}% | BE_5% = {bf_2026_w10['be_5'].mean()*100:.2f}% | ROI_5% = {bf_2026_w10['pnl_5'].mean()*100:+.2f}%")

print(f"\nBase Betfair 2026 com governanca + teto [4.5, 7.00]: N = {len(bf_2026_w7)}")
print(f"  Odd_D_Lay media = {bf_2026_w7['Odd_D_Lay'].mean():.2f} | mediana = {bf_2026_w7['Odd_D_Lay'].median():.2f}")
print(f"  WR = {(1-bf_2026_w7['draw'].mean())*100:.2f}% | BE_5% = {bf_2026_w7['be_5'].mean()*100:.2f}% | ROI_5% = {bf_2026_w7['pnl_5'].mean()*100:+.2f}%")

# Agora carregar Bet365 2026 para conferir N=2013
if os.path.exists(BASE_B365):
    b365 = pd.read_csv(BASE_B365, low_memory=False)
    b365["Date"] = pd.to_datetime(b365["Date"], errors="coerce")
    b365 = b365[b365["Date"].dt.year == 2026].dropna(subset=["Goals_H_FT", "Goals_A_FT", "Odd_H_FT", "Odd_D_FT", "Odd_A_FT"]).copy()
    b365["copa"] = b365["League"].astype(str).apply(lambda x: bool(RE_COPA.search(x)))
    b365["fav"] = b365[["Odd_H_FT", "Odd_A_FT"]].min(axis=1)
    b365["ign"] = b365.apply(lambda r: K.eh_jogo_ignorado(r["Home"], r["Away"], r["League"]), axis=1)
    cond_fav_b365 = np.where(b365["copa"], b365["Odd_H_FT"] <= 1.40, b365["fav"] <= 1.40)
    b365["odd_lay_est"] = b365["Odd_D_FT"] * 1.044
    b365_sub = b365[cond_fav_b365 & ~b365["ign"] & (b365["odd_lay_est"] >= 4.50) & (b365["odd_lay_est"] <= 7.00)].copy()
    b365_sub["draw"] = b365_sub["Goals_H_FT"] == b365_sub["Goals_A_FT"]
    b365_sub["pnl_3"] = np.where(b365_sub["draw"], -1.0, 0.97 / (b365_sub["odd_lay_est"] - 1.0))
    b365_sub["be_3"] = (b365_sub["odd_lay_est"] - 1.0) / (b365_sub["odd_lay_est"] - 0.03)
    print(f"\nBase Bet365 2026 com spread sintético 1.044x + teto [4.5, 7.00] + c=3%:")
    print(f"  N = {len(b365_sub)} | WR = {(1-b365_sub['draw'].mean())*100:.2f}% | BE_3% = {b365_sub['be_3'].mean()*100:.2f}% | ROI_3% = {b365_sub['pnl_3'].mean()*100:+.2f}%")
    print(f"  Odd_D_FT media = {b365_sub['Odd_D_FT'].mean():.2f} | Odd_Lay_Est media = {b365_sub['odd_lay_est'].mean():.2f}")

# ==============================================================================
# PARTE 2: VARREDURA DE LIMIAR (THRESHOLD SCAN) EM 2026
# ==============================================================================
print("\n" + "="*80)
print("VETOR 1: VARREDURA DE LIMIAR (THRESHOLD SCAN) EM 2026")
print("="*80)

CORTE_DATA = "2026-05-15"
bf_2026_w10["H1"] = bf_2026_w10["Date_str"] <= CORTE_DATA

def avaliar_corte(df, col, limiar):
    d_le = df[df[col] <= limiar]
    d_gt = df[df[col] > limiar]
    
    def stats(d):
        n = len(d)
        if n == 0:
            return {"n": 0, "wr": 0.0, "roi": 0.0, "pnl": 0.0}
        wr = (1.0 - d["draw"].mean()) * 100
        roi = d["pnl_5"].mean() * 100
        pnl = d["pnl_5"].sum()
        return {"n": n, "wr": wr, "roi": roi, "pnl": pnl}
        
    s_le = stats(d_le)
    s_gt = stats(d_gt)
    return s_le, s_gt

limiares = [1.04, 1.06, 1.08, 1.10, 1.12, 1.14, 1.15, 1.16, 1.18, 1.20, 1.25, 1.30]

print("\n--- Varredura de Limiares em 2026 Completo (Jan-Out, N=843, Lay [4.5, 10.0]) ---")
print(f"{'Limiar':<8} | {'N <= T':<6} {'WR':<7} {'ROI':<8} {'PnL':<8} | {'N > T':<6} {'WR':<7} {'ROI':<8} {'PnL':<8} | {'Delta ROI':<10}")
print("-" * 80)
for T in limiares:
    le, gt = avaliar_corte(bf_2026_w10, "spread", T)
    delta_roi = le["roi"] - gt["roi"]
    print(f"{T:<8.2f} | {le['n']:<6d} {le['wr']:<7.2f}% {le['roi']:<+7.2f}% {le['pnl']:<+7.2f}u | {gt['n']:<6d} {gt['wr']:<7.2f}% {gt['roi']:<+7.2f}% {gt['pnl']:<+7.2f}u | {delta_roi:<+9.2f}pp")

print("\n--- Varredura de Limiares em H1 (Jan-Mai, N=514) vs H2 (Mai-Out, N=329) ---")
h1 = bf_2026_w10[bf_2026_w10["H1"]]
h2 = bf_2026_w10[~bf_2026_w10["H1"]]

print(f"{'Limiar':<8} | {'H1 ROI <=T':<11} {'H1 ROI >T':<11} | {'H2 ROI <=T':<11} {'H2 ROI >T':<11} | {'Sustenta em H2?':<15}")
print("-" * 75)
for T in limiares:
    le_h1, gt_h1 = avaliar_corte(h1, "spread", T)
    le_h2, gt_h2 = avaliar_corte(h2, "spread", T)
    sustenta = "SIM" if le_h2["roi"] > gt_h2["roi"] and gt_h2["roi"] < 0 else "NÃO"
    print(f"{T:<8.2f} | {le_h1['roi']:<+10.2f}% {gt_h1['roi']:<+10.2f}% | {le_h2['roi']:<+10.2f}% {gt_h2['roi']:<+10.2f}% | {sustenta}")

print("\n--- Fatiamento Fino por Decis de Spread em 2026 (N=843) ---")
bins_finos = [1.00, 1.02, 1.04, 1.06, 1.08, 1.10, 1.12, 1.15, 1.20, 1.30, 5.00]
bf_2026_w10["faixa_fina"] = pd.cut(bf_2026_w10["spread"], bins_finos)
tab_fina = bf_2026_w10.groupby("faixa_fina", observed=True).agg(
    n=("draw", "size"),
    reds=("draw", "sum"),
    roi=("pnl_5", "mean"),
    pnl=("pnl_5", "sum"),
    odd_lay_med=("Odd_D_Lay", "median"),
    odd_back_med=("Odd_D_Back", "median")
)
tab_fina["wr"] = (1.0 - tab_fina["reds"] / tab_fina["n"]) * 100
tab_fina["roi"] = tab_fina["roi"] * 100
print(tab_fina[["n", "wr", "roi", "pnl", "odd_lay_med", "odd_back_med"]].round(2).to_string())

# ==============================================================================
# PARTE 3: SOBREVIVÊNCIA EM 2024 E 2025
# ==============================================================================
print("\n" + "="*80)
print("VETOR 4: SOBREVIVÊNCIA EM 2024 E 2025 (.cache_base_betfair.csv)")
print("="*80)

for ano in [2024, 2025, 2026]:
    df_ano = bf[(bf["ano"] == ano) & bf["qualifica"] & bf["Odd_D_Lay"].between(4.5, 10.0)].copy()
    n_tot = len(df_ano)
    if n_tot == 0:
        continue
    spread_med = df_ano["spread"].median()
    
    le, gt = avaliar_corte(df_ano, "spread", 1.15)
    print(f"\n--- ANO {ano} (N={n_tot}, Spread Mediano={spread_med:.4f}) ---")
    print(f"Base Total:       N={n_tot:4d} | WR={(1-df_ano['draw'].mean())*100:5.2f}% | ROI={df_ano['pnl_5'].mean()*100:+5.2f}% | PnL={df_ano['pnl_5'].sum():+6.2f}u")
    print(f"Spread <= 1.15:   N={le['n']:4d} | WR={le['wr']:5.2f}% | ROI={le['roi']:+5.2f}% | PnL={le['pnl']:+6.2f}u")
    print(f"Spread >  1.15:   N={gt['n']:4d} | WR={gt['wr']:5.2f}% | ROI={gt['roi']:+5.2f}% | PnL={gt['pnl']:+6.2f}u")
    print(f"Delta (<= - >):   {le['roi'] - gt['roi']:+5.2f} pp")

    # Também testar com teto 7.00
    df_ano_w7 = df_ano[df_ano["Odd_D_Lay"] <= 7.00]
    le7, gt7 = avaliar_corte(df_ano_w7, "spread", 1.15)
    print(f"Com teto <= 7.00: Spread <= 1.15: N={le7['n']:3d}, ROI={le7['roi']:+5.2f}% | Spread > 1.15: N={gt7['n']:3d}, ROI={gt7['roi']:+5.2f}% | Delta: {le7['roi']-gt7['roi']:+5.2f} pp")

# ==============================================================================
# PARTE 4: TESTE DE PERMUTAÇÃO (ESTRATIFICADO POR DIA)
# ==============================================================================
print("\n" + "="*80)
print("VETOR 5: TESTE DE PERMUTAÇÃO (1.000 SHUFFLES ESTRATIFICADOS POR DIA EM 2026)")
print("="*80)

# Observado em 2026:
obs_le = bf_2026_w10[bf_2026_w10["spread"] <= 1.15]["pnl_5"].mean() * 100
obs_gt = bf_2026_w10[bf_2026_w10["spread"] > 1.15]["pnl_5"].mean() * 100
obs_diff = obs_le - obs_gt
print(f"Diferença observada de ROI (spread<=1.15 vs spread>1.15): {obs_diff:+.2f} pp ({obs_le:+.2f}% vs {obs_gt:+.2f}%)")

np.random.seed(42)
N_PERM = 2000
diffs = []

# Preparar dados para permutação estratificada por dia
dias = bf_2026_w10["Date_str"].values
pnls = bf_2026_w10["pnl_5"].values
is_largo = (bf_2026_w10["spread"] > 1.15).values

unique_dates = np.unique(dias)
date_indices = [np.where(dias == d)[0] for d in unique_dates]

for _ in range(N_PERM):
    shuffled_largo = np.empty_like(is_largo)
    for idxs in date_indices:
        shuffled_largo[idxs] = np.random.permutation(is_largo[idxs])
    
    mean_le = pnls[~shuffled_largo].mean() * 100
    mean_gt = pnls[shuffled_largo].mean() * 100
    diffs.append(mean_le - mean_gt)

diffs = np.array(diffs)
p_val_one_tailed = (diffs >= obs_diff).mean()
p_val_two_tailed = (np.abs(diffs) >= np.abs(obs_diff)).mean()

print(f"Resultados da Permutação ({N_PERM} iterações):")
print(f"  Diferença média esperada sob hipótese nula H0: {diffs.mean():+.2f} pp")
print(f"  Desvio padrão da diferença sob H0: {diffs.std():.2f} pp")
print(f"  Intervalo 95% sob H0: [{np.percentile(diffs, 2.5):+.2f} pp, {np.percentile(diffs, 97.5):+.2f} pp]")
print(f"  p-valor empírico (unilateral, diff >= {obs_diff:.2f} pp): {p_val_one_tailed:.4f}")
print(f"  p-valor empírico (bilateral, |diff| >= {obs_diff:.2f} pp): {p_val_two_tailed:.4f}")

# ==============================================================================
# PARTE 5: SPREAD NA BASE VS COLETOR KO-10 (DINÂMICA DE LIQUIDEZ)
# ==============================================================================
print("\n" + "="*80)
print("VETOR 2: SPREAD NA BASE VS NO COLETOR KO-10")
print("="*80)

import glob, gzip
gz_files = sorted(glob.glob(os.path.join(ROOT, "backup_coletor", "*.csv.gz")))
print(f"Arquivos do coletor encontrados: {len(gz_files)}")

spreads_ko = []
for gz in gz_files[-2:]:
    print(f"Lendo amostra de {os.path.basename(gz)}...")
    with gzip.open(gz, "rt", encoding="utf-8", errors="replace") as f:
        for line in f:
            if "The Draw" in line and "MATCH_ODDS" in line:
                parts = line.strip().split(",")
                if len(parts) >= 14:
                    try:
                        min_to_ko = float(parts[6])
                        if 4.0 <= min_to_ko <= 16.0:
                            back_odd = float(parts[9])
                            lay_odd = float(parts[11])
                            if back_odd > 1.0 and lay_odd > 1.0 and lay_odd >= back_odd:
                                spreads_ko.append({
                                    "min_to_ko": min_to_ko,
                                    "back": back_odd,
                                    "lay": lay_odd,
                                    "spread": lay_odd / back_odd,
                                    "league": parts[2]
                                })
                    except Exception:
                        pass
            if len(spreads_ko) >= 3000:
                break
    if len(spreads_ko) >= 3000:
        break

if spreads_ko:
    df_ko = pd.DataFrame(spreads_ko)
    print(f"\nAmostra coletada no KO-10: N={len(df_ko)} snapshots do mercado The Draw:")
    print(f"  Spread mediano no KO-10: {df_ko['spread'].median():.4f}")
    print(f"  Percentil 25: {df_ko['spread'].quantile(0.25):.4f} | Percentil 75: {df_ko['spread'].quantile(0.75):.4f} | Percentil 90: {df_ko['spread'].quantile(0.90):.4f}")
    print(f"  % de jogos com spread <= 1.15 no KO-10: {(df_ko['spread'] <= 1.15).mean()*100:.1f}%")
    print(f"  % de jogos com spread > 1.15 no KO-10: {(df_ko['spread'] > 1.15).mean()*100:.1f}%")
else:
    print("Nenhum registro do coletor processado.")

print("\nAuditoria concluída com sucesso!")
