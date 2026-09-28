import sys
sys.stdout.reconfigure(encoding='utf-8')
import warnings
warnings.filterwarnings('ignore')

import glob
import numpy as np
import pandas as pd
from pathlib import Path
from estrategia_lay_3x0 import avaliar_jogos_lay_3x0_grade

ROOT = Path(__file__).resolve().parent

print("="*105)
print("🚀 PARTE 1: BACKFILL & SINCRONIZAÇÃO DO LAY 3X0 TOP 3 NAS PLANILHAS DIÁRIAS (AGO-SET/2026)")
print("="*105)

# Carregar base consolidada Betfair Exchange para backfill e estudo de liquidez
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

df_all_dates = df.dropna(subset=["Date", "Home", "Away"]).drop_duplicates(subset=["Date", "Home", "Away"], keep="last").sort_values(["Date", "Time"]).reset_index(drop=True)
for c in df_all_dates.columns:
    if c.startswith("Odd_"):
        df_all_dates[c] = pd.to_numeric(df_all_dates[c], errors="coerce")

# Sincronizar Lay 3x0 Top 3 (Aprovado) nas planilhas existentes em metodos_aprovados/Sinais_Metodos_Aprovados_*.xlsx
excel_files = sorted(glob.glob(str(ROOT / "metodos_aprovados" / "Sinais_Metodos_Aprovados_*.xlsx")))
added_3x0_total = 0
g_3x0 = 0
r_3x0 = 0

for fp in excel_files:
    dt_str = Path(fp).stem.replace("Sinais_Metodos_Aprovados_", "")
    df_ex = pd.read_excel(fp)
    # Remover eventual 3x0 anterior para evitar duplicata
    if "Método" in df_ex.columns:
        df_ex = df_ex[~df_ex["Método"].astype(str).str.contains("3x0", na=False)].copy()
    
    sub_dia = df_all_dates[df_all_dates["Date"] == dt_str].copy()
    if sub_dia.empty:
        continue
    picks_3x0 = avaliar_jogos_lay_3x0_grade(sub_dia, top_n=3, u25_max=1.75, odd_h_min=1.80)
    if not picks_3x0:
        continue
        
    new_rows = []
    for p3 in picks_3x0:
        odd_e = round(float(p3["odd_lay"]), 2)
        liab_r = 200.0
        stake_r = round(liab_r / (odd_e - 1.0), 2)
        # Buscar placar real
        m_row = sub_dia[(sub_dia["Home"] == p3["home"]) & (sub_dia["Away"] == p3["away"])]
        gh_val = m_row["Goals_H_FT"].iloc[0] if not m_row.empty else np.nan
        ga_val = m_row["Goals_A_FT"].iloc[0] if not m_row.empty else np.nan
        
        if pd.notna(gh_val) and pd.notna(ga_val):
            gh_i, ga_i = int(gh_val), int(ga_val)
            placar_s = f"{gh_i}x{ga_i}"
            is_red = (gh_i == 3 and ga_i == 0)
            res_s = "RED" if is_red else "GREEN"
            st_s = "🔴 RED" if is_red else "🟢 GREEN"
            num_10 = 0 if is_red else 1
            pnl_u = -1.0 if is_red else round(0.965 / (odd_e - 1.0), 4)
            pnl_rs = -liab_r if is_red else round(stake_r * 0.965, 2)
            if is_red: r_3x0 += 1
            else: g_3x0 += 1
        else:
            placar_s = "vs"
            res_s = "PENDENTE"
            st_s = "⏳ PENDENTE"
            num_10 = np.nan
            pnl_u = 0.0
            pnl_rs = 0.0
            
        new_rows.append({
            "Data": dt_str, "Hora": p3["hora"], "Liga": p3["league"],
            "Jogo": f"{p3['home']} x {p3['away']}", "Home": p3["home"], "Away": p3["away"],
            "Método": "Lay 3x0 Top 3 (Aprovado)", "Mercado": "Correct Score (3x0)", "Lado": "LAY",
            "Odd_Entrada": odd_e, "Odd_Fav": 0.0,
            "Stake_Sugerida_R$": stake_r, "Lucro_Green_R$": round(stake_r * 0.965, 2),
            "Risco_Red_R$": liab_r, "Placar": placar_s, "Resultado": res_s,
            "1/0": num_10, "PnL_u": pnl_u, "PnL_R$": pnl_rs, "Status": st_s
        })
        added_3x0_total += 1
        
    df_merged = pd.concat([df_ex, pd.DataFrame(new_rows)], ignore_index=True)
    df_merged["Hora"] = df_merged["Hora"].fillna("15:00").astype(str).str.slice(0, 5)
    df_merged = df_merged.sort_values(["Data", "Hora"]).reset_index(drop=True)
    with pd.ExcelWriter(fp, engine="openpyxl") as writer:
        df_merged.to_excel(writer, index=False, sheet_name="Sinais_Aprovados")

# Também rodar gerar_sinais_manha('2026-09-25') para atualizar a planilha de hoje com Lay 3x0 Top 3
from automacao_diaria_aprovados import gerar_sinais_manha
df_hoje = gerar_sinais_manha("2026-09-25", enviar_telegram=False)

print(f"[+] Lay 3x0 Top 3 sincronizado nas planilhas diárias: {added_3x0_total} sinais adicionados ({g_3x0}G / {r_3x0}R | WR={g_3x0/max(1, g_3x0+r_3x0)*100:.2f}%)")
print(f"[+] Sinais de hoje (2026-09-25) com Lay 3x0 Top 3 integrado ({len(df_hoje)} sinais):")
if not df_hoje.empty:
    print(df_hoje[["Data", "Hora", "Liga", "Jogo", "Método", "Odd_Entrada"]].to_string(index=False))

# ==============================================================================
# PARTE 2: AUDITORIA PROFUNDA DE LIQUIDEZ, SPREAD E ALTERNATIVAS DO LAY OVER 2.5 HT
# ==============================================================================
print("\n" + "="*105)
print("🔬 PARTE 2: AUDITORIA DE LIQUIDEZ E SPREAD DO LAY OVER 2.5 HT (E EQUIVALENTES DE ALTA LIQUIDEZ)")
print("="*105)

df_ft = df_all_dates.dropna(subset=["Goals_H_FT", "Goals_A_FT", "Goals_H_HT", "Goals_A_HT"]).copy()
gh = df_ft["Goals_H_FT"].values
ga = df_ft["Goals_A_FT"].values
gtot = gh + ga
gh_ht = df_ft["Goals_H_HT"].values
ga_ht = df_ft["Goals_A_HT"].values
gtot_ht = gh_ht + ga_ht

oh_b, oa_b = df_ft["Odd_H_Back"], df_ft["Odd_A_Back"]
u25_b = df_ft["Odd_Under25_FT_Back"]
o25_ht_b = df_ft["Odd_Over25_HT_Back"]
o25_ht_l = df_ft["Odd_Over25_HT_Lay"]
o15_ht_b = df_ft["Odd_Over15_HT_Back"]
o15_ht_l = df_ft["Odd_Over15_HT_Lay"]
o35_ft_b = df_ft["Odd_Over35_FT_Back"]
o35_ft_l = df_ft["Odd_Over35_FT_Lay"]
o45_ft_b = df_ft["Odd_Over45_FT_Back"]
o45_ft_l = df_ft["Odd_Over45_FT_Lay"]

# Calcular Bid-Ask Spread Relativo (Odd_Lay / Odd_Back) — proxy direto da profundidade do livro na Betfair Exchange!
spread_o25_ht = o25_ht_l / o25_ht_b
spread_o15_ht = o15_ht_l / o15_ht_b
spread_o35_ft = o35_ft_l / o35_ft_b
spread_o45_ft = o45_ft_l / o45_ft_b

base_eq = (u25_b > 0) & (u25_b <= 1.65) & (np.minimum(oh_b, oa_b) >= 2.00) & (o25_ht_l >= 5.0) & (o25_ht_l <= 16.0)
sub_ht = df_ft[base_eq].copy()
sub_ht["spread_ratio"] = spread_o25_ht[base_eq]
sub_ht["spread_ticks"] = o25_ht_l[base_eq] - o25_ht_b[base_eq]

print(f"\n1) DIAGNÓSTICO DO LIVRO DE OFERTAS (SPREAD LAY vs BACK) EM LAY OVER 2.5 HT EQUILIBRADO (N={len(sub_ht):,}):")
q_sp = sub_ht["spread_ratio"].quantile([0.10, 0.25, 0.50, 0.75, 0.90])
q_tk = sub_ht["spread_ticks"].quantile([0.10, 0.25, 0.50, 0.75, 0.90])
print(f"   - Razão Odd_Lay / Odd_Back: p10={q_sp[0.10]:.3f}x | p25={q_sp[0.25]:.3f}x | Mediana={q_sp[0.50]:.3f}x | p75={q_sp[0.75]:.3f}x | p90={q_sp[0.90]:.3f}x")
print(f"   - Gap Absoluto (Lay - Back): p10={q_tk[0.10]:.2f} | p25={q_tk[0.25]:.2f} | Mediana={q_tk[0.50]:.2f} | p75={q_tk[0.75]:.2f} | p90={q_tk[0.90]:.2f} ticks")
print(f"   - Proporção com Livro Denso (Spread <= 1.15x): {(sub_ht['spread_ratio'] <= 1.15).mean()*100:.1f}% ({int((sub_ht['spread_ratio'] <= 1.15).sum())} jogos)")
print(f"   - Proporção com Livro Moderado (1.15x < Spread <= 1.25x): {((sub_ht['spread_ratio'] > 1.15) & (sub_ht['spread_ratio'] <= 1.25)).mean()*100:.1f}%")
print(f"   - Proporção com Livro Raso / Ilíquido (Spread > 1.25x): {(sub_ht['spread_ratio'] > 1.25).mean()*100:.1f}% ({int((sub_ht['spread_ratio'] > 1.25).sum())} jogos)")

# Testar desempenho por Faixa de Liquidez (Spread) e comparar com alternativas FT de Alta Liquidez!
def _eval_liq(label, m_cond, odd_s, win_b):
    m_all = m_cond & odd_s.notna() & (odd_s > 1.02)
    n3 = int(m_all.sum())
    if n3 == 0: return None
    o = odd_s[m_all].values
    w = np.asarray(win_b)[m_all.values].astype(bool)
    wr = w.mean() * 100.0
    be5 = np.mean((o - 1.0) / (o - 0.05)) * 100.0
    pnl5 = np.sum(np.where(w, 0.95 / (o - 1.0), -1.0))
    roi5 = (pnl5 / n3) * 100.0
    roi35 = (np.sum(np.where(w, 0.965 / (o - 1.0), -1.0)) / n3) * 100.0
    
    # Ago-Set 2026
    m_as = m_all & (df_ft["Date"] >= "2026-08-01")
    nas = int(m_as.sum())
    if nas > 0:
        oas = odd_s[m_as].values
        was = np.asarray(win_b)[m_as.values].astype(bool)
        wras = was.mean() * 100.0
        pnlas5 = np.sum(np.where(was, 0.95 / (oas - 1.0), -1.0))
        roias5 = (pnlas5 / nas) * 100.0
    else:
        wras, pnlas5, roias5 = 0.0, 0.0, 0.0
        
    return {
        "Recorte / Alternativa de Liquidez": label,
        "N_Total": n3,
        "Odd_Med": round(float(np.median(o)), 2),
        "WR (%)": round(wr, 2),
        "BE_5%": round(be5, 2),
        "ROI_3Y (5%)": round(roi5, 2),
        "ROI_3Y (3.5%)": round(roi35, 2),
        "N_AgoSet26": nas,
        "WR_AgoSet26": round(wras, 2),
        "PnL_AgoSet26 (u)": round(pnlas5, 2),
        "ROI_AgoSet26 (5%)": round(roias5, 2)
    }

# Ligas Tier-1/Tier-2 de Alta Liquidez
TIER1_KEYWORDS = (
    "ENGLAND 1", "ENGLAND 2", "SPAIN 1", "SPAIN 2", "ITALY 1", "ITALY 2",
    "GERMANY 1", "GERMANY 2", "FRANCE 1", "FRANCE 2", "BRAZIL 1", "BRAZIL 2",
    "NETHERLANDS 1", "PORTUGAL 1", "BELGIUM 1", "TURKEY 1", "ARGENTINA 1",
    "SCOTLAND 1", "USA 1", "MEXICO 1", "JAPAN 1", "CHAMPIONS", "EUROPA LEAGUE", "LIBERTADORES"
)
is_tier1 = df_ft["League"].astype(str).str.upper().apply(lambda x: any(k in x for k in TIER1_KEYWORDS))

# Top-3 Menor Odd do Dia para Lay Over 2.5 HT Equilibrado (pega exatamente os 3 jogos de menor odd / maior liquidez do dia!)
rk_ht = df_ft.loc[base_eq, "Odd_Over25_HT_Lay"].groupby(df_ft.loc[base_eq, "Date"]).rank(method="first", ascending=True)
m_ht_top3 = pd.Series(False, index=df_ft.index); m_ht_top3.loc[rk_ht[rk_ht <= 3].index] = True

# Top-3 Menor Odd do Dia com Trava de Spread <= 1.18x (100% líquido)
base_eq_liq = base_eq & (spread_o25_ht <= 1.18) & (o25_ht_l <= 12.5)
rk_ht_liq = df_ft.loc[base_eq_liq, "Odd_Over25_HT_Lay"].groupby(df_ft.loc[base_eq_liq, "Date"]).rank(method="first", ascending=True)
m_ht_liq_top3 = pd.Series(False, index=df_ft.index); m_ht_liq_top3.loc[rk_ht_liq[rk_ht_liq <= 3].index] = True

liq_rows = [
    _eval_liq("1. Lay Over 2.5 HT Equilibrado — SEM FILTRO DE LIQUIDEZ (Lay 5-16)", base_eq, o25_ht_l, (gtot_ht <= 2)),
    _eval_liq("2. Lay Over 2.5 HT — SÓ LIVRO RASO / ILÍQUIDO (Spread > 1.22x)", base_eq & (spread_o25_ht > 1.22), o25_ht_l, (gtot_ht <= 2)),
    _eval_liq("3. Lay Over 2.5 HT — SÓ LIVRO DENSO (Spread <= 1.15x)", base_eq & (spread_o25_ht <= 1.15), o25_ht_l, (gtot_ht <= 2)),
    _eval_liq("4. Lay Over 2.5 HT — LIVRO DENSO + ODD BAIXA (Spread <= 1.18x & Lay 5.0-12.5)", base_eq_liq, o25_ht_l, (gtot_ht <= 2)),
    _eval_liq("5. Lay Over 2.5 HT — APENAS LIGAS TIER-1/2 DE ALTA LIQUIDEZ", base_eq & is_tier1, o25_ht_l, (gtot_ht <= 2)),
    _eval_liq("6. Lay Over 2.5 HT — TOP-3 MENOR ODD DO DIA (Com Spread <= 1.18x)", m_ht_liq_top3, o25_ht_l, (gtot_ht <= 2)),
    _eval_liq("7. [ALTERNATIVA FT LÍQUIDA] Lay Over 4.5 FT no MESMO Jogo (U25<=1.60 & Fav>=2.00 & Spread<=1.12x)", (u25_b<=1.60) & (np.minimum(oh_b, oa_b)>=2.00) & (o45_ft_l>=4.0) & (o45_ft_l<=16.0) & (spread_o45_ft<=1.12), o45_ft_l, (gtot <= 4)),
    _eval_liq("8. [ALTERNATIVA FT LÍQUIDA] Lay Over 3.5 FT no MESMO Jogo (U25<=1.55 & Fav>=2.00 & Spread<=1.08x)", (u25_b<=1.55) & (np.minimum(oh_b, oa_b)>=2.00) & (o35_ft_l>=2.8) & (o35_ft_l<=8.0) & (spread_o35_ft<=1.08), o35_ft_l, (gtot <= 3)),
    _eval_liq("9. [ALTERNATIVA HT LÍQUIDA] Lay Over 1.5 HT no MESMO Jogo (U25<=1.55 & Fav>=2.00 & Spread<=1.10x)", (u25_b<=1.55) & (np.minimum(oh_b, oa_b)>=2.00) & (o15_ht_l>=3.0) & (o15_ht_l<=8.0) & (spread_o15_ht<=1.10), o15_ht_l, (gtot_ht <= 1)),
]

df_liq_res = pd.DataFrame([r for r in liq_rows if r is not None])
print("\n2) COMPARATIVO DE PERFORMANCE POR PROFUNDIDADE DE LIVRO (SPREAD) VS ALTERNATIVAS DE ALTA LIQUIDEZ:")
print(df_liq_res.to_string(index=False))
