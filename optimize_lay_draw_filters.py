import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np

print("=== TESTANDO FILTROS DE ELITE PARA OTIMIZAR O LAY DRAW ===", flush=True)

df_raw = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Bet365.csv", low_memory=False)
df_raw["d_str"] = pd.to_datetime(df_raw["Date"], errors='coerce').dt.strftime("%Y-%m-%d")

def get_num(df, cols):
    for c in cols:
        if c in df.columns:
            s = pd.to_numeric(df[c], errors='coerce')
            if s.notna().sum() > 0:
                return s
    return pd.Series(np.nan, index=df.index)

df = df_raw.copy()
df["gh"] = get_num(df, ["Goals_H_FT", "Home_Score", "Goals_H"])
df["ga"] = get_num(df, ["Goals_A_FT", "Away_Score", "Goals_A"])
df["odd_h"] = get_num(df, ["Odd_H_FT", "Odd_H_FT_Back", "Odd_H", "Odd_H_Back"])
df["odd_d"] = get_num(df, ["Odd_D_FT", "Odd_D_FT_Back", "Odd_D", "Odd_D_Back", "Odd_D_Lay"])
df["odd_a"] = get_num(df, ["Odd_A_FT", "Odd_A_FT_Back", "Odd_A", "Odd_A_Back"])

df = df[df["gh"].notna() & df["ga"].notna() & (df["odd_d"] >= 3.0) & (df["odd_d"] <= 5.50)].copy()
df["is_draw"] = (df["gh"] == df["ga"]).astype(int)

# Preparar conjuntos
df_2026 = df[(df["d_str"] >= "2026-01-01") & (df["d_str"] <= "2026-08-20")].copy()
df_2025 = df[(df["d_str"] >= "2025-01-01") & (df["d_str"] <= "2025-12-31")].copy()
df_aug = df[(df["d_str"] >= "2026-08-01") & (df["d_str"] <= "2026-08-20")].copy()

def test_filter(df_in, name, min_odd, max_odd, fav_max_odd=None):
    sub = df_in[(df_in["odd_d"] >= min_odd) & (df_in["odd_d"] <= max_odd)].copy()
    if fav_max_odd is not None:
        # Pelo menos um time deve ser favorito com odd <= fav_max_odd
        sub = sub[(sub["odd_h"] <= fav_max_odd) | (sub["odd_a"] <= fav_max_odd)].copy()
        
    tot = len(sub)
    if tot == 0: return {}
    grn = (sub["is_draw"] == 0).sum()
    red = (sub["is_draw"] == 1).sum()
    wr = (grn / tot) * 100.0
    
    pnl = np.where(sub["is_draw"] == 0, 95.0, -(sub["odd_d"] - 1.0) * 100.0).sum()
    lucro = np.where(sub["is_draw"] == 0, 95.0, 0).sum()
    perda = abs(np.where(sub["is_draw"] == 1, -(sub["odd_d"] - 1.0) * 100.0, 0).sum())
    pf = (lucro / perda) if perda > 0 else np.nan
    
    # Media de jogos por dia
    n_dias = sub["d_str"].nunique()
    jogos_dia = tot / n_dias if n_dias > 0 else 0
    
    return {
        "Filtro": name,
        "Entradas": tot,
        "Média Jogos/Dia": f"{jogos_dia:.1f}",
        "Win Rate %": f"{wr:.2f}%",
        "Lucro Líquido R$": f"R$ {pnl:,.2f}",
        "Profit Factor": f"{pf:.2f}"
    }

filtros = [
    ("Base Atual (Odds 3.00 a 5.50 - Sem Filtro de Favorito)", 3.0, 5.50, None),
    ("Filtro 1: Odds 3.20 a 4.80 (Corta Extremos de Risco)", 3.20, 4.80, None),
    ("Filtro 2: Favorito Claro (Odd Mandante ou Visitante <= 2.20)", 3.0, 5.50, 2.20),
    ("Filtro 3: Super Favorito (Odd Mandante ou Visitante <= 1.95)", 3.0, 5.50, 1.95),
    ("Filtro Elite ARKAD: Odds 3.20-4.80 + Favorito <= 2.10", 3.20, 4.80, 2.10),
]

print("\n" + "="*95, flush=True)
print("📊 1. DESEMPENHO NO ANO DE 2026 COMPLETO (JANEIRO A AGOSTO)", flush=True)
print("="*95, flush=True)
rows_26 = [test_filter(df_2026, name, o_min, o_max, fav) for name, o_min, o_max, fav in filtros]
print(pd.DataFrame(rows_26).to_string(index=False), flush=True)

print("\n" + "="*95, flush=True)
print("📊 2. DESEMPENHO NO ANO DE 2025 COMPLETO (VALIDAÇÃO OUT-OF-SAMPLE)", flush=True)
print("="*95, flush=True)
rows_25 = [test_filter(df_2025, name, o_min, o_max, fav) for name, o_min, o_max, fav in filtros]
print(pd.DataFrame(rows_25).to_string(index=False), flush=True)

print("\n" + "="*95, flush=True)
print("📊 3. DESEMPENHO EM AGOSTO DE 2026 (01 A 20/08)", flush=True)
print("="*95, flush=True)
rows_aug = [test_filter(df_aug, name, o_min, o_max, fav) for name, o_min, o_max, fav in filtros]
print(pd.DataFrame(rows_aug).to_string(index=False), flush=True)
