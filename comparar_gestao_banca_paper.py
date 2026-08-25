import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np

print("==================================================================", flush=True)
print("   COMPARAÇÃO MATEMÁTICA: STAKE FIXA vs RESPONSABILIDADE FIXA     ", flush=True)
print("                  (TAXA BETFAIR: 4.5%)                           ", flush=True)
print("==================================================================", flush=True)

excel_path = "lay2x2/paper_resultados.xlsx"
df = pd.read_excel(excel_path)
print(f"[+] Planilha carregada: {len(df)} linhas", flush=True)
print(f"[+] Colunas disponíveis: {df.columns.tolist()}", flush=True)

# Filtrar apenas liquidadas
liq = df[df["Resultado"].isin(["GREEN", "RED"])].copy()
liq["Odd"] = pd.to_numeric(liq["Odd"], errors="coerce")
liq = liq.dropna(subset=["Odd", "Resultado"])
print(f"[+] Total de apostas liquidadas: {len(liq)}\n", flush=True)

TAXA = 0.045 # 4.5%

# 1. MODELO STAKE FIXA (R$ 100 de stake / ganho alvo)
STAKE_VAL = 100.0
# Green: Ganha Stake * (1 - Taxa)
# Red: Perde Stake * (Odd - 1)
liq["pnl_stake_fixa"] = np.where(
    liq["Resultado"] == "GREEN",
    STAKE_VAL * (1.0 - TAXA),
    -STAKE_VAL * (liq["Odd"] - 1.0)
)
liq["resp_stake_fixa"] = STAKE_VAL * (liq["Odd"] - 1.0)

# 2. MODELO RESPONSABILIDADE FIXA (R$ 100 arriscados no máximo)
RESP_VAL = 100.0
# Stake = Resp / (Odd - 1)
# Green: Ganha (Resp / (Odd - 1)) * (1 - Taxa)
# Red: Perde exatamente Resp (-R$ 100)
liq["stake_resp_fixa"] = RESP_VAL / (liq["Odd"] - 1.0)
liq["pnl_resp_fixa"] = np.where(
    liq["Resultado"] == "GREEN",
    liq["stake_resp_fixa"] * (1.0 - TAXA),
    -RESP_VAL
)

print("==================================================================")
print("           COMPARATIVO POR MÉTODO (TAXA 4.5% BETFAIR)            ")
print("==================================================================")

resumos = []
for m, g in liq.groupby("Metodo"):
    n = len(g)
    gr = (g["Resultado"] == "GREEN").sum()
    rd = n - gr
    wr = (gr / n) * 100.0
    odd_med = g["Odd"].median()
    be_wr = ((odd_med - 1.0) / (odd_med - TAXA)) * 100.0
    
    # 1. Stake Fixa
    pnl_sf = g["pnl_stake_fixa"].sum()
    stk_tot_sf = n * STAKE_VAL
    roi_sf = (pnl_sf / stk_tot_sf) * 100.0
    # Max DD Stake Fixa
    dd_sf = (g["pnl_stake_fixa"].cumsum() - g["pnl_stake_fixa"].cumsum().cummax()).min()
    
    # 2. Responsabilidade Fixa
    pnl_rf = g["pnl_resp_fixa"].sum()
    resp_tot_rf = n * RESP_VAL
    roi_rf = (pnl_rf / resp_tot_rf) * 100.0
    # Max DD Responsabilidade Fixa
    dd_rf = (g["pnl_resp_fixa"].cumsum() - g["pnl_resp_fixa"].cumsum().cummax()).min()
    
    resumos.append({
        "Método": m,
        "N": n,
        "Greens": gr,
        "Reds": rd,
        "WR (%)": round(wr, 1),
        "Odd Méd": round(odd_med, 2),
        "BE WR (%)": round(be_wr, 1),
        "Margem (%)": round(wr - be_wr, 1),
        "PnL Stake Fixa (R$)": round(pnl_sf, 2),
        "Max DD Stake Fixa": round(dd_sf, 2),
        "PnL Resp Fixa (R$)": round(pnl_rf, 2),
        "Max DD Resp Fixa": round(dd_rf, 2),
    })

tab = pd.DataFrame(resumos)
print(tab.to_string(index=False))

# Totais Gerais
print("\n==================================================================")
print("                     TOTAIS CONSOLIDADOS                          ")
print("==================================================================")
tot_sf = liq["pnl_stake_fixa"].sum()
tot_rf = liq["pnl_resp_fixa"].sum()
dd_tot_sf = (liq["pnl_stake_fixa"].cumsum() - liq["pnl_stake_fixa"].cumsum().cummax()).min()
dd_tot_rf = (liq["pnl_resp_fixa"].cumsum() - liq["pnl_resp_fixa"].cumsum().cummax()).min()

print(f"💰 P&L TOTAL COM STAKE FIXA (R$ 100/jogo):           R$ {tot_sf:,.2f} | Max Drawdown: R$ {dd_tot_sf:,.2f}")
print(f"🛡️ P&L TOTAL COM RESPONSABILIDADE FIXA (R$ 100/jogo): R$ {tot_rf:,.2f} | Max Drawdown: R$ {dd_tot_rf:,.2f}")
