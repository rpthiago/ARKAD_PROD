import pandas as pd
import json

# 1. Carregar Config
with open("config_prod_v1.json", "r", encoding="utf-8") as f:
    config = json.load(f)

filtros = config["runtime_data"]["filtros_metodo"]

# 2. Carregar Histórico
hist = pd.read_csv("recalculo_sem_combos_usuario.csv")
hist["Data_Arquivo"] = pd.to_datetime(hist["Data_Arquivo"], errors="coerce")

# Filtrar apenas o ano de 2025
df_ano = hist[hist["Data_Arquivo"].dt.year == 2025].copy()
df_ano["Metodo"] = df_ano["Metodo"].str.strip()

print(f"Total de jogos brutos em 2025 no histórico: {len(df_ano)}")

# 3. Função de Validação
def passes_filters(row):
    m = row.get("Metodo", "")
    flt = filtros.get(m)
    if not flt:
        return False
    
    odd = pd.to_numeric(row.get("Odd_Base"), errors="coerce")
    if pd.isna(odd):
        return False
    
    omn, omx = flt.get("odd_min"), flt.get("odd_max")
    if omn is not None and odd < float(omn):
        return False
    if omx is not None and odd > float(omx):
        return False
    
    ligas_permitidas = flt.get("ligas_permitidas")
    if ligas_permitidas is not None:
        if str(row.get("Liga", "")).strip().upper() not in [lg.upper() for lg in ligas_permitidas]:
            return False
            
    return True

# Aplicar o filtro do JSON
df_ano_filt = df_ano[df_ano.apply(passes_filters, axis=1)].copy()

# Regra dupla para o 1x0
jogos_com_0x1 = df_ano_filt[df_ano_filt["Metodo"] == "Lay_CS_0x1_B365"].groupby(df_ano_filt["Data_Arquivo"].dt.date)["Jogo"].apply(
    lambda x: set(x.str.strip())
)

def confirmado(row):
    if row["Metodo"] != "Lay_CS_1x0_B365":
        return True
    jogos_ok = jogos_com_0x1.get(row["Data_Arquivo"].date(), set())
    return str(row["Jogo"]).strip() in jogos_ok

df_ano_final = df_ano_filt[df_ano_filt.apply(confirmado, axis=1)].copy()

# 4. Resultados
print("=" * 60)
print("BACKTEST 2026 (JAN a ABR) - COM NOVA CONFIGURAÇÃO")
print("=" * 60)

n_total = len(df_ano_final)
if n_total > 0:
    g_total = (df_ano_final["Resultado_1_0"] == 1.0).sum()
    r_total = (df_ano_final["Resultado_1_0"] == 0.0).sum()
    wr_total = g_total / (g_total + r_total) * 100 if (g_total + r_total) > 0 else 0
    
    print(f"Total Apostas: {n_total} | Greens: {g_total} | Reds: {r_total} | WR: {wr_total:.1f}%")
    print("-" * 60)
    
    for m in ["Lay_CS_0x1_B365", "Lay_CS_1x0_B365"]:
        sub = df_ano_final[df_ano_final["Metodo"] == m]
        if len(sub) == 0: continue
        g = (sub["Resultado_1_0"] == 1.0).sum()
        r = (sub["Resultado_1_0"] == 0.0).sum()
        wr = g / (g + r) * 100 if (g + r) > 0 else 0
        
        # Simulando P&L
        # Stake flat de R$ 500, comissao 6.5%
        pnl = 0
        for _, row in sub.iterrows():
            if row["Resultado_1_0"] == 1.0:
                odd = row["Odd_Exec"] if not pd.isna(row.get("Odd_Exec")) else row["Odd_Base"]
                lucro = (500 / (odd - 1)) * 0.935
                pnl += lucro
            elif row["Resultado_1_0"] == 0.0:
                pnl -= 500
                
        print(f"  {m}: {len(sub):3d} ap | WR: {wr:5.1f}% | P&L Simulado: R$ {pnl:7.2f}")
else:
    print("Nenhuma aposta passou nos filtros para o ano de 2026 no histórico.")
