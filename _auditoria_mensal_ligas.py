import pandas as pd
import datetime

# Script de Auditoria Mensal para Manutenção da Whitelist Ouro
# Objetivo: Identificar ligas que falharam nos últimos 30 dias para remoção preventiva.

FILENAME = "recalculo_sem_combos_usuario.csv" # Ou Resultados_2026_Full.csv

def auditoria():
    print("="*60)
    print("AUDITORIA MENSAL DE PERFORMANCE - WHITELIST ARKAD")
    print("="*60)
    
    try:
        df = pd.read_csv(FILENAME)
    except Exception as e:
        print(f"Erro ao carregar {FILENAME}: {e}")
        return

    df["Data_Arquivo"] = pd.to_datetime(df["Data_Arquivo"])
    
    # Período: Últimos 30 dias
    hoje = pd.Timestamp.now()
    inicio = hoje - pd.Timedelta(days=30)
    
    recent = df[df["Data_Arquivo"] >= inicio].copy()
    
    if recent.empty:
        print("Nenhum dado encontrado nos últimos 30 dias.")
        return

    print(f"Analisando {len(recent)} entradas desde {inicio.strftime('%d/%m/%Y')}...")
    
    # Agrupar por Liga e Resultado
    # Consideramos Resultado_1_0: 1.0 = Green, 0.0 = Red
    resumo = recent.groupby("Liga").agg(
        Total=("Resultado_1_0", "count"),
        Reds=("Resultado_1_0", lambda x: (x == 0.0).sum()),
        WinRate=("Resultado_1_0", lambda x: (x == 1.0).sum() / len(x) * 100)
    ).reset_index()
    
    toxic = resumo[resumo["Reds"] > 0].sort_values("Reds", ascending=False)
    
    if toxic.empty:
        print("\n✅ EXCELENTE: Nenhuma liga da Whitelist apresentou RED nos últimos 30 dias.")
    else:
        print("\n⚠️ ALERTA: As seguintes ligas apresentaram instabilidade (RED):")
        for _, row in toxic.iterrows():
            print(f" - {row['Liga']:25} | Reds: {int(row['Reds'])} | WR: {row['WinRate']:.1f}%")
            
        print("\n💡 RECOMENDAÇÃO: Considere remover essas ligas do config_prod_v1.json (ligas_permitidas)")
        print("para manter o Win Rate acima de 97%.")

if __name__ == "__main__":
    auditoria()
