import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np

# Lista exata do usuario
raw_data = [
    ("2026-08-15", "05:00", "ROMANIA 2", "Politehnica Timisoara", "Unirea Slobozia", 7.6, "89.6%"),
    ("2026-08-15", "08:30", "ENGLAND 3", "Oxford Utd", "MK Dons", 9.0, "90.4%"),
    ("2026-08-15", "11:00", "ENGLAND 5", "Barrow", "Hornchurch", 8.4, "89.4%"),
    ("2026-08-15", "11:00", "ENGLAND 2", "Middlesbrough", "Lincoln", 8.4, "90.0%"),
    ("2026-08-15", "11:00", "ENGLAND 2", "Norwich", "West Brom", 8.6, "90.0%"),
    ("2026-08-15", "11:00", "ENGLAND 2", "Portsmouth", "QPR", 9.0, "90.2%"),
    ("2026-08-15", "11:00", "ENGLAND 3", "Cambridge Utd", "Wigan", 8.6, "89.6%"),
    ("2026-08-15", "11:00", "ENGLAND 4", "Grimsby", "Exeter", 9.2, "91.3%"),
    ("2026-08-15", "11:00", "ENGLAND 4", "Tranmere", "Shrewsbury", 9.4, "91.1%"),
    ("2026-08-15", "14:00", "PORTUGAL 2", "Torreense", "Penafiel", 8.2, "90.0%"),
    ("2026-08-15", "14:30", "ARGENTINA 1", "Aldosivi", "Tigre", 8.2, "89.4%"),
    ("2026-08-15", "14:30", "SPAIN 1", "Alaves", "Getafe", 6.6, "86.8%"),
    ("2026-08-15", "15:00", "CZECH 1", "Plzen", "Zlin", 8.8, "90.0%"),
    ("2026-08-15", "15:15", "POLAND 1", "Widzew Lodz", "Korona Kielce", 8.8, "89.6%"),
    ("2026-08-15", "16:00", "BRAZIL 2", "Criciuma", "Goias", 6.0, "85.1%"),
    ("2026-08-15", "16:00", "PARAGUAY 1", "Guarani (Par)", "Rubio Nu", 9.0, "90.5%"),
    ("2026-08-15", "16:30", "SPAIN 1", "Sevilla", "Rayo Vallecano", 8.4, "89.8%"),
    ("2026-08-15", "17:00", "BRAZIL 3", "Volta Redonda", "Ituano", 8.4, "89.8%"),
    ("2026-08-15", "18:30", "ECUADOR 1", "Guayaquil City", "Libertad FC", 9.2, "90.4%"),
    ("2026-08-15", "19:00", "ARGENTINA 2", "Quilmes", "San Telmo", 6.2, "86.2%"),
    ("2026-08-15", "21:00", "BRAZIL 1", "Sao Paulo", "Coritiba", 7.6, "89.4%")
]

df_user = pd.DataFrame(raw_data, columns=["Date", "Time", "League", "Home", "Away", "Odd_Lay_1x0", "Prob_ML"])

# Vamos consultar a base e fontes de resultados para auditar cada um dos 21 jogos
print("=== AUDITORIA COMPLETA DOS 21 JOGOS DO LAY 1X0 EM 15/08/2026 ===", flush=True)
print(f"Total de jogos informados: {len(df_user)}\n", flush=True)
