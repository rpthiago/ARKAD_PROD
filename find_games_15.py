import pandas as pd
df = pd.read_csv("b365_base_lean.csv", usecols=["Date", "League", "Home", "Away", "Odd_CS_2x0", "Odd_H_FT"], low_memory=False)

# Checa se Date contem '15/08/2026' ou '2026-08-15'
m1 = df["Date"].astype(str).str.contains("2026-08-15|15/08/2026")
df15 = df[m1]
print(f"Total de jogos encontrados para 15/08/2026: {len(df15)}")

if not df15.empty:
    print(df15[["League", "Home", "Away", "Odd_CS_2x0", "Odd_H_FT"]].to_string())
else:
    # Mostra as datas de Agosto 2026 que existem na base
    aug = df[df["Date"].astype(str).str.contains("2026-08|08/2026")]
    print(f"Total em Agosto 2026: {len(aug)}")
    if not aug.empty:
        print("Datas de Agosto 2026 presentes:")
        print(aug["Date"].value_counts().head(10))
