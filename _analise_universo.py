import pandas as pd, pathlib

df = pd.read_csv('recalculo_sem_combos_usuario.csv')
print('=== UNIVERSO BRUTO DO BACKTEST ===')
print('Total linhas:', len(df))
print('Colunas:', list(df.columns))

data_col = next((c for c in df.columns if 'data' in c.lower()), df.columns[0])
df['__data'] = pd.to_datetime(df[data_col], errors='coerce').dt.date
apd = df.groupby('__data').size()
print('Periodo:', df['__data'].min(), 'a', df['__data'].max())
print('Total dias:', df['__data'].nunique())
print('Media/dia:', round(apd.mean(), 1), ' Max:', apd.max())
print()

mc = next((c for c in df.columns if 'metodo' in c.lower()), None)
if mc:
    print('Metodos:')
    print(df[mc].value_counts().to_string())
