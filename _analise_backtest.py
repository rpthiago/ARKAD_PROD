import pandas as pd, pathlib, json

# ── 1. Config de producao: modo rodo ──────────────────────────────────────────
cfg = json.loads(pathlib.Path('config_prod_v1.json').read_text(encoding='utf-8'))
rd = cfg.get('runtime_data', cfg)
print('=== RODO MODE em config_prod_v1.json ===')
print('rodo_mode:', rd.get('rodo_mode', 'NAO ENCONTRADO'))
print('Total regras:', len(cfg.get('filtros_rodo', [])))
print()

# ── 2. Backtest historico ──────────────────────────────────────────────────────
df = pd.read_csv(
    'Arquivados_Apostas_Diarias/Relatorios/Comparativo_Juros_vs_Progressiva/progressiva_fixa_hist_20260417_ops.csv'
)
df['Data'] = pd.to_datetime(df['Data_Arquivo'], errors='coerce').dt.date
apd = df.groupby('Data').size()
print('=== BACKTEST HISTORICO +R$50k ===')
print(f'Periodo: {df["Data"].min()} a {df["Data"].max()}')
print(f'Total dias: {df["Data"].nunique()}')
print(f'Total apostas: {len(df)}')
print(f'Media/dia: {apd.mean():.1f}  Mediana: {apd.median():.0f}  Max: {apd.max()}  Min: {apd.min()}')
print()
print('Metodos:')
print(df['Metodo'].value_counts().to_string())
print()

# PnL por metodo (usando summary se disponivel)
summ = json.loads(
    pathlib.Path('Arquivados_Apostas_Diarias/Relatorios/Comparativo_Juros_vs_Progressiva/progressiva_fixa_hist_20260417_summary.json')
    .read_text(encoding='utf-8')
)
print('=== SUMMARY DO BACKTEST ===')
print(json.dumps(summ, indent=2, ensure_ascii=False))
