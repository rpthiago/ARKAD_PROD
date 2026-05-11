"""Analise de melhorias para o ARKAD PROD — faixas de odd, ligas, rodos"""
import json
import pandas as pd

cfg = json.loads(open('C:/Users/thiag/OneDrive/Documentos/GitHub/ARKAD_PROD/config_universo_97.json', encoding='utf-8').read())
fm = cfg['runtime_data']['filtros_metodo']
LIGAS_0x1 = {l.upper() for l in fm['Lay_CS_0x1_B365']['ligas_permitidas']}
LIGAS_1x0 = {l.upper() for l in fm['Lay_CS_1x0_B365']['ligas_permitidas']}

df = pd.read_csv('C:/Users/thiag/OneDrive/Documentos/GitHub/DASHBOARD_ARKAD-1/Bases_de_Dados_API_FutPythonTrader_Bet365.csv', low_memory=False)
df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
df2026 = df[df['Date'].dt.year == 2026].dropna(subset=['Goals_H_FT', 'Goals_A_FT']).copy()
df2026['GH'] = df2026['Goals_H_FT'].astype(int)
df2026['GA'] = df2026['Goals_A_FT'].astype(int)
df2026['Liga'] = df2026['League'].str.upper().str.strip()

def roi_lay(sub, odd_col, green_col):
    lucro = sum([(1 - 1/r) * (1 - 0.065) if gr else -1
                 for r, gr in zip(sub[odd_col], sub[green_col])])
    return lucro / len(sub) * 100 if len(sub) > 0 else 0

# ── 1. ROI por faixa de odd — 0x1 ────────────────────────────────────────────
print('=== LAY_CS_0x1_B365: ROI por faixa de odd ===')
sub0 = df2026[df2026['Liga'].isin(LIGAS_0x1)].copy()
sub0['Odd'] = pd.to_numeric(sub0['Odd_CS_0x1'], errors='coerce')
sub0 = sub0.dropna(subset=['Odd'])
sub0['Green'] = ~((sub0['GH'] == 0) & (sub0['GA'] == 1))
for lo, hi in [(8, 9), (9, 10), (10, 10.5), (10.5, 11), (11, 11.5), (11.5, 13)]:
    s = sub0[(sub0['Odd'] >= lo) & (sub0['Odd'] < hi)]
    if len(s) < 5:
        continue
    g = s['Green'].sum()
    n = len(s)
    roi = roi_lay(s, 'Odd', 'Green')
    mark = '<<< MELHOR' if roi > 5 else ('XXX PIOR' if roi < -5 else '')
    print(f'  Odd {lo:.1f}-{hi:.1f}: {n:4d} apostas | Green {g/n*100:.1f}% | ROI {roi:+.1f}% {mark}')

# ── 2. ROI por faixa de odd — 1x0 ────────────────────────────────────────────
print()
print('=== LAY_CS_1x0_B365: ROI por faixa de odd ===')
sub1 = df2026[df2026['Liga'].isin(LIGAS_1x0)].copy()
sub1['Odd'] = pd.to_numeric(sub1['Odd_CS_1x0'], errors='coerce')
sub1 = sub1.dropna(subset=['Odd'])
sub1['Green'] = ~((sub1['GH'] == 1) & (sub1['GA'] == 0))
for lo, hi in [(4.5, 6), (6, 7), (7, 8), (8, 9), (9, 10), (10, 11), (11, 11.5)]:
    s = sub1[(sub1['Odd'] >= lo) & (sub1['Odd'] < hi)]
    if len(s) < 5:
        continue
    g = s['Green'].sum()
    n = len(s)
    roi = roi_lay(s, 'Odd', 'Green')
    mark = '<<< MELHOR' if roi > 5 else ('XXX PIOR' if roi < -5 else '')
    print(f'  Odd {lo:.1f}-{hi:.1f}: {n:4d} apostas | Green {g/n*100:.1f}% | ROI {roi:+.1f}% {mark}')

# ── 3. Ligas fora da whitelist com ROI positivo ───────────────────────────────
print()
print('=== LIGAS FORA DA WHITELIST 0x1 com ROI > 5% (candidatas a ADICIONAR) ===')
todas_ligas = df2026['Liga'].unique()
ligas_fora = [l for l in todas_ligas if l not in LIGAS_0x1 and l not in LIGAS_1x0]
candidatas = []
for liga in ligas_fora:
    s = df2026[df2026['Liga'] == liga].copy()
    s['Odd'] = pd.to_numeric(s['Odd_CS_0x1'], errors='coerce')
    s = s.dropna(subset=['Odd'])
    s = s[(s['Odd'] >= 8) & (s['Odd'] <= 11.5)]
    if len(s) < 10:
        continue
    s['Green'] = ~((s['GH'] == 0) & (s['GA'] == 1))
    g = s['Green'].sum()
    n = len(s)
    roi = roi_lay(s, 'Odd', 'Green')
    if roi > 5:
        candidatas.append({'Liga': liga, 'N': n, 'Green_pct': round(g/n*100, 1), 'ROI_pct': round(roi, 1)})

for c in sorted(candidatas, key=lambda x: -x['ROI_pct'])[:10]:
    print(f"  {c['Liga']:<35} N={c['N']:3d} | Green {c['Green_pct']}% | ROI {c['ROI_pct']:+.1f}%")

# ── 4. Ligas na whitelist com ROI negativo ────────────────────────────────────
print()
print('=== LIGAS NA WHITELIST 0x1 com ROI < -5% (candidatas a RODO) ===')
for liga in sorted(LIGAS_0x1):
    s = df2026[df2026['Liga'] == liga].copy()
    s['Odd'] = pd.to_numeric(s['Odd_CS_0x1'], errors='coerce')
    s = s.dropna(subset=['Odd'])
    s = s[(s['Odd'] >= 8) & (s['Odd'] <= 11.5)]
    if len(s) < 5:
        continue
    s['Green'] = ~((s['GH'] == 0) & (s['GA'] == 1))
    g = s['Green'].sum()
    n = len(s)
    roi = roi_lay(s, 'Odd', 'Green')
    if roi < -5:
        print(f'  XX {liga:<35} N={n:3d} | Green {g/n*100:.1f}% | ROI {roi:+.1f}%')

# ── 5. Ligas 1x0 com ROI negativo ────────────────────────────────────────────
print()
print('=== LIGAS 1x0 (SPAIN/ITALY) por liga e faixa de odd ===')
for liga in sorted(LIGAS_1x0):
    for lo, hi in [(4.5, 7), (7, 9), (9, 11.5)]:
        s = df2026[df2026['Liga'] == liga].copy()
        s['Odd'] = pd.to_numeric(s['Odd_CS_1x0'], errors='coerce')
        s = s.dropna(subset=['Odd'])
        s = s[(s['Odd'] >= lo) & (s['Odd'] < hi)]
        if len(s) < 5:
            continue
        s['Green'] = ~((s['GH'] == 1) & (s['GA'] == 0))
        g = s['Green'].sum()
        n = len(s)
        roi = roi_lay(s, 'Odd', 'Green')
        mark = '<<< OK' if roi > 0 else 'XXX RODO'
        print(f'  {liga:<12} Odd {lo:.1f}-{hi:.1f}: {n:3d} apostas | Green {g/n*100:.1f}% | ROI {roi:+.1f}% {mark}')

print()
print('=== RESUMO DAS RECOMENDACOES ===')
