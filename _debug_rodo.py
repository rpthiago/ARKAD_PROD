import json, warnings, pandas as pd
warnings.filterwarnings('ignore')
from pathlib import Path
from main import _load_master_rodo_cuts, _matches_cut
from ingestao_tempo_real import load_live_dataframe

cfg = json.loads(Path('config_universo_97.json').read_text())
cuts, label, err = _load_master_rodo_cuts(cfg)
print(f'Rodos no master: {len(cuts)}')
print()

df_live, _ = load_live_dataframe('2026-04-29', cfg)
filtros = cfg['runtime_data']['filtros_metodo']

def passes(row):
    m = str(row.get('Metodo',''))
    flt = filtros.get(m)
    if not flt: return False, 'sem_metodo'
    odd = pd.to_numeric(row.get('Odd_Base'), errors='coerce')
    if pd.isna(odd): return False, 'odd_na'
    if not (flt['odd_min'] <= odd <= flt['odd_max']): return False, f'odd_fora {odd}'
    if str(row.get('Liga','')).strip() not in flt.get('ligas_permitidas',[]): return False, 'liga_fora'
    return True, 'ok'

print("Jogos que passam no filtro de odd/liga:")
for _, r in df_live.iterrows():
    ok, mot = passes(r)
    if ok:
        matched = [c['name'] for c in cuts if _matches_cut(r, c, 'Liga','Metodo','Odd_Base')]
        odd_bf = r.get('Odd_Betfair', '')
        print(f"  {r['Liga']} | {r['Jogo']} | Odd_B365={r['Odd_Base']} | Odd_BF={odd_bf}")
        if matched:
            for m in matched:
                print(f"    BLOQUEADO PELO RODO: {m}")
        else:
            print(f"    -> sem rodo → EXECUTED")
