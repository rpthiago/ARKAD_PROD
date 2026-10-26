"""Auditoria do metodo ML Lay Under 0.5 FT (Lay 0x0) — outubro/2026.
Testes: (1) ROI por liability vs nominal, (2) modelo vs mercado puro,
(3) espelho Back Over 0.5 (mesmo evento), (4) bootstrap de blocos-dia,
(5) consistencia do livro.
"""
import sys, unicodedata, re, random, collections
import pandas as pd, numpy as np

sys.stdout.reconfigure(encoding='utf-8')
C = 0.05

def canon(s):
    s = unicodedata.normalize('NFKD', str(s)).encode('ascii', 'ignore').decode().lower()
    return re.sub(r'[^a-z0-9]', '', s)

base = r"C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD"
dp = pd.read_csv(base + r"\metodos_aprovados\placares_ft.csv")
dp['ch'] = dp['home'].apply(canon); dp['ca'] = dp['away'].apply(canon)
dp['ds'] = pd.to_datetime(dp['ko'], errors='coerce').dt.strftime('%Y-%m-%d')
sd = {}
for _, r in dp.iterrows():
    if pd.notnull(r['gh']) and pd.notnull(r['ga']):
        sd[f"{r['ds']}_{r['ch']}_{r['ca']}"] = (int(r['gh']), int(r['ga']))

d = pd.read_csv(base + r"\scratch\october_pregame_odds.csv")
d['ch'] = d['home'].apply(canon); d['ca'] = d['away'].apply(canon)
g = [sd.get(f"{r['ko_date']}_{r['ch']}_{r['ca']}", (np.nan, np.nan)) for _, r in d.iterrows()]
d['gh'] = [x[0] for x in g]; d['ga'] = [x[1] for x in g]
req = ['Odd_H_Back','Odd_A_Back','Odd_D_Back','Odd_Under05_FT_Lay',
       'Odd_Over05_FT_Back','Odd_Over15_FT_Back','Odd_Over25_FT_Back','Odd_BTTS_Yes_Back']
for c in req: d[c] = pd.to_numeric(d[c], errors='coerce')
v = d.dropna(subset=['gh','ga'] + req).copy()
v['is0'] = ((v['gh'] == 0) & (v['ga'] == 0)).astype(int)
print(f"universo outubro com odds completas + placar: {len(v)}  |  taxa 0-0 = {100*v['is0'].mean():.2f}%")

def be_lay(o): return (o - 1) / (o - C)

def boot(pordia, n=4000, seed=11):
    dias = list(pordia)
    if len(dias) < 2: return float('nan'), float('nan')
    rnd = random.Random(seed); out = []
    for _ in range(n):
        s = 0.0; k = 0
        for _ in range(len(dias)):
            for x in pordia[rnd.choice(dias)]: s += x; k += 1
        if k: out.append(100 * s / k)
    out.sort(); return out[int(.025*len(out))], out[int(.975*len(out))]

def cel(sub, rot, lado='lay'):
    n = len(sub)
    if n == 0: print(f"  {rot:<46} N=0"); return
    if lado == 'lay':
        o = sub['Odd_Under05_FT_Lay'].values; red = sub['is0'].values == 1
        pl_liab = np.where(red, -1.0, (1-C)/(o-1))     # por liability
        pl_nom  = np.where(red, -(o-1.0), 1-C)          # por stake nominal
        bem = be_lay(o).mean(); wr = (~red).mean()
    else:  # back over 0.5
        o = sub['Odd_Over05_FT_Back'].values; win = sub['is0'].values == 0
        pl_liab = np.where(win, (o-1)*(1-C), -1.0)      # back: stake 1u = risco 1u
        pl_nom  = pl_liab
        bem = (1.0/o).mean(); wr = win.mean()
    pordia = collections.defaultdict(list)
    for dia, x in zip(sub['ko_date'].values, pl_liab): pordia[dia].append(x)
    lo, hi = boot(pordia)
    reds = int((~(sub['is0'].values == 0)).sum()) if lado=='lay' else int((sub['is0'].values==1).sum())
    flag = '  <5 reds' if reds < 5 else ''
    print(f"  {rot:<46} N={n:<5} WR={100*wr:5.2f}% BE={100*bem:5.2f}% gap={100*(wr-bem):+6.2f}pp "
          f"ROIliab={100*pl_liab.mean():+6.2f}% IC95=[{lo:+6.2f},{hi:+6.2f}] ROInom={100*pl_nom.mean():+7.2f}% reds={reds}{flag}")

# ---- probabilidades de mercado + EV sem modelo ----
v['p_imp_lay'] = 0.95 / np.maximum(1.01, v['Odd_Under05_FT_Lay'] - 0.05)
v['fav'] = np.minimum(v['Odd_H_Back'], v['Odd_A_Back'])

print("\n" + "="*132)
print("1. O EFEITO EXISTE SEM MODELO NENHUM? (so teto de odd — nenhuma selecao de ML)")
print("="*132)
for teto in (10.0, 12.0, 14.0, 16.5, 25.0, 1000.0):
    cel(v[(v['Odd_Under05_FT_Lay'] >= 2.0) & (v['Odd_Under05_FT_Lay'] <= teto)], f"LAY 0x0, teto <= {teto}, sem modelo")

print("\n" + "="*132)
print("2. ESPELHO: BACK OVER 0.5 — MESMO EVENTO, mesmo universo, preco real de back")
print("   (se os DOIS lados mostram edge, o preco nao e executavel: o overround some)")
print("="*132)
for teto in (10.0, 12.0, 16.5, 1000.0):
    s = v[(v['Odd_Under05_FT_Lay'] >= 2.0) & (v['Odd_Under05_FT_Lay'] <= teto)]
    cel(s, f"BACK Over 0.5, mesmo subconjunto teto<={teto}", lado='back')

print("\n" + "="*132)
print("3. CONSISTENCIA DO LIVRO — o lay de Under 0.5 e o back de Over 0.5 concordam?")
print("="*132)
s = v[(v['Odd_Under05_FT_Lay'] >= 2.0) & (v['Odd_Under05_FT_Lay'] <= 16.5)].copy()
p_lay_u = 1.0 / s['Odd_Under05_FT_Lay']          # implicito cru do lay de under
p_back_o = 1.0 / s['Odd_Over05_FT_Back']
soma = p_lay_u + p_back_o
print(f"  N={len(s)}")
print(f"  P(0-0) implicito no LAY de Under 0.5 : mediana {p_lay_u.median():.4f}")
print(f"  P(gol)  implicito no BACK de Over 0.5: mediana {p_back_o.median():.4f}")
print(f"  soma das duas (deveria ser ~1.00 + overround): mediana {soma.median():.4f}")
print(f"  soma < 1.00 (livro inconsistente / arbitragem aparente): {100*(soma<1.0).mean():.1f}% dos jogos")
print(f"  taxa real de 0-0 neste subconjunto: {100*s['is0'].mean():.2f}%")

print("\n" + "="*132)
print("4. O FILTRO QUE REALMENTE CARREGA O RESULTADO: Fav <= 1.80 (puro mercado, sem ML)")
print("="*132)
for teto in (12.0, 16.5):
    s2 = v[(v['Odd_Under05_FT_Lay'] >= 2.0) & (v['Odd_Under05_FT_Lay'] <= teto)]
    cel(s2[s2['fav'] <= 1.80], f"teto<={teto} + Fav<=1.80", 'lay')
    cel(s2[s2['fav'] > 1.80], f"teto<={teto} + Fav> 1.80", 'lay')

print("\n" + "="*132)
print("5. LIVRO SUSPEITO — o edge sobrevive se eu tirar os jogos de livro obviamente vazio?")
print("="*132)
v['vig'] = 1/v['Odd_H_Back'] + 1/v['Odd_D_Back'] + 1/v['Odd_A_Back']
s3 = v[(v['Odd_Under05_FT_Lay'] >= 2.0) & (v['Odd_Under05_FT_Lay'] <= 16.5)]
cel(s3, "teto<=16.5 — tudo", 'lay')
cel(s3[(s3['Odd_D_Back'] <= 6.0)], "  + empate back <= 6.00 (livro plausivel)", 'lay')
cel(s3[(s3['Odd_D_Back'] <= 6.0) & (s3['vig'].between(0.98, 1.15))], "  + overround 1X2 entre 0.98 e 1.15", 'lay')
cel(s3[(s3['Odd_D_Back'] <= 6.0) & (s3['vig'].between(0.98, 1.15)) & (s3['fav'] >= 1.20)], "  + fav >= 1.20 (exclui jogo decidido)", 'lay')
