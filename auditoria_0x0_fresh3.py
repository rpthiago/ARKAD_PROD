"""A regra de outubro (lay Under 0.5, teto 16.5, Fav<=1.80) vale na base independente?
Base: FRESH3 (API FutPythonTrader). So 2026. ROI por liability. Bootstrap de blocos-dia.
Inclui o ESPELHO VERDADEIRO: back Under 0.5 (lado oposto do mesmo mercado).
"""
import sys, random, collections, math
import pandas as pd, numpy as np
sys.stdout.reconfigure(encoding='utf-8')
C = 0.05
B = r"C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD"

d = pd.read_csv(B + r"\Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH3.csv",
                encoding='utf-8-sig', low_memory=False)
for c in ['Odd_H_Back','Odd_A_Back','Odd_D_Back','Odd_Under05_FT_Lay','Odd_Under05_FT_Back',
          'Goals_H_FT','Goals_A_FT']:
    d[c] = pd.to_numeric(d[c], errors='coerce')
d['dt'] = pd.to_datetime(d['Date'], errors='coerce')
d = d.dropna(subset=['dt','Goals_H_FT','Goals_A_FT','Odd_H_Back','Odd_A_Back','Odd_Under05_FT_Lay'])
d['ano'] = d['dt'].dt.year
d['mes'] = d['dt'].dt.to_period('M').astype(str)
d['dia'] = d['dt'].dt.strftime('%Y-%m-%d')
d['is0'] = ((d['Goals_H_FT']==0)&(d['Goals_A_FT']==0)).astype(int)
d['fav'] = np.minimum(d['Odd_H_Back'], d['Odd_A_Back'])
d26 = d[d['ano']==2026].copy()
print(f"FRESH3: {len(d)} jogos | 2026: {len(d26)} | taxa 0-0 em 2026 = {100*d26['is0'].mean():.2f}%")

def be_lay(o): return (o-1)/(o-C)
def boot(pordia, n=4000, seed=11):
    dias=list(pordia)
    if len(dias)<2: return float('nan'),float('nan')
    rnd=random.Random(seed); out=[]
    for _ in range(n):
        s=0.0;k=0
        for _ in range(len(dias)):
            for x in pordia[rnd.choice(dias)]: s+=x;k+=1
        if k: out.append(100*s/k)
    out.sort(); return out[int(.025*len(out))], out[int(.975*len(out))]

def cel(sub, rot, lado='lay'):
    n=len(sub)
    if n==0: print(f"  {rot:<44} N=0"); return
    red = sub['is0'].values==1
    if lado=='lay':
        o=sub['Odd_Under05_FT_Lay'].values
        pl=np.where(red,-1.0,(1-C)/(o-1)); bem=be_lay(o).mean(); wr=(~red).mean(); nred=int(red.sum())
    else:  # BACK Under 0.5: ganha no 0-0
        o=sub['Odd_Under05_FT_Back'].values
        pl=np.where(red,(o-1)*(1-C),-1.0); bem=(1.0/o).mean(); wr=red.mean(); nred=int((~red).sum())
    pordia=collections.defaultdict(list)
    for dia,x in zip(sub['dia'].values,pl): pordia[dia].append(x)
    lo,hi=boot(pordia)
    se=math.sqrt(bem*(1-bem)/n); z=(wr-bem)/se if se>0 else 0
    flag='  <5 reds' if nred<5 else ''
    print(f"  {rot:<44} N={n:<5} WR={100*wr:5.2f}% BE={100*bem:5.2f}% gap={100*(wr-bem):+6.2f}pp "
          f"z={z:+5.2f} ROIliab={100*pl.mean():+6.2f}% IC95=[{lo:+6.2f},{hi:+6.2f}] reds={nred}{flag}")

reg = d26[(d26['Odd_Under05_FT_Lay']>=2.0)&(d26['Odd_Under05_FT_Lay']<=16.5)]

print("\n"+"="*134)
print("1. A REGRA DE OUTUBRO NA BASE INDEPENDENTE (FRESH3, so 2026)")
print("="*134)
cel(reg, "lay U0.5, teto<=16.5, todos os favoritos")
cel(reg[reg['fav']<=1.80], "lay U0.5, teto<=16.5, Fav<=1.80   <-- a regra")
cel(reg[reg['fav']>1.80],  "lay U0.5, teto<=16.5, Fav> 1.80")
print()
cel(reg[reg['fav']<=1.50], "  Fav<=1.50")
cel(reg[(reg['fav']>1.50)&(reg['fav']<=1.80)], "  Fav 1.50-1.80")
cel(reg[(reg['fav']>1.80)&(reg['fav']<=2.20)], "  Fav 1.80-2.20")

print("\n"+"="*134)
print("2. ESPELHO VERDADEIRO — BACK Under 0.5 (lado OPOSTO do mesmo mercado, mesmo subconjunto)")
print("   se o lay ganha X, o back tem que perder ~X + overround. Se os DOIS perdem, o lay nao tem edge.")
print("="*134)
rb = reg.dropna(subset=['Odd_Under05_FT_Back'])
cel(rb, "BACK U0.5, teto<=16.5, todos", 'back')
cel(rb[rb['fav']<=1.80], "BACK U0.5, teto<=16.5, Fav<=1.80", 'back')
print()
s=rb[rb['fav']<=1.80]
if len(s):
    pl_lay=(1-C)/(s['Odd_Under05_FT_Lay'].values-1); 
    p_lay=0.95/(s['Odd_Under05_FT_Lay'].values-0.05)
    p_back=1.0/s['Odd_Under05_FT_Back'].values
    print(f"  P(0-0) do lado LAY  (0.95/(L-0.05)): mediana {np.median(p_lay):.4f}")
    print(f"  P(0-0) do lado BACK (1/B)          : mediana {np.median(p_back):.4f}")
    print(f"  soma dos dois lados (overround real): mediana {np.median(p_lay+p_back):.4f}")
    print(f"  taxa real de 0-0 no subconjunto     : {100*s['is0'].mean():.2f}%")

print("\n"+"="*134)
print("3. MES A MES EM 2026 — a regra Fav<=1.80 (dose-resposta no tempo)")
print("="*134)
for m in sorted(reg['mes'].unique()):
    cel(reg[(reg['mes']==m)&(reg['fav']<=1.80)], f"  {m}")

print("\n"+"="*134)
print("4. ANO A ANO (mesma regra fora de 2026 — so para ver estabilidade, nao e evidencia)")
print("="*134)
for a in sorted(d['ano'].unique()):
    s=d[(d['ano']==a)&(d['Odd_Under05_FT_Lay']>=2.0)&(d['Odd_Under05_FT_Lay']<=16.5)&(d['fav']<=1.80)]
    cel(s, f"  {a} Fav<=1.80")

print("\n"+"="*134)
print("5. TAXA DE 0-0 POR FAIXA DE FAVORITO (2026) — o mecanismo e real?")
print("="*134)
for lo,hi in [(1.0,1.3),(1.3,1.5),(1.5,1.8),(1.8,2.2),(2.2,3.0),(3.0,99)]:
    s=d26[(d26['fav']>=lo)&(d26['fav']<hi)]
    if len(s)<30: continue
    u=s['Odd_Under05_FT_Lay']
    print(f"  fav {lo:.1f}-{hi:.1f}: N={len(s):<6} taxa 0-0 real={100*s['is0'].mean():5.2f}%  "
          f"preco lay U0.5 mediano={u.median():6.2f} -> implicito={100*0.95/(u.median()-0.05):5.2f}%  "
          f"gap={100*(0.95/(u.median()-0.05)-s['is0'].mean()):+5.2f}pp")
