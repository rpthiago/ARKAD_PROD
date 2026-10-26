import sys, unicodedata, re, random, collections, math, itertools
import pandas as pd, numpy as np
sys.stdout.reconfigure(encoding='utf-8')
C=0.05
SC=r"C:\Users\thiag\AppData\Local\Temp\claude\c--Users-thiag-OneDrive-Documentos-GitHub-DASHBOARD-ARKAD-1\7f5245c7-6d69-449c-b748-6d1e28580635\scratchpad"
B=r"C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD"
def canon(s):
    s=unicodedata.normalize('NFKD',str(s)).encode('ascii','ignore').decode().lower()
    return re.sub(r'[^a-z0-9]','',s)
d=pd.read_csv(SC+r"\ht10.csv")
for c in ['mtk','ht_ob','ht_obs','ht_ub','ft_ob','pre_h','pre_d','pre_a']:
    d[c]=pd.to_numeric(d[c],errors='coerce')
d['dia']=d['ko'].str[:10]
p=pd.read_csv(B+r"\metodos_aprovados\placares_ft.csv")
p['k']=pd.to_datetime(p['ko'],errors='coerce').dt.strftime('%Y-%m-%d')+"_"+p['home'].apply(canon)+"_"+p['away'].apply(canon)
p=p.dropna(subset=['ht05']).drop_duplicates('k')
d['k']=d['dia']+"_"+d['home'].apply(canon)+"_"+d['away'].apply(canon)
d=d.merge(p[['k','ht05']],on='k',how='left').dropna(subset=['ht05','ht_ob'])
d['green']=(d['ht05']=='Over 0.5 Goals')
d['fav']=np.minimum(d['pre_h'],d['pre_a'])
d=d[(d['ft_ob']>1.05)|(d['ft_ob'].isna())]          # ainda 0-0 na captura

T1=['English Premier','Spanish La Liga','Italian Serie A','German Bundesliga','French Ligue 1',
    'Brazilian Serie A','Argentinian Primera Division','UEFA Champions','UEFA Europa','Portuguese Primeira',
    'Dutch Eredivisie','Belgian First']
T2=['Championship','2. Bundesliga','Spanish Segunda','Italian Serie B','French Ligue 2',
    'Brazilian Serie B','Argentinian Primera Nacional','Dutch Eerste','Portuguese Segunda','Scottish Premiership']
def tier(c):
    c=str(c)
    if any(k.lower() in c.lower() for k in T1): return 1
    if any(k.lower() in c.lower() for k in T2): return 2
    return 3
d['tier']=d['competition'].apply(tier)
print(f"universo (0-0 na captura, com ht05): {len(d)}  | tier1={sum(d.tier==1)} tier2={sum(d.tier==2)} tier3={sum(d.tier==3)}")

def marg(sub, g=None):
    n=len(sub)
    if n==0: return None,0,0
    o=sub['ht_ob'].values; gg=sub['green'].values if g is None else g
    bem=(1.0/o).mean(); return gg.mean()-bem, n, int((~gg).sum())
def roi(sub,g=None):
    o=sub['ht_ob'].values; gg=sub['green'].values if g is None else g
    return np.where(gg,(o-1)*(1-C),-1.0).mean()

print("\n"+"="*120); print("1. O COMBO A, reproduzido"); print("="*120)
for rot,sel in (("periodo todo (27/09-26/10)", d),("so outubro", d[d['dia']>='2026-10-01'])):
    s=sel[(sel['tier'].isin([1,2]))&(sel['fav']<=1.70)&(sel['ht_ob']>=1.40)&(sel['ht_ob']<=1.75)]
    m,n,r=marg(s)
    if n: print(f"  {rot:<28} N={n:<4} margem={100*m:+6.2f}pp ROI={100*roi(s):+6.2f}% reds={r}"
                f"  z={m/math.sqrt((1/s['ht_ob'].values).mean()*(1-(1/s['ht_ob'].values).mean())/n):+5.2f}")
print("  reportado por eles: N=54 margem=+7,82pp ROI=+11,89% (41G/13R)")

# ---------- a GRADE que foi varrida ----------
tiersets=[(1,),(2,),(3,),(1,2),(2,3),(1,3),(1,2,3)]
favs=[1.30,1.40,1.50,1.60,1.70,1.80,2.00,2.20,99.0]
odds=[(1.15,1.40),(1.30,1.50),(1.40,1.60),(1.40,1.75),(1.40,1.80),(1.50,1.70),(1.55,1.75),
      (1.60,1.80),(1.80,2.10),(1.15,2.10)]
periodos=[("todo",d),("out",d[d['dia']>='2026-10-01'])]
celulas=[]
for rotp,base in periodos:
    for ts in tiersets:
        for fv in favs:
            for lo,hi in odds:
                mask=(base['tier'].isin(ts))&(base['fav']<=fv)&(base['ht_ob']>=lo)&(base['ht_ob']<=hi)
                sub=base[mask]
                if len(sub)>=30:
                    _,n,r=marg(sub)
                    if r>=5: celulas.append((rotp,ts,fv,lo,hi,sub.index.values))
print(f"\n"+"="*120)
print(f"2. TAMANHO DO ESPAÇO DE BUSCA: {len(celulas)} células elegíveis (N>=30 e >=5 reds)")
print("   tier(7 subconjuntos) x fav(9 cortes) x faixa de odd(10) x periodo(2)")
print("="*120)
res=[]
for rotp,ts,fv,lo,hi,idx in celulas:
    sub=d.loc[idx]; m,n,r=marg(sub); res.append((m,n,r,rotp,ts,fv,lo,hi))
res.sort(reverse=True)
print("  as 8 melhores células do grid:")
for m,n,r,rotp,ts,fv,lo,hi in res[:8]:
    print(f"    margem={100*m:+6.2f}pp N={n:<4} reds={r:<3} | tier={ts} fav<={fv} odd[{lo},{hi}] periodo={rotp}")
obs_max=res[0][0]

# ---------- permutacao: embaralha o desfecho, revarre a grade ----------
gl=d['green'].values.copy()
pos={ix:i for i,ix in enumerate(d.index.values)}
cel_pos=[np.array([pos[i] for i in idx]) for *_,idx in celulas]
cel_be=[ (1.0/d['ht_ob'].values[pp]).mean() for pp in cel_pos ]
rnd=np.random.default_rng(7)
NP=2000; ge=0; nulos=[]
for _ in range(NP):
    sh=rnd.permutation(gl)
    best=-9
    for pp,bem in zip(cel_pos,cel_be):
        mm=sh[pp].mean()-bem
        if mm>best: best=mm
    nulos.append(best)
    if best>=obs_max: ge+=1
nulos=np.sort(np.array(nulos))
print("\n"+"="*120)
print("3. PERMUTAÇÃO SOBRE A GRADE INTEIRA (2.000 sorteios; jogos e odds fixos, desfecho embaralhado)")
print("   H0: nenhuma célula tem edge; o melhor resultado é só o máximo de uma busca")
print("="*120)
print(f"  melhor célula OBSERVADA         : {100*obs_max:+6.2f}pp")
print(f"  melhor célula sob H0 — mediana  : {100*np.median(nulos):+6.2f}pp")
print(f"  melhor célula sob H0 — p95      : {100*nulos[int(.95*NP)]:+6.2f}pp")
print(f"  P(melhor sob H0 >= observado)   : {ge/NP:.4f}   {'SIGNIFICATIVO' if ge/NP<0.05 else '>>> NAO SIGNIFICATIVO'}")
print()
s=d[(d['tier'].isin([1,2]))&(d['fav']<=1.70)&(d['ht_ob']>=1.40)&(d['ht_ob']<=1.75)]
if len(s):
    m,n,r=marg(s); pp=np.array([pos[i] for i in s.index.values]); bem=(1.0/d['ht_ob'].values[pp]).mean()
    ge2=sum(1 for _ in range(NP) if (rnd.permutation(gl)[pp].mean()-bem)>=m)
    print(f"  o Combo A isolado: margem={100*m:+6.2f}pp, P sem correcao = {ge2/NP:.4f}")
    print(f"  mas ele foi ESCOLHIDO entre {len(celulas)} células -> o P que vale e o da grade, acima")
