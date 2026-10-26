import sys, unicodedata, re, math
import pandas as pd, numpy as np
sys.stdout.reconfigure(encoding='utf-8')
C=0.05
SC=r"C:\Users\thiag\AppData\Local\Temp\claude\c--Users-thiag-OneDrive-Documentos-GitHub-DASHBOARD-ARKAD-1\7f5245c7-6d69-449c-b748-6d1e28580635\scratchpad"
B=r"C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD"
def canon(s):
    s=unicodedata.normalize('NFKD',str(s)).encode('ascii','ignore').decode().lower()
    return re.sub(r'[^a-z0-9]','',s)
d=pd.read_csv(SC+r"\ht10.csv")
for c in ['mtk','ht_ob','ht_obs','ft_ob','pre_h','pre_a']: d[c]=pd.to_numeric(d[c],errors='coerce')
d['dia']=d['ko'].str[:10]
p=pd.read_csv(B+r"\metodos_aprovados\placares_ft.csv")
p['k']=pd.to_datetime(p['ko'],errors='coerce').dt.strftime('%Y-%m-%d')+"_"+p['home'].apply(canon)+"_"+p['away'].apply(canon)
p=p.dropna(subset=['ht05']).drop_duplicates('k')
d['k']=d['dia']+"_"+d['home'].apply(canon)+"_"+d['away'].apply(canon)
d=d.merge(p[['k','ht05']],on='k',how='left').dropna(subset=['ht05','ht_ob'])
d['green']=(d['ht05']=='Over 0.5 Goals'); d['fav']=np.minimum(d['pre_h'],d['pre_a'])
u=d[(d['ft_ob']>1.05)|(d['ft_ob'].isna())]
T1=['English Premier','Spanish La Liga','Italian Serie A','German Bundesliga','French Ligue 1','Brazilian Serie A','Argentinian Primera Division','UEFA Champions','UEFA Europa','Portuguese Primeira','Dutch Eredivisie','Belgian First']
T2=['Championship','2. Bundesliga','Spanish Segunda','Italian Serie B','French Ligue 2','Brazilian Serie B','Argentinian Primera Nacional','Dutch Eerste','Portuguese Segunda','Scottish Premiership']
def tier(c):
    c=str(c)
    if any(k.lower() in c.lower() for k in T1): return 1
    if any(k.lower() in c.lower() for k in T2): return 2
    return 3
u=u.copy(); u['tier']=u['competition'].apply(tier)

print("="*118); print("1. DOSE-RESPOSTA POR TIER (mecanismo real e monotono; serra e ruido)"); print("="*118)
for t in (1,2,3):
    s=u[u['tier']==t]; o=s['ht_ob'].values; g=s['green'].values
    bem=(1/o).mean(); pl=np.where(g,(o-1)*(1-C),-1.0)
    print(f"  tier {t}: N={len(s):<5} WR={100*g.mean():6.2f}% BE={100*bem:6.2f}% margem={100*(g.mean()-bem):+6.2f}pp ROI={100*pl.mean():+6.2f}% reds={int((~g).sum())}")
print("  reportado por eles: T1 -9,15% | T2 +6,18% | T3 -4,42%  -> negativo, POSITIVO, negativo")

print("\n"+"="*118); print("2. CUSTO DA LATENCIA IN-PLAY"); print("="*118)
mm=-u['mtk'].median(); wr=u['green'].mean()
rest=45.0-mm+2.0
lam=-math.log(1-wr)/rest
print(f"  minuto mediano de captura: {mm:.1f} | WR ate o HT: {100*wr:.2f}% | tempo restante ate o HT: ~{rest:.1f} min")
print(f"  taxa de gol implicita: lambda = {lam:.4f} gol/min (Poisson)")
print()
o=u['ht_ob'].values; g=u['green'].values; bem=(1/o).mean()
print(f"  {'atraso':>8} {'P(gol no atraso)':>18} {'greens perdidos':>17} {'WR efetivo':>12} {'margem':>10}")
print(f"  {'0 s':>8} {'--':>18} {'--':>17} {100*wr:11.2f}% {100*(wr-bem):+9.2f}pp")
for t in (1,2,3,5):
    pg=1-math.exp(-lam*t)
    wr2=(wr-pg)/(1-pg)
    print(f"  {str(t)+' min':>8} {100*pg:17.2f}% {100*pg:16.2f}% {100*wr2:11.2f}% {100*(wr2-bem):+9.2f}pp")
print()
print("  O vies e de MAO UNICA: voce so deixa de entrar quando o gol sai antes de voce pegar preco,")
print("  e esses jogos eram todos GREEN. Cada minuto de atraso tira ~2,8pp do WR.")

print("\n"+"="*118); print("3. O COMBO A sob atraso de 2 min"); print("="*118)
s=u[(u['tier'].isin([1,2]))&(u['fav']<=1.70)&(u['ht_ob']>=1.40)&(u['ht_ob']<=1.75)]
o=s['ht_ob'].values; g=s['green'].values; bem=(1/o).mean(); wr=g.mean()
print(f"  medido      : N={len(s)} WR={100*wr:.2f}% BE={100*bem:.2f}% margem={100*(wr-bem):+.2f}pp")
for t in (1,2,3):
    pg=1-math.exp(-lam*t); wr2=(wr-pg)/(1-pg)
    print(f"  atraso {t} min: WR={100*wr2:.2f}% margem={100*(wr2-bem):+.2f}pp")
