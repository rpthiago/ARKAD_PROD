import sys, unicodedata, re, random, collections, math
import pandas as pd, numpy as np
sys.stdout.reconfigure(encoding='utf-8')
C=0.05
SC=r"C:\Users\thiag\AppData\Local\Temp\claude\c--Users-thiag-OneDrive-Documentos-GitHub-DASHBOARD-ARKAD-1\7f5245c7-6d69-449c-b748-6d1e28580635\scratchpad"
B=r"C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD"
def canon(s):
    s=unicodedata.normalize('NFKD',str(s)).encode('ascii','ignore').decode().lower()
    return re.sub(r'[^a-z0-9]','',s)

d=pd.read_csv(SC+r"\ht10.csv")
for c in ['mtk','ht_ob','ht_obs','ht_ol','ht_ols','ht_ub','ft_ob','pre_h','pre_d','pre_a']:
    d[c]=pd.to_numeric(d[c],errors='coerce')
d['dia']=d['ko'].str[:10]
print(f"extraido do coletor: {len(d)} jogos com captura FIRST_HALF_GOALS_05 em mtk [-18,-7]")
print(f"  minuto de jogo (= -mtk): min {-d['mtk'].max():.1f} | mediana {-d['mtk'].median():.1f} | max {-d['mtk'].min():.1f}")

p=pd.read_csv(B+r"\metodos_aprovados\placares_ft.csv")
p['k']=pd.to_datetime(p['ko'],errors='coerce').dt.strftime('%Y-%m-%d')+"_"+p['home'].apply(canon)+"_"+p['away'].apply(canon)
p=p.dropna(subset=['ht05']).drop_duplicates('k')
d['k']=d['dia']+"_"+d['home'].apply(canon)+"_"+d['away'].apply(canon)
d=d.merge(p[['k','ht05','gh','ga']],on='k',how='left')
print(f"  casados com ht05 autoritativo: {d['ht05'].notna().sum()}")
d=d.dropna(subset=['ht05','ht_ob'])
d['green']=(d['ht05']=='Over 0.5 Goals')

print("\n--- o jogo estava 0-0 na captura? (estado factual pela linha Over 0.5 FT) ---")
print(f"  ft_ob (back de Over 0.5 FT) <= 1.05 => gol JA tinha saido: {(d['ft_ob']<=1.05).sum()} jogos")
print(f"  ft_ob > 1.05 (ainda 0-0): {(d['ft_ob']>1.05).sum()} | ft_ob nulo: {d['ft_ob'].isna().sum()}")
zz=d[d['ft_ob']<=1.05]
if len(zz): print(f"  nos que ja tinham gol: WR={100*zz['green'].mean():.2f}% odd HT media={zz['ht_ob'].mean():.2f}")

def cel(s,rot,sh=None):
    n=len(s)
    if n==0: return None
    o=s['ht_ob'].values; g=(s['green'].values if sh is None else sh)
    pl=np.where(g,(o-1)*(1-C),-1.0); bem=(1.0/o).mean(); wr=g.mean(); nred=int((~g).sum())
    se=math.sqrt(bem*(1-bem)/n); z=(wr-bem)/se if se>0 else 0
    if rot is not None:
        print(f"  {rot:<40} N={n:<5} WR={100*wr:6.2f}% BE={100*bem:6.2f}% margem={100*(wr-bem):+6.2f}pp "
              f"z={z:+5.2f} ROI={100*pl.mean():+6.2f}% P&L={pl.sum():+7.2f}u reds={nred}{'  <5 reds' if nred<5 else ''}")
    return (wr-bem), n, nred

print("\n"+"="*126); print("1. UNIVERSO GERAL — Back Over 0.5 HT na 1a captura da janela"); print("="*126)
cel(d,"todos os casados")
u=d[(d['ft_ob']>1.05)|(d['ft_ob'].isna())]
cel(u,"so os que ainda estavam 0-0")
print()
print("  reportado por eles: N=3.297 WR=65,64% BE=68,45% margem=-2,82pp ROI=-4,31%")

print("\n"+"="*126); print("2. ESPELHO — Back Under 0.5 HT (lado oposto do mesmo mercado)"); print("="*126)
uu=u.dropna(subset=['ht_ub'])
o=uu['ht_ub'].values; g=(~uu['green'].values)
pl=np.where(g,(o-1)*(1-C),-1.0); bem=(1.0/o).mean()
print(f"  BACK Under 0.5 HT                        N={len(uu):<5} WR={100*g.mean():6.2f}% BE={100*bem:6.2f}% "
      f"margem={100*(g.mean()-bem):+6.2f}pp ROI={100*pl.mean():+6.2f}%")
print(f"  soma das margens dos dois lados = o overround real do mercado")

print("\n"+"="*126); print("3. POR FAIXA DE ODD IN-PLAY"); print("="*126)
for lo,hi in [(1.15,1.40),(1.40,1.60),(1.60,1.80),(1.80,2.10)]:
    cel(u[(u['ht_ob']>=lo)&(u['ht_ob']<hi)],f"  odd [{lo:.2f}, {hi:.2f})")

print("\n"+"="*126); print("4. LIQUIDEZ — da para entrar de verdade?"); print("="*126)
s=u.dropna(subset=['ht_obs'])
print(f"  back_size no lado Over 0.5 HT: mediana R$ {s['ht_obs'].median():.0f} | p25 R$ {s['ht_obs'].quantile(.25):.0f} | p10 R$ {s['ht_obs'].quantile(.10):.0f}")
for piso in (50,100,200,500):
    cel(s[s['ht_obs']>=piso],f"  com back_size >= R$ {piso}")
