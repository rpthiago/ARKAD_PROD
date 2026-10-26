"""Versao honesta do estudo Mai-Ago/2026: mesmo split temporal, mesmo modelo,
mas ROI por LIABILITY, bootstrap de blocos-dia, e comparacao contra o mercado puro
(mesmo universo sem modelo) para isolar o que o ML adiciona.
"""
import sys, random, collections, math
import pandas as pd, numpy as np, lightgbm as lgb
import warnings; warnings.filterwarnings('ignore')
sys.stdout.reconfigure(encoding='utf-8')
C=0.05
B=r"C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD"
d=pd.read_parquet(B+r"\scratch\dataset_leak_free_features.parquet")
d['Date']=pd.to_datetime(d['Date']); d=d[d['Date']>='2026-01-01'].sort_values('Date').reset_index(drop=True)

oh=pd.to_numeric(d['Odd_H_Back'],errors='coerce'); od=pd.to_numeric(d['Odd_D_Back'],errors='coerce')
oa=pd.to_numeric(d['Odd_A_Back'],errors='coerce'); ul=pd.to_numeric(d['Odd_Under05_FT_Lay'],errors='coerce')
ph,pdd,pa=1/np.maximum(1.01,oh),1/np.maximum(1.01,od),1/np.maximum(1.01,oa); vig=ph+pdd+pa
d['p_H_clean'],d['p_D_clean'],d['p_A_clean']=ph/vig,pdd/vig,pa/vig
d['p_o15']=1/np.maximum(1.01,pd.to_numeric(d['Odd_Over15_FT_Back'],errors='coerce').fillna(1.3))
d['p_o25']=1/np.maximum(1.01,pd.to_numeric(d['Odd_Over25_FT_Back'],errors='coerce').fillna(2.0))
d['p_btts']=1/np.maximum(1.01,pd.to_numeric(d['Odd_BTTS_Yes_Back'],errors='coerce').fillna(1.9))
d['p_ht05']=1/np.maximum(1.01,pd.to_numeric(d['Odd_Over05_HT_Back'],errors='coerce').fillna(1.4))
d['p_implied_0x0_lay']=0.95/np.maximum(1.01,ul-0.05)
d['fav_odd']=np.minimum(oh,oa); d['fav_prob']=np.maximum(d['p_H_clean'],d['p_A_clean'])
d['fav_is_home']=(oh<oa).astype(float); d['fav_disparity']=np.abs(d['p_H_clean']-d['p_A_clean'])
d['draw_fav_ratio']=d['p_D_clean']/np.maximum(0.01,d['fav_prob'])
d['entropy_1x2']=-(d['p_H_clean']*np.log(d['p_H_clean']+1e-9)+d['p_D_clean']*np.log(d['p_D_clean']+1e-9)+d['p_A_clean']*np.log(d['p_A_clean']+1e-9))
d['ratio_o15_o25']=d['p_o15']/np.maximum(0.01,d['p_o25'])
d['ratio_btts_o25']=d['p_btts']/np.maximum(0.01,d['p_o25'])
d['ratio_ht05_o15']=d['p_ht05']/np.maximum(0.01,d['p_o15'])
d['exp_goals_h']=(d['H_GF_r5']+d['A_GA_r5'])/2; d['exp_goals_a']=(d['A_GF_r5']+d['H_GA_r5'])/2
d['total_exp_goals']=d['exp_goals_h']+d['exp_goals_a']; d['exp_goals_diff']=np.abs(d['exp_goals_h']-d['exp_goals_a'])
d['total_SoTF_r5']=d['H_SoTF_r5']+d['A_SoTF_r5']; d['net_SoT']=d['total_SoTF_r5']-8
d['total_CornersF_r5']=d['H_CornersF_r5']+d['A_CornersF_r5']; d['total_xGF_r5']=d['H_xGF_r5']+d['A_xGF_r5']
d['goals_per_implied_p00']=d['total_exp_goals']*d['p_implied_0x0_lay']
d['sot_per_implied_p00']=d['total_SoTF_r5']*d['p_implied_0x0_lay']
d['odd_lay']=ul
FC=['fav_odd','fav_prob','fav_is_home','fav_disparity','draw_fav_ratio','entropy_1x2','p_D_clean',
'p_o15','p_o25','p_btts','p_ht05','ratio_o15_o25','ratio_btts_o25','ratio_ht05_o15','p_implied_0x0_lay',
'H_GF_r5','H_GA_r5','A_GF_r5','A_GA_r5','exp_goals_h','exp_goals_a','total_exp_goals','exp_goals_diff',
'H_SoTF_r5','A_SoTF_r5','total_SoTF_r5','net_SoT','H_CornersF_r5','A_CornersF_r5','total_CornersF_r5',
'total_xGF_r5','goals_per_implied_p00','sot_per_implied_p00','liga_0x0_rate','liga_o25_rate','liga_btts_rate','liga_draw_rate']
for c in FC: d[c]=pd.to_numeric(d[c],errors='coerce').fillna(0.0)
d=d.dropna(subset=['odd_lay','is_0x0'])
tr=d[d['Date']<'2026-05-01']; te=d[d['Date']>='2026-05-01'].copy()
print(f"treino Jan-Abr: {len(tr)} | teste OOS Mai-Ago: {len(te)} (0-0 = {100*te['is_0x0'].mean():.2f}%)")
clf=lgb.LGBMClassifier(n_estimators=100,max_depth=3,num_leaves=7,learning_rate=0.03,
    min_child_samples=50,subsample=0.8,colsample_bytree=0.8,random_state=42,verbose=-1)
clf.fit(tr[FC],tr['is_0x0'].astype(int))
te['p0']=clf.predict_proba(te[FC])[:,1]
te['EV']=(1-te['p0'])*0.95-te['p0']*(te['odd_lay']-1)
te['dia']=te['Date'].dt.strftime('%Y-%m-%d')

def be(o): return (o-1)/(o-C)
def boot(pd_,n=4000,seed=11):
    dias=list(pd_)
    if len(dias)<2: return float('nan'),float('nan')
    rnd=random.Random(seed); out=[]
    for _ in range(n):
        s=0.0;k=0
        for _ in range(len(dias)):
            for x in pd_[rnd.choice(dias)]: s+=x;k+=1
        if k: out.append(100*s/k)
    out.sort(); return out[int(.025*len(out))],out[int(.975*len(out))]
def cel(s,rot):
    n=len(s)
    if n==0: print(f"  {rot:<50} N=0"); return
    o=s['odd_lay'].values; red=s['is_0x0'].values==1
    liab=np.where(red,-1.0,(1-C)/(o-1)); nom=np.where(red,-(o-1),1-C)
    bem=be(o).mean(); wr=(~red).mean(); nr=int(red.sum())
    pdia=collections.defaultdict(list)
    for dia,x in zip(s['dia'].values,liab): pdia[dia].append(x)
    lo,hi=boot(pdia); se=math.sqrt(bem*(1-bem)/n); z=(wr-bem)/se if se>0 else 0
    print(f"  {rot:<50} N={n:<5} WR={100*wr:5.2f}% BE={100*bem:5.2f}% gap={100*(wr-bem):+6.2f}pp z={z:+5.2f} "
          f"ROIliab={100*liab.mean():+6.2f}% IC95=[{lo:+6.2f},{hi:+6.2f}] ROInom={100*nom.mean():+7.2f}% reds={nr}{'  <5 reds' if nr<5 else ''}")

print("\n"+"="*146); print("A REGRA DO ESTUDO, no OOS Mai-Ago/2026, com ROI POR LIABILITY"); print("="*146)
base=te[(te['odd_lay']>=2.0)&(te['odd_lay']<=16.5)]
cel(base[(base['EV']>=0.02)&(base['liga_0x0_rate']<0.10)], "COM modelo (EV>=2%) + governanca  <-- a regra")
cel(base[base['liga_0x0_rate']<0.10],                      "SEM modelo, so governanca + teto")
cel(base[base['EV']>=0.02],                                "COM modelo, SEM governanca")
cel(base,                                                  "SEM modelo, SEM governanca (mercado puro)")
print()
cel(base[(base['EV']>=0.02)&(base['liga_0x0_rate']<0.10)&(base['fav_odd']<=1.80)], "a regra + Fav<=1.80")
print("\n"+"="*146); print("MES A MES (a regra completa)"); print("="*146)
for m in sorted(te['Date'].dt.to_period('M').astype(str).unique()):
    s=base[(base['EV']>=0.02)&(base['liga_0x0_rate']<0.10)&(base['Date'].dt.to_period('M').astype(str)==m)]
    cel(s,f"  {m}")
