# -*- coding: utf-8 -*-
"""Bloco A da varredura da semana: re-liquida under-limite (A1) e refaz Rota C (A3) com placar EXTERNO exato."""
import os, sys, re, unicodedata, warnings; warnings.filterwarnings("ignore"); sys.stdout.reconfigure(encoding="utf-8")
ROOT=os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0,ROOT)
import pandas as pd, numpy as np, relatorio_forward_5metodos as RF
T=os.path.join(os.environ.get("TEMP","."),"var"); C=0.05
P=RF.placares(); idx=RF._por_dia(P)

def boot(v, blocos, B=10000, seed=3):
    mm={k:v[blocos==k] for k in pd.unique(blocos)}; ks=list(mm)
    if len(ks)<6: return np.nan,np.nan,np.nan,len(ks)
    rng=np.random.default_rng(seed); r=np.empty(B)
    for i in range(B): r[i]=np.concatenate([mm[ks[j]] for j in rng.integers(0,len(ks),len(ks))]).mean()
    lo,hi=np.percentile(r,[2.5,97.5]); return lo,hi,float((r<=0).mean()),len(ks)

# ================================================================== A1 — UNDER-LIMITE
print("="*90); print("A1 — UNDER-LIMITE: re-liquidacao com placar externo"); print("="*90)
u=pd.read_csv(os.path.join(T,"under_alertas_log.csv"),encoding="utf-8-sig")
u=u[u.status=="Finalizado"].copy()
u["linha_n"]=u.linha.str.extract(r"([\d.]+)").astype(float); u["odd"]=pd.to_numeric(u.odd,errors="coerce")
u["fonte_grav"]=np.where(u.ft_total.astype(str).str.startswith("BF:"),"oficial","coletor")
gh=[];ga=[];fo=[]
for _,r in u.iterrows():
    sc=RF.achar_placar(P,idx,str(r.data)[:10],r.home,r.away)
    gh.append(sc[0] if sc else np.nan); ga.append(sc[1] if sc else np.nan); fo.append(sc[2].split(":")[0] if sc else "")
u["gh"]=gh; u["ga"]=ga; u["fonte_ext"]=fo
v=u.dropna(subset=["gh","ga"]).copy()
v["res_ext"]=np.where(v.gh+v.ga<v.linha_n,"GREEN","RED")
v["bate"]=v.res_ext==v.resultado
print("finalizados: %d | com placar externo: %d (%.0f%%)"%(len(u),len(v),100*len(v)/len(u)))
for f,g in v.groupby("fonte_grav"):
    print("  gravado por %-8s N=%3d | resultado bate com externo: %d | DIVERGE: %d (%.1f%%)"%(f,len(g),g.bate.sum(),(~g.bate).sum(),100*(~g.bate).mean()))
d=v[~v.bate]
if len(d):
    print("  divergencias (gravado -> externo):"); print("   ", d[["data","jogo","linha","resultado","res_ext","gh","ga","fonte_grav"]].to_string(index=False).replace("\n","\n    "))
    print("  falso GREEN (gravado GREEN, externo RED): %d | falso RED: %d"%(((d.resultado=="GREEN")&(d.res_ext=="RED")).sum(),((d.resultado=="RED")&(d.res_ext=="GREEN")).sum()))
def pnl(res,odd): return np.where(res=="GREEN",(odd-1)*(1-C),-1.0)
for nome,col in [("GRAVADO",v.resultado),("EXTERNO",v.res_ext)]:
    p=pnl(col,v.odd); wr=(col=="GREEN").mean(); be=(1/(1+(v.odd-1)*(1-C))).mean()
    lo,hi,p0,nk=boot(p,v.data.values)
    print("  %-8s N=%d WR=%.1f%% BE=%.1f%% gap=%+.1fpp ROI=%+.2f%% IC95=[%+.1f%%,%+.1f%%] p=%.3f (%d dias)"%(nome,len(v),100*wr,100*be,100*(wr-be),100*p.mean(),100*lo,100*hi,p0,nk))
u.to_csv(os.path.join(ROOT,"varredura_over","A1_under_limite_reliquidado.csv"),index=False,encoding="utf-8-sig")

# ================================================================== A3 — ROTA C
print(); print("="*90); print("A3 — ROTA C (Lay 0x1 in-play): 0-0 no min 55-75, lay do CS 0-1 em 2,00-5,50, placar EXTERNO"); print("="*90)
cs=pd.read_csv(os.path.join(T,"cs.csv"),header=None,names=["ts","ko","home","away","mtk","score"],dtype=str)
cs["mtk"]=pd.to_numeric(cs.mtk,errors="coerce"); cs["minuto"]=-cs.mtk-15
c01=pd.read_csv(os.path.join(T,"cs01.csv"),header=None,names=["ts","ko","home","away","mtk","lay01"],dtype=str)
c01["lay01"]=pd.to_numeric(c01.lay01,errors="coerce"); c01["mtk"]=pd.to_numeric(c01.mtk,errors="coerce")
m=c01.merge(cs[["ts","ko","home","away","score"]],on=["ts","ko","home","away"],how="inner")
m=m[m.score=="0 - 0"].sort_values("mtk",key=lambda s:(s+65).abs())   # captura mais perto do min 50... (mtk -65)
m=m.drop_duplicates(["ko","home","away"])
print("jogos 0-0 no min 55-75 com lay do 0-1 capturado: %d"%len(m))
gh=[];ga=[]
for _,r in m.iterrows():
    sc=RF.achar_placar(P,idx,r.ko[:10],r.home,r.away); gh.append(sc[0] if sc else np.nan); ga.append(sc[1] if sc else np.nan)
m["gh"]=gh; m["ga"]=ga; mm=m.dropna(subset=["gh","ga"]).copy()
mm["e01"]=((mm.gh==0)&(mm.ga==1)).astype(int); mm["dia"]=mm.ko.str[:10]
print("  com placar externo: %d"%len(mm))
faixa=mm[(mm.lay01>=2.0)&(mm.lay01<=5.5)]
print("  TODOS:            0x1 final = %d de %d = %.2f%%"%(mm.e01.sum(),len(mm),100*mm.e01.mean()))
print("  NA FAIXA 2,00-5,50: 0x1 final = %d de %d = %.2f%%   (auditoria de 10/09 pelo coletor: 28,73%%)"%(faixa.e01.sum(),len(faixa),100*faixa.e01.mean()))
if len(faixa):
    be=((faixa.lay01-1)/(faixa.lay01-C)).mean(); wr=1-faixa.e01.mean()
    p=np.where(faixa.e01==0,(1-C)/(faixa.lay01-1),-1.0); lo,hi,p0,nk=boot(p,faixa.dia.values)
    print("  lay 0x1 na faixa: odd media %.2f | break-even %.2f%% | WR real %.2f%% | gap %+.2fpp | ROI liab %+.2f%% IC95=[%+.1f%%,%+.1f%%] p=%.3f (%d dias)"%(faixa.lay01.mean(),100*be,100*wr,100*(wr-be),100*p.mean(),100*lo,100*hi,p0,nk))
