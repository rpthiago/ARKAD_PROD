# -*- coding: utf-8 -*-
import os, sys, warnings; warnings.filterwarnings("ignore"); sys.stdout.reconfigure(encoding="utf-8")
import pandas as pd, numpy as np
T=os.path.join(os.environ["TEMP"],"var")
d=pd.read_csv(os.path.join(T,"cs_pre7.csv"),header=None,names=["ts","ko","home","away","mtk","runner","back","back_size","lay","lay_size"])
for c in ("mtk","back","back_size","lay","lay_size"): d[c]=pd.to_numeric(d[c],errors="coerce")
d=d.dropna(subset=["mtk","lay"]); d["dia"]=d.ko.str[:10]; d["spread"]=(d.lay-d.back)/d.back*100
print("capturas: %d | jogos: %d | %s -> %s"%(len(d),d[["ko","home","away"]].drop_duplicates().shape[0],d.dia.min(),d.dia.max()))
print("\n%-22s %-6s %6s %8s %8s %8s %9s %8s %7s | %9s %9s"%("runner (faixa)","janela","N","spr p50","spr p75","spr p90","size p50","size p25",">=R$100","drift|d|","saiu faixa"))
for runner,lo,hi in (("Any Other Away Win",15,60),("Any Other Home Win",15,60),("0 - 1",5,15),("1 - 0",5,15),("0 - 0",10,20),("0 - 2",5,25),("2 - 0",5,25)):
    r=d[(d.runner==runner)&(d.lay>=lo)&(d.lay<=hi)]
    ko=r[(r.mtk>=-5)&(r.mtk<=15)]; cedo=r[r.mtk>=180]
    # drift: jogos com captura >=3h antes na faixa; onde esta o lay no KO?
    c1=cedo.sort_values("mtk",ascending=False).groupby(["ko","home","away"]).first()
    all_ko=d[(d.runner==runner)&(d.mtk>=-5)&(d.mtk<=15)].sort_values("mtk").groupby(["ko","home","away"]).first()
    m=c1.join(all_ko[["lay"]],rsuffix="_ko",how="inner")
    saiu=100*((m.lay_ko<lo)|(m.lay_ko>hi)).mean() if len(m) else np.nan
    drift=(m.lay_ko-m.lay).abs().median() if len(m) else np.nan
    for tag,w in (("KO",ko),("3h antes",cedo)):
        if len(w)==0: continue
        print("%-22s %-6s %6d %7.1f%% %7.1f%% %7.1f%% %9.0f %8.0f %6.0f%% | %9s %9s"%(("%s %d-%d"%(runner,lo,hi))[:22] if tag=="KO" else "",tag,len(w),w.spread.median(),w.spread.quantile(.75),w.spread.quantile(.9),w.lay_size.median(),w.lay_size.quantile(.25),100*(w.lay_size>=100).mean(),("%.1f"%drift) if tag=="KO" else "",("%.0f%% (n=%d)"%(saiu,len(m))) if tag=="KO" else ""))
