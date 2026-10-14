# -*- coding: utf-8 -*-
import sys, os
sys.path.insert(0, r"C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD")
os.chdir(r"C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD")
sys.stdout.reconfigure(encoding="utf-8")
import pandas as pd, numpy as np
S=sys.argv[1]; sys.path.insert(0,S)
import sinais_ko_core as K
rng=np.random.default_rng(20261013)
B=pd.read_csv(r"metodos_aprovados\.cache_base_betfair.csv", low_memory=False)
B["Date"]=B["Date"].astype(str).str[:10]
B=B[(B["Date"]>="2026-01-01")]
for c in ("Odd_CS_0x3_Lay","Odd_CS_2x2_Lay","Odd_Under25_FT_Back","Odd_A_Back","Odd_H_Back",
          "Odd_Over35_FT_Back","Goals_H_FT","Goals_A_FT"):
    B[c]=pd.to_numeric(B.get(c),errors="coerce")
B=B.dropna(subset=["Goals_H_FT","Goals_A_FT"])
B=B[~B.apply(lambda r: K.eh_jogo_ignorado(r["Home"],r["Away"],r["League"]),axis=1)]
print("\nuniverso 2026 na base da API, com governanca: %d jogos (%s a %s)"%(len(B),B["Date"].min(),B["Date"].max()))
C=0.05
def prep(d,odd,red):
    d=d.copy(); d["odd"]=d[odd]; d["red"]=red(d); d=d.dropna(subset=["odd"])
    d["pnl"]=np.where(d["red"],-1.0,(1-C)/(d["odd"]-1)); d["be"]=(d["odd"]-1)/(d["odd"]-C)
    d=d.sort_values("Date"); d["top3"]=d.groupby("Date")["odd"].rank(method="first")<=3
    return d
def boot(g,n=3000):
    uq=pd.unique(g["Date"].values)
    if len(uq)<6: return (np.nan,np.nan)
    ix={x:np.where(g["Date"].values==x)[0] for x in uq}
    p=g["pnl"].values; out=np.empty(n)
    for i in range(n):
        e=rng.choice(uq,size=len(uq),replace=True); out[i]=np.concatenate([p[ix[x]] for x in e]).mean()
    return tuple(np.percentile(out,[2.5,97.5]))
def ln(rot,g):
    if len(g)<15: print("  %-42s %5d  (pequena)"%(rot,len(g))); return
    lo,hi=boot(g); ic="[%+.2f%%, %+.2f%%]"%(100*lo,100*hi) if not np.isnan(lo) else "-"
    print("  %-42s %5d %6.2f%% %8.2f%% %+8.2f%% %+8.2f   %s"%(rot,len(g),100*(1-g["red"].mean()),100*g["be"].mean(),100*g["pnl"].mean(),g["pnl"].sum(),ic))
CT="2026-05-15"
# 0x3
s3=(B["Odd_Under25_FT_Back"]<=2.10)&B["Odd_CS_0x3_Lay"].between(14,35)&((B["Odd_A_Back"].isna())|(B["Odd_A_Back"]>=1.85))
D3=prep(B[s3.fillna(False)],"Odd_CS_0x3_Lay",lambda d:(d["Goals_H_FT"]==0)&(d["Goals_A_FT"]==3))
print("\n"+"="*112); print("LAY 0x3 — 2026 inteiro na base da API (n=%d)"%len(D3)); print("="*112)
print("  %-42s %5s %6s %8s %9s %9s   %s"%("cenario","N","WR","break-even","ROI","PnL(u)","IC95")); print("  "+"-"*102)
ln("ampla (todos)",D3); ln("TOP 3 do dia",D3[D3["top3"]]); ln("fora do Top 3",D3[~D3["top3"]])
ln("ampla, odd <= 30",D3[D3["odd"]<=30]); ln("Top 3 + odd <= 30",D3[D3["top3"]&(D3["odd"]<=30)])
print(); ln("  1a metade (jan-mai)",D3[D3["Date"]<=CT]); ln("  2a metade (mai-set)",D3[D3["Date"]>CT])
# 2x2
s2=B["Odd_CS_2x2_Lay"].between(8,20)&((B["Odd_Under25_FT_Back"]<=2.00)|(B["Odd_H_Back"]<=1.55)|(B["Odd_A_Back"]<=1.60))
bl=B["League"].astype(str).str.upper().apply(lambda x: any(b in x for b in K.BLACKLIST_2X2))
D2=prep(B[s2.fillna(False)&~bl],"Odd_CS_2x2_Lay",lambda d:(d["Goals_H_FT"]==2)&(d["Goals_A_FT"]==2))
o35=D2["Odd_Over35_FT_Back"]
print("\n"+"="*112); print("LAY 2x2 — 2026 inteiro na base da API (n=%d)"%len(D2)); print("="*112)
print("  %-42s %5s %6s %8s %9s %9s   %s"%("cenario","N","WR","break-even","ROI","PnL(u)","IC95")); print("  "+"-"*102)
ln("regra antiga (8-20), sem Top 3",D2); ln("COM Top 3",D2[D2["top3"]]); ln("fora do Top 3",D2[~D2["top3"]])
ln("excluir Over 3.5 <= 2,20",D2[(o35>2.20)|o35.isna()])
print(); ln("  1a metade (jan-mai)",D2[D2["Date"]<=CT]); ln("  2a metade (mai-set)",D2[D2["Date"]>CT])
