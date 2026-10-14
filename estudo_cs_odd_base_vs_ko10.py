# -*- coding: utf-8 -*-
import sys, os, re, unicodedata
sys.stdout.reconfigure(encoding="utf-8")
import pandas as pd, numpy as np
S=sys.argv[1]
def canon(s):
    s=unicodedata.normalize("NFKD",str(s)).encode("ascii","ignore").decode().lower()
    return re.sub(r"[^a-z0-9]","",s)
cs=pd.read_csv(os.path.join(S,"vps_cs.csv"), low_memory=False)
cs["runner"]=cs["runner"].astype(str).str.strip()
for c in ("back","lay","lay_size"): cs[c]=pd.to_numeric(cs[c],errors="coerce")
V=cs[cs["market"]=="CORRECT_SCORE"].pivot_table(index=["dia","home","away"],columns="runner",values="lay",aggfunc="first").reset_index()
V.columns=[c if c in ("dia","home","away") else "ko_"+c for c in V.columns]
V["k"]=V["dia"]+"|"+V["home"].apply(canon)+"|"+V["away"].apply(canon)
B=pd.read_csv(r"C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD\metodos_aprovados\.cache_base_betfair.csv", low_memory=False)
B["Date"]=B["Date"].astype(str).str[:10]
B=B[(B["Date"]>="2026-08-17")&(B["Date"]<="2026-09-24")]
for c in ("Odd_CS_0x3_Lay","Odd_CS_2x2_Lay"): B[c]=pd.to_numeric(B[c],errors="coerce")
B["k"]=B["Date"]+"|"+B["Home"].apply(canon)+"|"+B["Away"].apply(canon)
M=V.merge(B[["k","Odd_CS_0x3_Lay","Odd_CS_2x2_Lay"]].drop_duplicates("k"),on="k",how="inner")
print("\njogos casados nas duas fontes (17/08 a 24/09): %d\n"%len(M))
for nome, ko, base in (("0x3","ko_0 - 3","Odd_CS_0x3_Lay"),("2x2","ko_2 - 2","Odd_CS_2x2_Lay")):
    d=M.dropna(subset=[ko,base])
    d=d[(d[ko]>1)&(d[base]>1)]
    r=d[ko]/d[base]
    print("=== LAY %s — odd no KO-10 vs odd da base ==="%nome)
    print("  n=%d | correlacao %.3f" % (len(d), d[[ko,base]].corr().iloc[0,1]))
    print("  mediana: base %.1f | KO-10 %.1f | razao KO/base: mediana %.3f | media %.3f"
          % (d[base].median(), d[ko].median(), r.median(), r.mean()))
    print("  a odd do KO-10 e MENOR que a da base em %.0f%% dos jogos" % (100*(r<1).mean()))
    print("  razao: p10 %.2f | p25 %.2f | p75 %.2f | p90 %.2f" % (r.quantile(.1),r.quantile(.25),r.quantile(.75),r.quantile(.9)))
    jan = (14,35) if nome=="0x3" else (8,20)
    nb=((d[base]>=jan[0])&(d[base]<=jan[1])).sum()
    nk=((d[ko]>=jan[0])&(d[ko]<=jan[1])).sum()
    amb=((d[base]>=jan[0])&(d[base]<=jan[1])&(d[ko]>=jan[0])&(d[ko]<=jan[1])).sum()
    print("  na janela %s: base %d | KO-10 %d | nos DOIS %d (%.0f%% dos da base)" % (jan, nb, nk, amb, 100*amb/max(nb,1)))
    print()
