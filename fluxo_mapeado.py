# -*- coding: utf-8 -*-
"""Fluxo REAL para frente: so ligas do football-data produzem sinal. E qual a razao mediana
por faixa restrita a regiao operacional (lay <= 7,00)?"""
import re, sys, unicodedata
import numpy as np, pandas as pd
FAV, TETO, PISO = 1.40, 7.00, 200.0
BANDAS = [2.5, 3.5, 4.5, 6.0, 8.0, 12.0, 20.0, 1e9]
d = pd.read_csv(sys.argv[1])
for c in ["min_to_ko","back","back_size","lay","lay_size"]: d[c]=pd.to_numeric(d[c],errors="coerce")
d["jogo"]=d["competition"]+"|"+d["home"]+"|"+d["away"]+"|"+d["ko"]
d["dist"]=(d["min_to_ko"]-10.0).abs()
d=d.sort_values("dist").groupby(["jogo","lado"],as_index=False).first()
p=d.pivot_table(index=["jogo","competition","home","away","ko"],columns="lado",
                values=["back","lay","lay_size"],aggfunc="first").reset_index()
p.columns=["_".join([str(x) for x in c if x!=""]) for c in p.columns]
p=p.dropna(subset=["back_H","back_A","back_D","lay_D","lay_size_D"])
p["fav"]=p[["back_H","back_A"]].min(axis=1)
p=p[(p["back_D"]>1.01)&(p["lay_D"]>=p["back_D"])&(p["lay_D"]<1000)]
r=p[p["fav"]<=FAV].copy(); r["razao"]=r["lay_D"]/r["back_D"]
r["data"]=pd.to_datetime(r["ko"],errors="coerce"); r=r.dropna(subset=["data"])
def norm(s):
    s=unicodedata.normalize("NFKD",str(s)).encode("ascii","ignore").decode()
    s=re.sub(r"[^a-z0-9 ]"," ",s.lower())
    for w in ["fc","cf","sc","ac","afc","cd","ca","club","de","do","da"]: s=re.sub(r"\b%s\b"%w," ",s)
    return re.sub(r"\s+"," ",s).strip()
fd=pd.read_csv(sys.argv[2],low_memory=False)
fd["dia"]=pd.to_datetime(fd["Date"],errors="coerce").dt.date
fd["k"]=fd["dia"].astype(str)+"|"+fd["Home"].map(norm)+"|"+fd["Away"].map(norm)
p["dia"]=pd.to_datetime(p["ko"],errors="coerce").dt.date
p["k"]=p["dia"].astype(str)+"|"+p["home"].map(norm)+"|"+p["away"].map(norm)
cj=p.merge(fd[["k","liga"]].dropna().drop_duplicates("k"),on="k",how="inner")
nj=cj.groupby("competition")["liga"].size()
cw=cj.groupby("competition")["liga"].agg(lambda s:s.value_counts().idxmax())[nj>=3]
r["liga_fd"]=r["competition"].map(cw)
m=r[r["liga_fd"].notna()].copy()
dias=(r["data"].max()-r["data"].min()).days or 1
print("janela: %s a %s (%d dias)\n"%(r["data"].min().date(),r["data"].max().date(),dias))
print("### FLUXO restrito as ligas do football-data (as unicas que geram sinal p/ frente)")
for rot,sub in [("Fav<=1.40, ligas mapeadas",m),
                ("+ teto lay <= 7.00",m[m["lay_D"]<=TETO]),
                ("+ spread lay/back <= 1.15",m[(m["lay_D"]<=TETO)&(m["razao"]<=1.15)])]:
    mes=30.0*len(sub)/dias
    print("  %-32s N=%4d -> %5.1f sinais/mes | N=250 em %4.1f meses"%(rot,len(sub),mes,250.0/max(.1,mes)))
print("\n### RAZAO MEDIANA por faixa, NA REGIAO OPERACIONAL (lay<=7.00), universo da regra")
op=r[r["lay_D"]<=TETO].copy()
rot=["%g-%g"%(BANDAS[i],BANDAS[i+1]) if BANDAS[i+1]<1e8 else "%g+"%BANDAS[i] for i in range(len(BANDAS)-1)]
op["faixa"]=pd.cut(op["back_D"],bins=BANDAS,labels=rot,right=False)
print("  %-10s %6s %10s %9s %9s %8s"%("faixa","N","razao med","p25","p75","P(liq200)"))
tab={}
for f in rot:
    s=op[op["faixa"]==f]
    if len(s)<20: continue
    tab[f]=s["razao"].median()
    print("  %-10s %6d %10.4f %9.4f %9.4f %7.0f%%"%(f,len(s),s["razao"].median(),
          s["razao"].quantile(.25),s["razao"].quantile(.75),100*(s["lay_size_D"]>=PISO).mean()))
print("\n  tabela congelada (faixa -> razao): %s"%{k:round(v,4) for k,v in tab.items()})
import hashlib
blob="|".join("%s=%.4f"%(k,v) for k,v in sorted(tab.items()))
print("  SHA256: %s"%hashlib.sha256(blob.encode()).hexdigest()[:32])
