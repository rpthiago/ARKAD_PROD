import os; import pandas as pd; import numpy as np; import joblib
from datetime import datetime

MODEL_PATH="modelo_Over_05_HT_Agressivo.pkl"; SCALER_PATH="scaler_Over_05_HT_Agressivo.pkl"
FEATURES_PATH="features_Over_05_HT_Agressivo.pkl"; LIGAS_PATH="ligas_Over_05_HT_Agressivo.txt"

def normalize_live_data(p):
    n={}; n['Home']=p.get('Home',''); n['Away']=p.get('Away',''); n['League']=p.get('League',''); n['Time']=p.get('Time','')
    n['Date']=pd.to_datetime(p.get('Date',datetime.now().date()))
    n['Odd_Over05_HT']=pd.to_numeric(p.get('Odd_Over05_HT',np.nan),errors='coerce')
    n['Odd_Under05_HT']=pd.to_numeric(p.get('Odd_Under05_HT',np.nan),errors='coerce')
    n['Odd_H_FT']=pd.to_numeric(p.get('Odd_H_FT',np.nan),errors='coerce')
    n['Odd_A_FT']=pd.to_numeric(p.get('Odd_A_FT',np.nan),errors='coerce')
    for c in ['Goals_H_HT','Goals_A_HT','xG_H_HT','xG_A_HT','Total_Shots_H_HT','Total_Shots_A_HT','Shots_On_Target_H_HT','Shots_On_Target_A_HT','xG_H_FT','xG_A_FT']:
        n[c]=pd.to_numeric(p.get(c,0.0),errors='coerce')
    return n

def check_entry_conditions(m):
    odd=m.get('Odd_Over05_HT',0.0)
    if pd.isna(odd) or odd<1.30 or odd>1.50: return False,"ODD"
    if m.get('Prob_ML',0.0)<0.60: return False,"PROB"
    if os.path.exists(LIGAS_PATH):
        with open(LIGAS_PATH,'r',encoding='utf-8') as f:
            if m.get('League','') not in [l.strip() for l in f.read().split(',') if l.strip()]: return False,"LIGA"
    return True,"OK"

def log_paper_trade(m):
    lf="paper_trading_log_over_05_ht.csv"
    row={'Date':str(m.get('Date','')),'League':m.get('League',''),'Home':m.get('Home',''),'Away':m.get('Away',''),
         'Odd_Over05_HT':m.get('Odd_Over05_HT',0.0),'Prob_ML':m.get('Prob_ML',0.0),'Decision':m.get('Decision',''),'Reason':m.get('Reason',''),
         'H_Scored_HT_5':m.get('H_Scored_HT_5',0.0),'A_Conceded_HT_5':m.get('A_Conceded_HT_5',0.0),'xG_HT_Diff':m.get('xG_HT_Diff',0.0),
         'Timestamp':datetime.now().isoformat()}
    dr=pd.DataFrame([row])
    if not os.path.exists(lf): dr.to_csv(lf,index=False)
    else: dr.to_csv(lf,mode='a',header=False,index=False)

def predict_and_evaluate_live(payload, df_hist):
    if not (os.path.exists(MODEL_PATH) and os.path.exists(SCALER_PATH) and os.path.exists(FEATURES_PATH)): return []
    model=joblib.load(MODEL_PATH); scaler=joblib.load(SCALER_PATH); features=joblib.load(FEATURES_PATH)
    dh=df_hist.copy(); dh['Date']=pd.to_datetime(dh['Date'],errors='coerce')
    dh=dh.dropna(subset=['Goals_H_HT','Goals_A_HT','Date','Home','Away']).copy()
    if payload: fd=pd.to_datetime(payload[0].get('Date',datetime.now().date())).date(); dh=dh[dh['Date'].dt.date<fd].copy()
    dh=dh.sort_values('Date').reset_index(drop=True)
    for c in ['Goals_H_HT','Goals_A_HT','xG_H_HT','xG_A_HT','Total_Shots_H_HT','Total_Shots_A_HT','Shots_On_Target_H_HT','Shots_On_Target_A_HT','xG_H_FT','xG_A_FT']:
        dh[c]=pd.to_numeric(dh[c],errors='coerce').fillna(0.0)

    df_h=dh[['Date','Home','Goals_H_HT','Goals_A_HT','xG_H_HT','xG_A_HT','Total_Shots_H_HT','Shots_On_Target_H_HT','xG_H_FT']].copy()
    df_h.columns=['Date','Team','Goals_Scored_HT','Goals_Conceded_HT','xG_Scored_HT','xG_Conceded_HT','Shots_HT','SoT_HT','xG_FT']; df_h['Is_Home']=1
    df_a=dh[['Date','Away','Goals_A_HT','Goals_H_HT','xG_A_HT','xG_H_HT','Total_Shots_A_HT','Shots_On_Target_A_HT','xG_A_FT']].copy()
    df_a.columns=['Date','Team','Goals_Scored_HT','Goals_Conceded_HT','xG_Scored_HT','xG_Conceded_HT','Shots_HT','SoT_HT','xG_FT']; df_a['Is_Home']=0

    dt=pd.concat([df_h,df_a],ignore_index=True).sort_values(['Team','Date']).reset_index(drop=True)
    dt['Home_Scored_HT_Flag']=((dt['Is_Home']==1)&(dt['Goals_Scored_HT']>=1)).astype(float)
    dt['H_Scored_HT_5']=dt.groupby('Team')['Home_Scored_HT_Flag'].transform(lambda x: x.shift(1).rolling(5,min_periods=1).mean())
    dt['Away_Conc_HT_Flag']=((dt['Is_Home']==0)&(dt['Goals_Conceded_HT']>=1)).astype(float)
    dt['A_Conceded_HT_5']=dt.groupby('Team')['Away_Conc_HT_Flag'].transform(lambda x: x.shift(1).rolling(5,min_periods=1).mean())
    dt['Over05_HT_Flag']=((dt['Goals_Scored_HT']+dt['Goals_Conceded_HT'])>=1).astype(float)
    dt['Over05_HT_Rate_5']=dt.groupby('Team')['Over05_HT_Flag'].transform(lambda x: x.shift(1).rolling(5,min_periods=1).mean())
    dt['Roll_Goals_HT_Scored_5']=dt.groupby('Team')['Goals_Scored_HT'].transform(lambda x: x.shift(1).rolling(5,min_periods=1).mean())
    dt['Roll_Goals_HT_Conceded_5']=dt.groupby('Team')['Goals_Conceded_HT'].transform(lambda x: x.shift(1).rolling(5,min_periods=1).mean())
    dt['Roll_xG_HT_Scored_5']=dt.groupby('Team')['xG_Scored_HT'].transform(lambda x: x.shift(1).rolling(5,min_periods=1).mean())
    dt['Roll_xG_HT_Conceded_5']=dt.groupby('Team')['xG_Conceded_HT'].transform(lambda x: x.shift(1).rolling(5,min_periods=1).mean())
    dt['Roll_SoT_HT_5']=dt.groupby('Team')['SoT_HT'].transform(lambda x: x.shift(1).rolling(5,min_periods=1).mean())
    dt['Roll_xG_FT_5']=dt.groupby('Team')['xG_FT'].transform(lambda x: x.shift(1).rolling(5,min_periods=1).mean())

    latest=dt.groupby('Team').last().reset_index(); ev=[]
    for g in payload:
        ng=normalize_live_data(g); h=ng['Home']; a=ng['Away']
        sh=latest[latest['Team']==h]; sa=latest[latest['Team']==a]
        if sh.empty or sa.empty: continue
        sh=sh.iloc[0]; sa=sa.iloc[0]
        ng['H_Scored_HT_5']=sh['H_Scored_HT_5']; ng['A_Conceded_HT_5']=sa['A_Conceded_HT_5']
        ng['H_Over05_HT_Rate_5']=sh['Over05_HT_Rate_5']; ng['A_Over05_HT_Rate_5']=sa['Over05_HT_Rate_5']
        ng['H_Goals_HT_5']=sh['Roll_Goals_HT_Scored_5']; ng['H_Conceded_HT_5']=sh['Roll_Goals_HT_Conceded_5']
        ng['A_Goals_HT_5']=sa['Roll_Goals_HT_Scored_5'];         ng['A_Goals_Conc_HT_5']=sa['Roll_Goals_HT_Conceded_5']
        ng['H_xG_HT_5']=sh['Roll_xG_HT_Scored_5']; ng['H_xG_Conc_HT_5']=sh['Roll_xG_HT_Conceded_5']
        ng['A_xG_HT_5']=sa['Roll_xG_HT_Scored_5']; ng['A_xG_Conc_HT_5']=sa['Roll_xG_HT_Conceded_5']
        ng['H_SoT_HT_5']=sh['Roll_SoT_HT_5']; ng['A_SoT_HT_5']=sa['Roll_SoT_HT_5']
        ng['H_xG_FT_5']=sh['Roll_xG_FT_5']; ng['A_xG_FT_5']=sa['Roll_xG_FT_5']

        ng['Over05_HT_Sum_5']=ng['H_Over05_HT_Rate_5']+ng['A_Over05_HT_Rate_5']
        ng['xG_HT_Diff']=ng['H_xG_HT_5']+ng['A_xG_Conc_HT_5']
        ng['Frenesi_Score']=ng['H_Scored_HT_5']*ng['A_Conceded_HT_5']
        ng['Goals_HT_Sum_5']=ng['H_Goals_HT_5']+ng['A_Goals_HT_5']
        ng['SoT_HT_Sum_5']=ng['H_SoT_HT_5']+ng['A_SoT_HT_5']
        ng['Defesa_Dormindo']=ng['A_Conceded_HT_5']/(ng['H_Scored_HT_5']+0.1)

        ng['Prob_Odd_Over05_HT']=1/(ng['Odd_Over05_HT']+1e-10)
        ng['Ratio_HA']=ng['Odd_H_FT']/(ng['Odd_A_FT']+1e-10)

        rm=pd.DataFrame([{col:ng.get(col,0.0) for col in features}])
        ng['Prob_ML']=model.predict_proba(scaler.transform(rm))[0,1]
        ap,r=check_entry_conditions(ng); ng['Decision']='APOSTA' if ap else 'SKIP'; ng['Reason']=r
        if ap: log_paper_trade(ng)
        ev.append(ng)
    return ev
