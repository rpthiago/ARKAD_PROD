import os; import pandas as pd; import numpy as np; import joblib
from datetime import datetime

MODEL_PATH="modelo_Under_15_Agressivo.pkl"; SCALER_PATH="scaler_Under_15_Agressivo.pkl"
FEATURES_PATH="features_Under_15_Agressivo.pkl"; LIGAS_PATH="ligas_Under_15_Agressivo.txt"

def normalize_live_data(p):
    n={}; n['Home']=p.get('Home',''); n['Away']=p.get('Away',''); n['League']=p.get('League',''); n['Time']=p.get('Time','')
    n['Date']=pd.to_datetime(p.get('Date',datetime.now().date()))
    n['Odd_Under15_FT']=pd.to_numeric(p.get('Odd_Under15_FT',np.nan),errors='coerce')
    n['Odd_Over15_FT']=pd.to_numeric(p.get('Odd_Over15_FT',np.nan),errors='coerce')
    n['Odd_H_FT']=pd.to_numeric(p.get('Odd_H_FT',np.nan),errors='coerce')
    n['Odd_A_FT']=pd.to_numeric(p.get('Odd_A_FT',np.nan),errors='coerce')
    for c in ['Total_Shots_H_FT','Total_Shots_A_FT','Shots_On_Target_H_FT','Shots_On_Target_A_FT','xG_H_FT','xG_A_FT','Goals_H_FT','Goals_A_FT','Goals_H_HT','Goals_A_HT']:
        n[c]=pd.to_numeric(p.get(c,0.0),errors='coerce')
    return n

def check_entry_conditions(m):
    odd=m.get('Odd_Under15_FT',0.0)
    if pd.isna(odd) or odd<2.00 or odd>3.50: return False,"ODD"
    if m.get('Prob_ML',0.0)<0.60: return False,"PROB"
    if os.path.exists(LIGAS_PATH):
        with open(LIGAS_PATH,'r',encoding='utf-8') as f:
            if m.get('League','') not in [l.strip() for l in f.read().split(',') if l.strip()]: return False,"LIGA"
    return True,"OK"

def log_paper_trade(m):
    lf="paper_trading_log_under15.csv"
    row={'Date':str(m.get('Date','')),'League':m.get('League',''),'Home':m.get('Home',''),'Away':m.get('Away',''),
         'Odd_Under15_FT':m.get('Odd_Under15_FT',0.0),'Prob_ML':m.get('Prob_ML',0.0),'Decision':m.get('Decision',''),'Reason':m.get('Reason',''),
         'HT_0x0_Sum_5':m.get('HT_0x0_Sum_5',0.0),'Retranca_Score':m.get('Retranca_Score',0.0),'SoT_Sum_5':m.get('SoT_Sum_5',0.0),
         'Timestamp':datetime.now().isoformat()}
    dr=pd.DataFrame([row])
    if not os.path.exists(lf): dr.to_csv(lf,index=False)
    else: dr.to_csv(lf,mode='a',header=False,index=False)

def predict_and_evaluate_live(payload, df_hist):
    if not (os.path.exists(MODEL_PATH) and os.path.exists(SCALER_PATH) and os.path.exists(FEATURES_PATH)): return []
    model=joblib.load(MODEL_PATH); scaler=joblib.load(SCALER_PATH); features=joblib.load(FEATURES_PATH)
    dh=df_hist.copy(); dh['Date']=pd.to_datetime(dh['Date'],errors='coerce')
    dh=dh.dropna(subset=['Goals_H_FT','Goals_A_FT','Date','Home','Away']).copy()
    if payload: fd=pd.to_datetime(payload[0].get('Date',datetime.now().date())).date(); dh=dh[dh['Date'].dt.date<fd].copy()
    dh=dh.sort_values('Date').reset_index(drop=True)
    for c in ['Total_Shots_H_FT','Total_Shots_A_FT','Shots_On_Target_H_FT','Shots_On_Target_A_FT','xG_H_FT','xG_A_FT','Goals_H_FT','Goals_A_FT','Goals_H_HT','Goals_A_HT']:
        dh[c]=pd.to_numeric(dh[c],errors='coerce').fillna(0.0)

    df_h=dh[['Date','Home','Goals_H_FT','Goals_A_FT','Goals_H_HT','Goals_A_HT','Total_Shots_H_FT','Shots_On_Target_H_FT','xG_H_FT','xG_A_FT']].copy()
    df_h.columns=['Date','Team','Goals_Scored','Goals_Conceded','Goals_Scored_HT','Goals_Conceded_HT','Shots','SoT','xG_Scored','xG_Conceded']; df_h['Is_Home']=1
    df_a=dh[['Date','Away','Goals_A_FT','Goals_H_FT','Goals_A_HT','Goals_H_HT','Total_Shots_A_FT','Shots_On_Target_A_FT','xG_A_FT','xG_H_FT']].copy()
    df_a.columns=['Date','Team','Goals_Scored','Goals_Conceded','Goals_Scored_HT','Goals_Conceded_HT','Shots','SoT','xG_Scored','xG_Conceded']; df_a['Is_Home']=0

    dt=pd.concat([df_h,df_a],ignore_index=True).sort_values(['Team','Date']).reset_index(drop=True)
    dt['HT_0x0_Flag']=((dt['Goals_Scored_HT']==0)&(dt['Goals_Conceded_HT']==0)).astype(float)
    dt['HT_0x0_Rate_5']=dt.groupby('Team')['HT_0x0_Flag'].rolling(5,min_periods=1).mean().reset_index(level=0,drop=True)
    dt['Under15_Flag']=((dt['Goals_Scored']+dt['Goals_Conceded'])<2).astype(float)
    dt['Under15_Rate_5']=dt.groupby('Team')['Under15_Flag'].rolling(5,min_periods=1).mean().reset_index(level=0,drop=True)
    dt['Under25_Flag']=((dt['Goals_Scored']+dt['Goals_Conceded'])<3).astype(float)
    dt['Under25_Rate_5']=dt.groupby('Team')['Under25_Flag'].rolling(5,min_periods=1).mean().reset_index(level=0,drop=True)
    dt['Roll_Goals_Scored_5']=dt.groupby('Team')['Goals_Scored'].rolling(5,min_periods=1).mean().reset_index(level=0,drop=True)
    dt['Roll_Goals_Conceded_5']=dt.groupby('Team')['Goals_Conceded'].rolling(5,min_periods=1).mean().reset_index(level=0,drop=True)
    dt['Roll_SoT_5']=dt.groupby('Team')['SoT'].rolling(5,min_periods=1).mean().reset_index(level=0,drop=True)
    dt['Roll_xG_5']=dt.groupby('Team')['xG_Scored'].rolling(5,min_periods=1).mean().reset_index(level=0,drop=True)
    dt['FT_0x0_Flag']=((dt['Goals_Scored']==0)&(dt['Goals_Conceded']==0)).astype(float)
    dt['FT_0x0_Rate_5']=dt.groupby('Team')['FT_0x0_Flag'].rolling(5,min_periods=1).mean().reset_index(level=0,drop=True)

    latest=dt.groupby('Team').last().reset_index(); ev=[]
    for g in payload:
        ng=normalize_live_data(g); h=ng['Home']; a=ng['Away']
        sh=latest[latest['Team']==h]; sa=latest[latest['Team']==a]
        if sh.empty or sa.empty: continue
        sh=sh.iloc[0]; sa=sa.iloc[0]
        ng['H_HT_0x0_Rate_5']=sh['HT_0x0_Rate_5']; ng['A_HT_0x0_Rate_5']=sa['HT_0x0_Rate_5']
        ng['H_Under15_Rate_5']=sh['Under15_Rate_5']; ng['A_Under15_Rate_5']=sa['Under15_Rate_5']
        ng['H_Under25_Rate_5']=sh['Under25_Rate_5']; ng['A_Under25_Rate_5']=sa['Under25_Rate_5']
        ng['H_Goals_Scored_5']=sh['Roll_Goals_Scored_5']; ng['H_Goals_Conceded_5']=sh['Roll_Goals_Conceded_5']
        ng['A_Goals_Scored_5']=sa['Roll_Goals_Scored_5']; ng['A_Goals_Conceded_5']=sa['Roll_Goals_Conceded_5']
        ng['H_SoT_5']=sh['Roll_SoT_5']; ng['A_SoT_5']=sa['Roll_SoT_5']
        ng['H_xG_5']=sh['Roll_xG_5']; ng['A_xG_5']=sa['Roll_xG_5']
        ng['H_FT_0x0_Rate_5']=sh['FT_0x0_Rate_5']; ng['A_FT_0x0_Rate_5']=sa['FT_0x0_Rate_5']

        ng['HT_0x0_Sum_5']=ng['H_HT_0x0_Rate_5']+ng['A_HT_0x0_Rate_5']
        ng['Under15_Sum_5']=ng['H_Under15_Rate_5']+ng['A_Under15_Rate_5']
        ng['SoT_Sum_5']=ng['H_SoT_5']+ng['A_SoT_5']
        ng['xG_Sum_5']=ng['H_xG_5']+ng['A_xG_5']
        ng['Goals_Sum_5']=ng['H_Goals_Scored_5']+ng['A_Goals_Scored_5']
        ng['FT_0x0_Sum_5']=ng['H_FT_0x0_Rate_5']+ng['A_FT_0x0_Rate_5']
        ng['Retranca_Score']=(ng['HT_0x0_Sum_5']*ng['Under15_Sum_5'])/(ng['SoT_Sum_5']+1)

        ng['Prob_Odd_Under15_FT']=1/(ng['Odd_Under15_FT']+1e-10)
        ng['Prob_Odd_Over15_FT']=1/(ng['Odd_Over15_FT']+1e-10)
        ng['Ratio_UO']=ng['Odd_Under15_FT']/(ng['Odd_Over15_FT']+1e-10)
        ng['Ratio_HA']=ng['Odd_H_FT']/(ng['Odd_A_FT']+1e-10)

        rm=pd.DataFrame([{col:ng.get(col,0.0) for col in features}])
        ng['Prob_ML']=model.predict_proba(scaler.transform(rm))[0,1]
        ap,r=check_entry_conditions(ng); ng['Decision']='APOSTA' if ap else 'SKIP'; ng['Reason']=r
        if ap: log_paper_trade(ng)
        ev.append(ng)
    return ev
