import io
import json
import warnings
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import requests
from pandas.errors import PerformanceWarning

from config import API_TOKEN


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "Arquivados_Apostas_Diarias" / "Backtests"
HEADERS = {"Authorization": f"Token {API_TOKEN}", "User-Agent": "Mozilla/5.0"}
HOME_BASE_SCORES = ((1, 0), (2, 0), (2, 1), (3, 0), (3, 1), (3, 2))
AWAY_BASE_SCORES = ((0, 1), (0, 2), (1, 2), (0, 3), (1, 3), (2, 3))

warnings.filterwarnings("ignore", category=PerformanceWarning)


def download_csv(url):
    response = requests.get(url, headers=HEADERS, timeout=120)
    response.raise_for_status()
    df = pd.read_csv(io.StringIO(response.text))
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    return df


def any_other_home_win(dataframe):
    goals_home = pd.to_numeric(dataframe["Goals_H_FT"], errors="coerce").fillna(-99).astype(int)
    goals_away = pd.to_numeric(dataframe["Goals_A_FT"], errors="coerce").fillna(-99).astype(int)
    base_scores = np.zeros(len(dataframe), dtype=bool)
    for score_home, score_away in HOME_BASE_SCORES:
        base_scores |= (goals_home == score_home) & (goals_away == score_away)
    return (goals_home > goals_away) & ~base_scores


def any_other_away_win(dataframe):
    goals_home = pd.to_numeric(dataframe["Goals_H_FT"], errors="coerce").fillna(-99).astype(int)
    goals_away = pd.to_numeric(dataframe["Goals_A_FT"], errors="coerce").fillna(-99).astype(int)
    base_scores = np.zeros(len(dataframe), dtype=bool)
    for score_home, score_away in AWAY_BASE_SCORES:
        base_scores |= (goals_home == score_home) & (goals_away == score_away)
    return (goals_away > goals_home) & ~base_scores


def criar_features_b365(dataframe):
    dataframe = dataframe.copy()
    odds_colunas = [col for col in dataframe.columns if "Odd_" in col]
    for col in odds_colunas:
        dataframe[col] = pd.to_numeric(dataframe[col], errors="coerce")
        dataframe[f"Prob_{col}"] = 1 / (dataframe[col] + 1e-10)
    if "Odd_H_FT" in dataframe.columns and "Odd_A_FT" in dataframe.columns:
        dataframe["Ratio_HA"] = dataframe["Odd_H_FT"] / (dataframe["Odd_A_FT"] + 1e-10)
    if "Odd_Over25_FT" in dataframe.columns and "Odd_Under25_FT" in dataframe.columns:
        dataframe["Ratio_OverUnder"] = dataframe["Odd_Over25_FT"] / (dataframe["Odd_Under25_FT"] + 1e-10)
    if "Odd_BTTS_Yes" in dataframe.columns and "Odd_BTTS_No" in dataframe.columns:
        dataframe["Ratio_BTTS"] = dataframe["Odd_BTTS_Yes"] / (dataframe["Odd_BTTS_No"] + 1e-10)
    return dataframe


def criar_features_betfair(dataframe):
    dataframe = dataframe.copy()
    odds_colunas = [col for col in dataframe.columns if "Odd_" in col]
    for col in odds_colunas:
        dataframe[col] = pd.to_numeric(dataframe[col], errors="coerce")
        dataframe[f"Prob_{col}"] = 1 / (dataframe[col] + 1e-10)
    for mercado in ["H", "D", "A", "Over25_FT", "Under25_FT", "Over15_FT", "Under15_FT", "Over05_FT", "Under05_FT", "BTTS_Yes", "BTTS_No"]:
        odd_back = f"Odd_{mercado}_Back"
        odd_lay = f"Odd_{mercado}_Lay"
        if odd_back in dataframe.columns and odd_lay in dataframe.columns:
            dataframe[f"Ratio_{mercado}"] = dataframe[odd_back] / (dataframe[odd_lay] + 1e-10)
    if "Odd_H_Back" in dataframe.columns and "Odd_A_Back" in dataframe.columns:
        dataframe["Dif_HA"] = dataframe["Odd_H_Back"] - dataframe["Odd_A_Back"]
        favorito_home = dataframe["Odd_H_Back"] <= dataframe["Odd_A_Back"]
        dataframe["Favorito_Home"] = favorito_home.astype(int)
        if "Odd_CS_Goleada_H_Lay" in dataframe.columns and "Odd_CS_Goleada_A_Lay" in dataframe.columns:
            dataframe["Odd_CS_Goleada_Favorito_Lay"] = np.where(
                favorito_home,
                pd.to_numeric(dataframe["Odd_CS_Goleada_H_Lay"], errors="coerce"),
                pd.to_numeric(dataframe["Odd_CS_Goleada_A_Lay"], errors="coerce"),
            )
    if "League" in dataframe.columns:
        dataframe = pd.concat([dataframe, pd.get_dummies(dataframe["League"], prefix="League")], axis=1)
    return dataframe


B365_METHODS = [
    {"scope": "B365", "name": "BTTS_Yes", "prefix": "BTTS_Yes", "label": "BTTS Yes B365", "odd_col": "Odd_BTTS_Yes", "min_prob": 0.65, "lay": False, "target": lambda df: np.where((df["Goals_H_FT"] > 0) & (df["Goals_A_FT"] > 0), 1, 0)},
    {"scope": "B365", "name": "BTTS_No", "prefix": "BTTS_No", "label": "BTTS No B365", "odd_col": "Odd_BTTS_No", "min_prob": 0.55, "lay": False, "target": lambda df: np.where((df["Goals_H_FT"] == 0) | (df["Goals_A_FT"] == 0), 1, 0)},
    {"scope": "B365", "name": "Under25_FT", "prefix": "Under25_FT", "label": "Under 2.5 FT B365", "odd_col": "Odd_Under25_FT", "min_prob": 0.80, "lay": False, "target": lambda df: np.where((df["Goals_H_FT"] + df["Goals_A_FT"]) < 3, 1, 0)},
    {"scope": "B365", "name": "Under15_FT", "prefix": "Under15_FT", "label": "Under 1.5 FT B365", "odd_col": "Odd_Under15_FT", "min_prob": 0.55, "lay": False, "target": lambda df: np.where((df["Goals_H_FT"] + df["Goals_A_FT"]) < 2, 1, 0)},
    {"scope": "B365", "name": "Over05_HT", "prefix": "Over05_HT", "label": "Over 0.5 HT B365", "odd_col": "Odd_Over05_HT", "min_prob": 0.65, "lay": False, "target": lambda df: np.where((df["Goals_H_HT"] + df["Goals_A_HT"]) > 0, 1, 0)},
    {"scope": "B365", "name": "Over15_FT", "prefix": "Over15_FT", "label": "Over 1.5 FT B365", "odd_col": "Odd_Over15_FT", "min_prob": 0.60, "lay": False, "target": lambda df: np.where((df["Goals_H_FT"] + df["Goals_A_FT"]) > 1, 1, 0)},
    {"scope": "B365", "name": "Lay_0x1", "prefix": "Lay_0x1", "label": "Lay 0x1 B365", "odd_col": "Odd_CS_0x1", "min_prob": 0.80, "lay": True, "target": lambda df: np.where((df["Goals_H_FT"] == 0) & (df["Goals_A_FT"] == 1), 0, 1)},
    {"scope": "B365", "name": "Lay_1x0", "prefix": "Lay_1x0", "label": "Lay 1x0 B365", "odd_col": "Odd_CS_1x0", "min_prob": 0.80, "lay": True, "target": lambda df: np.where((df["Goals_H_FT"] == 1) & (df["Goals_A_FT"] == 0), 0, 1)},
    {"scope": "B365", "name": "Back_Home", "prefix": "Back_Home", "label": "Back Home B365", "odd_col": "Odd_H_FT", "min_prob": 0.40, "lay": False, "target": lambda df: np.where(df["Goals_H_FT"] > df["Goals_A_FT"], 1, 0)},
    {"scope": "B365", "name": "Back_Away", "prefix": "Back_Away", "label": "Back Away B365", "odd_col": "Odd_A_FT", "min_prob": 0.30, "lay": False, "target": lambda df: np.where(df["Goals_A_FT"] > df["Goals_H_FT"], 1, 0)},
    {"scope": "B365", "name": "Over05_FT", "prefix": "Over05_FT", "label": "Over 0.5 FT B365", "odd_col": "Odd_Over05_FT", "min_prob": 0.70, "lay": False, "target": lambda df: np.where((df["Goals_H_FT"] + df["Goals_A_FT"]) > 0, 1, 0)},
    {"scope": "B365", "name": "Over25", "prefix": "Over25", "label": "Over 2.5 FT B365", "odd_col": "Odd_Over25_FT", "min_prob": 0.60, "lay": False, "target": lambda df: np.where((df["Goals_H_FT"] + df["Goals_A_FT"]) > 2, 1, 0)},
]


BETFAIR_METHODS = [
    {"scope": "Betfair", "name": "BTTS_Yes", "prefix": "BTTS_Yes_CORRIGIDO", "label": "BTTS Yes Betfair", "odd_col": "Odd_BTTS_Yes_Back", "min_prob": 0.55, "lay": False, "target": lambda df: np.where((df["Goals_H_FT"] > 0) & (df["Goals_A_FT"] > 0), 1, 0)},
    {"scope": "Betfair", "name": "Back_Home", "prefix": "Back_Home", "label": "Back Home Betfair", "odd_col": "Odd_H_Back", "min_prob": 0.40, "lay": False, "target": lambda df: np.where(df["Goals_H_FT"] > df["Goals_A_FT"], 1, 0)},
    {"scope": "Betfair", "name": "Back_Away", "prefix": "Back_Away", "label": "Back Away Betfair", "odd_col": "Odd_A_Back", "min_prob": 0.30, "lay": False, "target": lambda df: np.where(df["Goals_A_FT"] > df["Goals_H_FT"], 1, 0)},
    {"scope": "Betfair", "name": "Lay_Home", "prefix": "Lay_Home", "label": "Lay Home Betfair", "odd_col": "Odd_H_Lay", "min_prob": 0.55, "odd_min": 1.01, "odd_max": 10.0, "lay": True, "target": lambda df: np.where(df["Goals_H_FT"] <= df["Goals_A_FT"], 1, 0)},
    {"scope": "Betfair", "name": "Lay_Away", "prefix": "Lay_Away", "label": "Lay Away Betfair", "odd_col": "Odd_A_Lay", "min_prob": 0.65, "odd_min": 1.01, "odd_max": 15.0, "lay": True, "target": lambda df: np.where(df["Goals_A_FT"] <= df["Goals_H_FT"], 1, 0)},
    {"scope": "Betfair", "name": "Over25_FT", "prefix": "Over_2.5_FT_CORRIGIDO", "label": "Over 2.5 FT Betfair", "odd_col": "Odd_Over25_FT_Back", "min_prob": 0.65, "lay": False, "target": lambda df: np.where((df["Goals_H_FT"] + df["Goals_A_FT"]) > 2, 1, 0)},
    {"scope": "Betfair", "name": "Over15_FT", "prefix": "Over15_FT", "label": "Over 1.5 FT Betfair", "odd_col": "Odd_Over15_FT_Back", "min_prob": 0.58, "lay": False, "target": lambda df: np.where((df["Goals_H_FT"] + df["Goals_A_FT"]) > 1, 1, 0)},
    {"scope": "Betfair", "name": "Under25_FT", "prefix": "Under_2.5_FT_CORRIGIDO", "label": "Under 2.5 FT Betfair", "odd_col": "Odd_Under25_FT_Back", "min_prob": 0.60, "lay": False, "target": lambda df: np.where((df["Goals_H_FT"] + df["Goals_A_FT"]) < 3, 1, 0)},
    {"scope": "Betfair", "name": "Under15_FT", "prefix": "Under15_FT", "label": "Under 1.5 FT Betfair", "odd_col": "Odd_Under15_FT_Back", "min_prob": 0.55, "lay": False, "target": lambda df: np.where((df["Goals_H_FT"] + df["Goals_A_FT"]) < 2, 1, 0)},
    {"scope": "Betfair", "name": "BTTS_No", "prefix": "BTTS_No", "label": "BTTS No Betfair", "odd_col": "Odd_BTTS_No_Back", "min_prob": 0.52, "lay": False, "target": lambda df: np.where((df["Goals_H_FT"] == 0) | (df["Goals_A_FT"] == 0), 1, 0)},
    {"scope": "Betfair", "name": "Lay_Goleada_H", "prefix": "Lay_Goleada_H", "label": "Lay Goleada Home Betfair", "odd_col": "Odd_CS_Goleada_H_Lay", "min_prob": 0.90, "odd_min": 1.01, "odd_max": 40.0, "lay": True, "target": lambda df: np.where(~any_other_home_win(df), 1, 0)},
    {"scope": "Betfair", "name": "Lay_Goleada_A", "prefix": "Lay_Goleada_A", "label": "Lay Goleada Away Betfair", "odd_col": "Odd_CS_Goleada_A_Lay", "min_prob": 0.95, "odd_min": 1.01, "odd_max": 80.0, "lay": True, "target": lambda df: np.where(~any_other_away_win(df), 1, 0)},
    {"scope": "Betfair", "name": "Lay_Goleada_Favorito", "prefix": "Lay_Goleada_Favorito", "label": "Lay Goleada Favorito Betfair", "odd_col": "Odd_CS_Goleada_Favorito_Lay", "min_prob": 0.90, "odd_min": 1.01, "odd_max": 40.0, "lay": True, "target": lambda df: np.where(~np.where((pd.to_numeric(df["Odd_H_Back"], errors="coerce") <= pd.to_numeric(df["Odd_A_Back"], errors="coerce")), any_other_home_win(df), any_other_away_win(df)), 1, 0)},
    {"scope": "Betfair", "name": "Over05_FT", "prefix": "Over05_FT_v2", "label": "Over 0.5 FT Betfair", "odd_col": "Odd_Over05_FT_Back", "min_prob": 0.75, "lay": False, "target": lambda df: np.where((df["Goals_H_FT"] + df["Goals_A_FT"]) > 0, 1, 0)},
    {"scope": "Betfair", "name": "OVER25_BTTSYES", "prefix": "Over_2.5_FT", "label": "OVER25 BTTSYES Betfair", "odd_col": "Odd_Over25_FT_Back", "min_prob": 0.34, "lay": False, "target": lambda df: np.where((df["Goals_H_FT"] + df["Goals_A_FT"]) > 2, 1, 0)},
]


def carregar_ligas(prefix):
    for nome in (f"ligas_{prefix}.txt", f"ligas_boas_{prefix}.txt"):
        caminho = ROOT / nome
        if caminho.exists():
            return [item.strip() for item in caminho.read_text(encoding="utf-8").split(",") if item.strip()]
    return []


def resolver_arquivos_metodo(method):
    if method["scope"] == "B365":
        prefix = f"{method['prefix']}_b365"
        ligas = ROOT / f"ligas_{method['prefix']}_b365.txt"
    else:
        prefix = method["prefix"]
        ligas = None

    return {
        "model": ROOT / f"modelo_{prefix}.pkl",
        "scaler": ROOT / f"scaler_{prefix}.pkl",
        "features": ROOT / f"features_{prefix}.pkl",
        "ligas": ligas,
    }


def preparar_features(df_modelo, features, scaler):
    matriz = pd.DataFrame(
        {coluna: df_modelo[coluna] if coluna in df_modelo.columns else 0 for coluna in features},
        index=df_modelo.index,
    )
    matriz = matriz.apply(pd.to_numeric, errors="coerce").fillna(0).replace([np.inf, -np.inf], 0)

    if hasattr(scaler, "n_features_in_"):
        required = scaler.n_features_in_
        if matriz.shape[1] > required:
            matriz = matriz.iloc[:, :required]
        elif matriz.shape[1] < required:
            for index in range(required - matriz.shape[1]):
                matriz[f"pad_{index}"] = 0

    return matriz


def alinhar_saida_modelo(x_scaled, modelo):
    if hasattr(modelo, "n_features_in_"):
        required = modelo.n_features_in_
        if x_scaled.shape[1] > required:
            x_scaled = x_scaled[:, :required]
        elif x_scaled.shape[1] < required:
            padding = np.zeros((x_scaled.shape[0], required - x_scaled.shape[1]), dtype=np.float32)
            x_scaled = np.hstack([x_scaled, padding])
    return x_scaled


def run_methods(df_raw, methods, feature_builder, year):
    df_raw = df_raw.copy()
    df_raw = df_raw.dropna(subset=["Goals_H_FT", "Goals_A_FT"])
    df_raw["Goals_H_FT"] = pd.to_numeric(df_raw["Goals_H_FT"], errors="coerce")
    df_raw["Goals_A_FT"] = pd.to_numeric(df_raw["Goals_A_FT"], errors="coerce")
    if "Goals_H_HT" in df_raw.columns:
        df_raw["Goals_H_HT"] = pd.to_numeric(df_raw["Goals_H_HT"], errors="coerce")
    if "Goals_A_HT" in df_raw.columns:
        df_raw["Goals_A_HT"] = pd.to_numeric(df_raw["Goals_A_HT"], errors="coerce")

    df_year = df_raw[df_raw["Date"].dt.year == year].copy().sort_values("Date")
    if df_year.empty:
        return pd.DataFrame(), pd.DataFrame()

    df_feat = feature_builder(df_year)
    estatisticas = []
    picks = []
    skipped = []

    for method in methods:
        odd_col = method["odd_col"]
        if odd_col not in df_feat.columns:
            continue

        arquivos = resolver_arquivos_metodo(method)
        model_path = arquivos["model"]
        scaler_path = arquivos["scaler"]
        features_path = arquivos["features"]
        if not (model_path.exists() and scaler_path.exists() and features_path.exists()):
            skipped.append({
                "mercado": method["scope"],
                "metodo": method["label"],
                "ano": year,
                "motivo": f"Arquivos não encontrados: {model_path.name}, {scaler_path.name}, {features_path.name}",
            })
            continue

        try:
            modelo = joblib.load(model_path)
            scaler = joblib.load(scaler_path)
            if hasattr(scaler, "feature_names_in_"):
                features = list(scaler.feature_names_in_)
            else:
                features = joblib.load(features_path)
                if hasattr(scaler, "n_features_in_") and len(features) > scaler.n_features_in_:
                    features = features[:scaler.n_features_in_]
        except Exception as exc:
            skipped.append({
                "mercado": method["scope"],
                "metodo": method["label"],
                "ano": year,
                "motivo": str(exc),
            })
            continue

        if arquivos["ligas"] is not None and arquivos["ligas"].exists():
            ligas = [item.strip() for item in arquivos["ligas"].read_text(encoding="utf-8").split(",") if item.strip()]
        else:
            ligas = carregar_ligas(method["prefix"])

        df_modelo = df_feat.dropna(subset=[odd_col]).copy()
        if df_modelo.empty:
            continue

        df_modelo[odd_col] = pd.to_numeric(df_modelo[odd_col], errors="coerce")
        df_modelo = df_modelo[df_modelo[odd_col].notna()]
        if method.get("lay"):
            df_modelo = df_modelo[df_modelo[odd_col] <= 15.0]
        if df_modelo.empty:
            continue

        if "odd_min" in method:
            df_modelo = df_modelo[df_modelo[odd_col] > method["odd_min"]]
        if "odd_max" in method:
            df_modelo = df_modelo[df_modelo[odd_col] <= method["odd_max"]]
        if df_modelo.empty:
            continue

        df_modelo["Target"] = method["target"](df_modelo)

        x = preparar_features(df_modelo, features, scaler)
        x_scaled = scaler.transform(x.values.astype(np.float32))
        x_scaled = alinhar_saida_modelo(x_scaled, modelo)
        df_modelo["Prob_ML"] = modelo.predict_proba(x_scaled)[:, 1]

        mask = df_modelo["Prob_ML"] >= method["min_prob"]
        if ligas:
            mask &= df_modelo["League"].isin(ligas)

        if not method["lay"]:
            df_modelo["EV"] = (df_modelo["Prob_ML"] * df_modelo[odd_col]) - 1
            mask &= df_modelo["EV"] > 0

        sinais = df_modelo[mask].copy()
        if sinais.empty:
            continue

        if method["lay"]:
            if method["scope"] == "B365":
                sinais["Profit"] = np.where(sinais["Target"] == 1, 1.0, -((sinais[odd_col] * 1.10) - 1.0))
            else:
                sinais["Profit"] = np.where(sinais["Target"] == 1, 1.0 * (1 - 0.065), -(sinais[odd_col] - 1.0))
        else:
            sinais["Profit"] = np.where(sinais["Target"] == 1, sinais[odd_col] - 1.0, -1.0)

        sinais["CumProfit"] = sinais["Profit"].cumsum()
        sinais["Drawdown"] = sinais["CumProfit"] - sinais["CumProfit"].cummax()

        total_picks = len(sinais)
        lucro_total = float(sinais["Profit"].sum())
        roi = lucro_total / total_picks if total_picks else 0.0
        win_rate = float(sinais["Target"].mean()) if total_picks else 0.0
        odd_media = float(sinais[odd_col].mean()) if total_picks else 0.0
        max_drawdown = float(sinais["Drawdown"].min()) if total_picks else 0.0

        estatisticas.append({
            "mercado": method["scope"],
            "metodo": method["label"],
            "ano": year,
            "picks": total_picks,
            "win_rate": win_rate,
            "roi": roi,
            "lucro_total": lucro_total,
            "odd_media": odd_media,
            "max_drawdown": max_drawdown,
            "data_inicio": str(sinais["Date"].min().date()),
            "data_fim": str(sinais["Date"].max().date()),
        })

        colunas_export = [col for col in ["Date", "Time", "League", "Home", "Away"] if col in sinais.columns]
        colunas_export += [odd_col, "Prob_ML", "Target", "Profit", "CumProfit"]
        sinais_export = sinais[colunas_export].copy()
        sinais_export.insert(0, "metodo", method["label"])
        sinais_export.insert(1, "mercado", method["scope"])
        sinais_export.rename(columns={odd_col: "Odd_Operacao"}, inplace=True)
        picks.append(sinais_export)

    estatisticas_df = pd.DataFrame(estatisticas)
    if not estatisticas_df.empty:
        estatisticas_df = estatisticas_df.sort_values(["mercado", "lucro_total"], ascending=[True, False]).reset_index(drop=True)
    picks_df = pd.concat(picks, ignore_index=True) if picks else pd.DataFrame()
    skipped_df = pd.DataFrame(skipped)
    return estatisticas_df, picks_df, skipped_df


def executar():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    bet365 = download_csv("https://apicomunidade.futpythontrader.com/api/dados/bet365/download/")
    betfair = download_csv("https://apicomunidade.futpythontrader.com/api/dados/betfair/download/")

    resumo_geral = []
    arquivos = []

    for year in (2025, 2026):
        stats_b365, picks_b365, skipped_b365 = run_methods(bet365, B365_METHODS, criar_features_b365, year)
        stats_betfair, picks_betfair, skipped_betfair = run_methods(betfair, BETFAIR_METHODS, criar_features_betfair, year)

        stats_year = pd.concat([df for df in [stats_b365, stats_betfair] if not df.empty], ignore_index=True) if (not stats_b365.empty or not stats_betfair.empty) else pd.DataFrame()
        picks_year = pd.concat([df for df in [picks_b365, picks_betfair] if not df.empty], ignore_index=True) if (not picks_b365.empty or not picks_betfair.empty) else pd.DataFrame()
        skipped_year = pd.concat([df for df in [skipped_b365, skipped_betfair] if not df.empty], ignore_index=True) if (not skipped_b365.empty or not skipped_betfair.empty) else pd.DataFrame()

        output_file = OUTPUT_DIR / f"Backtest_Modelos_{year}_Realista.xlsx"
        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
            if not stats_year.empty:
                stats_year.to_excel(writer, sheet_name="Resumo", index=False)
            if not picks_year.empty:
                picks_year.to_excel(writer, sheet_name="Operacoes", index=False)
            if not skipped_year.empty:
                skipped_year.to_excel(writer, sheet_name="Modelos_Ignorados", index=False)
        arquivos.append(str(output_file))

        if not stats_year.empty:
            resumo = stats_year.groupby("mercado", as_index=False).agg(
                picks_total=("picks", "sum"),
                lucro_total=("lucro_total", "sum"),
                media_roi=("roi", "mean"),
                media_win_rate=("win_rate", "mean"),
            )
            resumo["ano"] = year
            resumo_geral.append(resumo)

    resumo_df = pd.concat(resumo_geral, ignore_index=True) if resumo_geral else pd.DataFrame()
    resumo_json = OUTPUT_DIR / "backtest_2025_2026_summary.json"
    resumo_json.write_text(resumo_df.to_json(orient="records", force_ascii=False), encoding="utf-8")
    arquivos.append(str(resumo_json))

    payload = {
        "summary": resumo_df.to_dict(orient="records"),
        "files": arquivos,
        "bet365_max_date": str(bet365["Date"].max().date()) if not bet365.empty else None,
        "betfair_max_date": str(betfair["Date"].max().date()) if not betfair.empty else None,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    executar()