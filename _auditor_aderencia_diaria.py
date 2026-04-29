"""
Auditoria diaria de aderencia: jogos do dia (Apostas_YYYYMMDD.xlsx)
vs referencia de backtest (recalculo_sem_combos_usuario.csv + filtros da config).

Gera automaticamente:
1) tabela de match por dia (match exato + match fuzzy);
2) lista de divergencias com motivo detalhado;
3) semaforo (verde/amarelo/vermelho) diario e geral.

Motivos de divergencia:
- match_exato          : chave identica (data|liga|metodo|home|away)
- nome_diferente_fuzzy : mesmo jogo, nome ligeiramente diferente na API vs base
- liga_fora_do_universo: liga nao mapeada na config_universo_97 (aposta sem historico!)
- nao_veio_na_base     : jogo da API sem correspondente na base (liga nova ou sazonalidade)
- nao_veio_no_live     : jogo do backtest que nao apareceu no dia (cancelado/horario)
- fora_da_liga / fora_faixa_odd / metodo_nao_mapeado: filtros nao respeitados
"""

from __future__ import annotations

import json
import re
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
CFG_PATH = ROOT / "config_universo_97.json"
BASE_PATH = ROOT / "recalculo_sem_combos_usuario.csv"
DIARIO_DIR = ROOT / "Apostas_Diarias"
OUT_DIR = ROOT / "Arquivados_Apostas_Diarias" / "Relatorios" / "Comparativo_Automatizado" / "Auditoria_Aderencia_Diaria"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Prefixos e sufixos de clube para remover na normalização fuzzy
_CLUB_PREFIXES = [
    "red bull ", "rb ", "1. ", "1.", "fc ", "fk ", "sk ", "sc ", "ac ",
    "ss ", "us ", "sv ", "gd ", "as ", "cf ", "bk ", "hb ", "if ",
    "aik ", "il ", "ik ", "bk ", "nk ", "ok ", "rk ", "sd ", "cd ",
    "ud ", "sd ", "rcd ", "rc ", "ca ", "sa ", "se ", "ec ", "cr ",
    "esporte clube ", "atletico ", "atletico-",
]
_CLUB_SUFFIXES = [
    " fc", " fk", " sc", " sk", " ac", " cf", " bk", " if",
    " ec", " se", " cr", " ca",
    " (per)", " (chi)", " (col)", " (uru)", " (arg)", " (bra)",
    "-sc", "-fc",
]
# Abreviações de estado/cidade frequentes na base brasileira
_STATE_ABBR_RE = re.compile(r"\b(mg|rj|sp|sc|rs|pr|ba|ce|go|pe|am)\b")
FUZZY_SIMILARITY_THRESHOLD = 0.80


def _strip_accents(text: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn"
    )


def _normalize_text(text: object) -> str:
    """Normalização leve — usada para chave exata."""
    if not isinstance(text, str):
        return ""
    out = text.lower().strip()
    out = re.sub(r"\s+", " ", out)
    out = out.replace("fc ", "")
    out = out.replace(" cf", "")
    out = out.replace(" (per)", "")
    out = out.replace(" (chi)", "")
    out = out.replace(" (col)", "")
    out = out.replace(" (uru)", "")
    return out.strip()


def _fuzzy_team(name: object) -> str:
    """Normalização agressiva para matching fuzzy de nomes de clube."""
    if not isinstance(name, str):
        return ""
    out = _strip_accents(name.lower().strip())
    # substitui hifens por espaço
    out = out.replace("-", " ")
    # remove pontuação exceto espaços
    out = re.sub(r"[^\w\s]", " ", out)
    # remove abreviações de estado BR
    out = _STATE_ABBR_RE.sub("", out)
    # remove prefixos de clube
    for prefix in _CLUB_PREFIXES:
        if out.startswith(prefix):
            out = out[len(prefix):]
            break
    # remove sufixos de clube
    for suffix in _CLUB_SUFFIXES:
        if out.endswith(suffix):
            out = out[: -len(suffix)]
            break
    return re.sub(r"\s+", " ", out).strip()


def _teams_similar(a: str, b: str) -> bool:
    """Retorna True se dois nomes de time são suficientemente similares."""
    fa, fb = _fuzzy_team(a), _fuzzy_team(b)
    if fa == fb:
        return True
    # um é substring do outro (cobre "Atletico MG" vs "Atletico")
    if fa in fb or fb in fa:
        return True
    ratio = SequenceMatcher(None, fa, fb).ratio()
    return ratio >= FUZZY_SIMILARITY_THRESHOLD


def _split_match(jogo: object) -> tuple[str, str]:
    if not isinstance(jogo, str):
        return "", ""
    norm = jogo.replace(" VS ", " x ").replace(" vs ", " x ").replace(" Vs ", " x ")
    parts = [p.strip() for p in norm.split(" x ")]
    if len(parts) >= 2:
        return _normalize_text(parts[0]), _normalize_text(parts[1])
    return _normalize_text(norm), ""


def _split_fuzzy(jogo: object) -> tuple[str, str]:
    if not isinstance(jogo, str):
        return "", ""
    norm = jogo.replace(" VS ", " x ").replace(" vs ", " x ").replace(" Vs ", " x ")
    parts = [p.strip() for p in norm.split(" x ")]
    if len(parts) >= 2:
        return _fuzzy_team(parts[0]), _fuzzy_team(parts[1])
    return _fuzzy_team(norm), ""


def _build_rodo_list(cfg: dict) -> list[dict]:
    merged: dict[int, dict] = {}
    for r in cfg.get("filtros_rodo", []):
        if "id" in r:
            merged[int(r["id"])] = r
    for r in cfg.get("filters", {}).get("filtros_rodo", []):
        if "id" in r:
            merged[int(r["id"])] = r
    for r in cfg.get("filters", {}).get("toxic_cuts", []):
        if "id" in r:
            merged[int(r["id"])] = r
    return list(merged.values())


def _passes_method_filters(row: pd.Series, filtros_metodo: dict) -> tuple[bool, str]:
    metodo = str(row.get("Metodo", "")).strip()
    liga = str(row.get("Liga", "")).strip().upper()
    odd = pd.to_numeric(row.get("Odd_Base"), errors="coerce")

    if metodo not in filtros_metodo:
        return False, "metodo_nao_mapeado"

    fm = filtros_metodo.get(metodo, {})
    if pd.isna(odd):
        return False, "odd_ausente"

    odd_min = fm.get("odd_min")
    odd_max = fm.get("odd_max")
    if odd_min is not None and float(odd) < float(odd_min):
        return False, "fora_faixa_odd"
    if odd_max is not None and float(odd) > float(odd_max):
        return False, "fora_faixa_odd"

    ligas_ok = {str(x).strip().upper() for x in fm.get("ligas_permitidas", [])}
    if ligas_ok and liga not in ligas_ok:
        return False, "fora_da_liga"

    return True, "ok"


def _match_rodo(row: pd.Series, rodos: list[dict]) -> tuple[bool, str]:
    liga = str(row.get("Liga", "")).strip()
    metodo = str(row.get("Metodo", "")).strip()
    odd = pd.to_numeric(row.get("Odd_Base"), errors="coerce")
    if pd.isna(odd):
        return False, ""

    for r in rodos:
        if r.get("league") and str(r.get("league")) != liga:
            continue
        if r.get("method_equals") and str(r.get("method_equals")) != metodo:
            continue
        rmin = r.get("odd_min")
        rmax = r.get("odd_max")
        if rmin is not None and float(odd) < float(rmin):
            continue
        if rmax is not None and float(odd) > float(rmax):
            continue
        return True, str(r.get("name", f"Rodo_{r.get('id', 'NA')}"))

    return False, ""


def _semaforo(match_rate: float) -> str:
    if match_rate >= 90:
        return "verde"
    if match_rate >= 70:
        return "amarelo"
    return "vermelho"


def _load_base() -> pd.DataFrame:
    base = pd.read_csv(BASE_PATH)
    base["Data"] = pd.to_datetime(base["Data_Arquivo"], errors="coerce").dt.date
    base["Liga"] = base["Liga"].astype(str).str.strip()
    base["Metodo"] = base["Metodo"].astype(str).str.strip()
    base["Odd_Base"] = pd.to_numeric(base["Odd_Base"], errors="coerce")
    base[["home_norm", "away_norm"]] = base["Jogo"].apply(lambda s: pd.Series(_split_match(s)))
    base[["home_fuzzy", "away_fuzzy"]] = base["Jogo"].apply(lambda s: pd.Series(_split_fuzzy(s)))
    base["key"] = base.apply(
        lambda r: f"{r['Data']}|{r['Liga']}|{r['Metodo']}|{r['home_norm']}|{r['away_norm']}",
        axis=1,
    )
    base["key_no_method"] = base.apply(
        lambda r: f"{r['Data']}|{r['Liga']}|{r['home_norm']}|{r['away_norm']}",
        axis=1,
    )
    base["key_fuzzy"] = base.apply(
        lambda r: f"{r['Data']}|{r['Liga']}|{r['Metodo']}|{r['home_fuzzy']}|{r['away_fuzzy']}",
        axis=1,
    )
    return base


def _load_live_from_xlsx() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for p in sorted(DIARIO_DIR.glob("Apostas_*.xlsx")):
        stamp = p.stem.replace("Apostas_", "")
        if len(stamp) != 8 or not stamp.isdigit():
            continue
        dt = pd.to_datetime(stamp, format="%Y%m%d", errors="coerce")
        if pd.isna(dt):
            continue

        x = pd.read_excel(p)
        if x.empty:
            continue

        x["Data"] = dt.date()
        x["Liga"] = x.get("Liga", "").astype(str).str.strip()
        x["Metodo"] = x.get("Metodo", "").astype(str).str.strip()
        if "Odd_Base" in x.columns:
            x["Odd_Base"] = pd.to_numeric(x["Odd_Base"], errors="coerce")
        else:
            x["Odd_Base"] = pd.NA
        x[["home_norm", "away_norm"]] = x["Jogo"].apply(lambda s: pd.Series(_split_match(s)))
        x[["home_fuzzy", "away_fuzzy"]] = x["Jogo"].apply(lambda s: pd.Series(_split_fuzzy(s)))
        x["key"] = x.apply(
            lambda r: f"{r['Data']}|{r['Liga']}|{r['Metodo']}|{r['home_norm']}|{r['away_norm']}",
            axis=1,
        )
        x["key_no_method"] = x.apply(
            lambda r: f"{r['Data']}|{r['Liga']}|{r['home_norm']}|{r['away_norm']}",
            axis=1,
        )
        x["key_fuzzy"] = x.apply(
            lambda r: f"{r['Data']}|{r['Liga']}|{r['Metodo']}|{r['home_fuzzy']}|{r['away_fuzzy']}",
            axis=1,
        )
        x["arquivo_origem"] = p.name
        frames.append(x)

    if not frames:
        return pd.DataFrame(columns=["Data", "Liga", "Metodo", "Jogo", "Odd_Base", "key", "key_no_method", "arquivo_origem"])

    return pd.concat(frames, ignore_index=True)


def _find_fuzzy_match(
    live_row: pd.Series,
    base_df: pd.DataFrame,
    ligas_u97: set[str],
) -> tuple[bool, str]:
    """
    Tenta casar live_row com alguma linha da base para a mesma data+liga+metodo
    usando similaridade fuzzy de nomes de time.
    Retorna (matched, motivo_se_nao_match).
    """
    d = live_row["Data"]
    liga = str(live_row.get("Liga", "")).strip()
    metodo = str(live_row.get("Metodo", "")).strip()

    cands = base_df[(base_df["Data"] == d) & (base_df["Liga"] == liga) & (base_df["Metodo"] == metodo)]
    if cands.empty:
        # tenta sem metodo
        cands = base_df[(base_df["Data"] == d) & (base_df["Liga"] == liga)]

    live_home = live_row.get("home_fuzzy", "")
    live_away = live_row.get("away_fuzzy", "")

    for _, base_row in cands.iterrows():
        base_home = base_row.get("home_fuzzy", "")
        base_away = base_row.get("away_fuzzy", "")
        if _teams_similar(live_home, base_home) and _teams_similar(live_away, base_away):
            return True, "nome_diferente_fuzzy_match"

    # Não achou na base — classifica o motivo
    if liga not in ligas_u97:
        return False, "liga_fora_do_universo"
    return False, "nao_veio_na_base"


def main() -> None:
    cfg = json.loads(CFG_PATH.read_text(encoding="utf-8"))
    filtros_metodo = cfg.get("runtime_data", {}).get("filtros_metodo", {})
    rodos = _build_rodo_list(cfg)

    # Ligas do universo 97 para classificar "liga_fora_do_universo"
    ligas_u97: set[str] = set()
    for fm in filtros_metodo.values():
        for l in fm.get("ligas_permitidas", []):
            ligas_u97.add(str(l).strip())

    base = _load_base()
    live = _load_live_from_xlsx()

    if live.empty:
        print("Nenhuma planilha Apostas_*.xlsx encontrada em Apostas_Diarias.")
        return

    common_dates = sorted(set(base["Data"].dropna()) & set(live["Data"].dropna()))
    if not common_dates:
        print("Nao existem datas em comum entre base e planilhas diarias.")
        return

    base_c = base[base["Data"].isin(common_dates)].copy()
    live_c = live[live["Data"].isin(common_dates)].copy()

    base_keys_exact = set(base_c["key"])
    live_keys_exact = set(live_c["key"])
    inter_exact = base_keys_exact & live_keys_exact

    base_keys_fuzzy = set(base_c["key_fuzzy"])
    live_keys_fuzzy = set(live_c["key_fuzzy"])
    inter_fuzzy_only = (base_keys_fuzzy & live_keys_fuzzy) - {
        # converte exact matches para key_fuzzy para excluir do fuzzy-only
        k for k in inter_exact
    }

    # Divergencias do lado live — jogos que nao bateram na chave exata
    only_live_df = live_c[~live_c["key"].isin(inter_exact)].copy()
    motivo_live: list[str] = []
    rodo_live: list[str] = []
    nome_base_live: list[str] = []

    for _, r in only_live_df.iterrows():
        # Nível 1: fuzzy key (data|liga|metodo|home_fuzzy|away_fuzzy)
        if r["key_fuzzy"] in base_keys_fuzzy:
            motivo_live.append("nome_diferente_fuzzy_match")
            rodo_live.append("")
            nome_base_live.append("")
            continue

        # Nível 2: busca por similaridade de strings
        fuzzy_matched, fuzzy_motivo = _find_fuzzy_match(r, base_c, ligas_u97)
        if fuzzy_matched:
            motivo_live.append(fuzzy_motivo)
            rodo_live.append("")
            nome_base_live.append("")
            continue

        # Não achou nem fuzzy — classifica
        liga = str(r.get("Liga", "")).strip()
        if liga not in ligas_u97:
            motivo_live.append("liga_fora_do_universo")
            rodo_live.append("")
            nome_base_live.append("")
            continue

        ok_metodo, mot_metodo = _passes_method_filters(r, filtros_metodo)
        if not ok_metodo:
            motivo_live.append(mot_metodo)
            rodo_live.append("")
            nome_base_live.append("")
            continue

        blocked, rodo_name = _match_rodo(r, rodos)
        if blocked:
            motivo_live.append("bloqueado_rodo_no_backtest")
            rodo_live.append(rodo_name)
            nome_base_live.append("")
            continue

        motivo_live.append("nao_veio_na_base")
        rodo_live.append("")
        nome_base_live.append("")

    only_live_df = only_live_df.copy()
    only_live_df["lado"] = "somente_live"
    only_live_df["motivo"] = motivo_live
    only_live_df["rodo"] = rodo_live

    # Divergencias do lado base — jogos do backtest que nao apareceram no dia
    only_base_df = base_c[~base_c["key"].isin(inter_exact)].copy()
    motivo_base: list[str] = []
    for _, r in only_base_df.iterrows():
        if r["key_fuzzy"] in live_keys_fuzzy:
            motivo_base.append("nome_diferente_fuzzy_match")
            continue
        fuzzy_matched, _ = _find_fuzzy_match(r, live_c, ligas_u97)
        if fuzzy_matched:
            motivo_base.append("nome_diferente_fuzzy_match")
            continue
        has_any_live = bool(
            ((live_c["Data"] == r["Data"]) & (live_c["key_no_method"] == r["key_no_method"])).any()
        )
        if has_any_live:
            motivo_base.append("metodo_ou_liga_divergente_no_live")
        else:
            motivo_base.append("nao_veio_no_live")

    only_base_df = only_base_df.copy()
    only_base_df["lado"] = "somente_base"
    only_base_df["motivo"] = motivo_base
    only_base_df["rodo"] = ""

    # Tabela diária com match exato + fuzzy
    rows_daily = []
    for d in common_dates:
        b_exact = set(base_c.loc[base_c["Data"] == d, "key"])
        l_exact = set(live_c.loc[live_c["Data"] == d, "key"])
        b_fuzzy = set(base_c.loc[base_c["Data"] == d, "key_fuzzy"])
        l_fuzzy = set(live_c.loc[live_c["Data"] == d, "key_fuzzy"])
        i_exact = len(b_exact & l_exact)
        i_fuzzy = len((b_fuzzy & l_fuzzy)) - i_exact  # somente fuzzy (não exato)
        i_total = i_exact + i_fuzzy
        live_n = len(l_exact)
        base_n = len(b_exact)
        match_pct = (i_total / live_n * 100.0) if live_n else 0.0
        rows_daily.append(
            {
                "Data": d,
                "live_total": live_n,
                "base_total": base_n,
                "match_exato": i_exact,
                "match_fuzzy": i_fuzzy,
                "match_total": i_total,
                "precision_total_pct": round(match_pct, 2),
                "semaforo": _semaforo(match_pct),
            }
        )

    daily_df = pd.DataFrame(rows_daily).sort_values("Data")

    # Concat divergências
    div_cols = [
        "Data", "lado", "motivo", "rodo",
        "Liga", "Metodo", "Jogo", "Odd_Base",
        "arquivo_origem", "key",
    ]
    diverg_df = pd.concat(
        [
            only_live_df.reindex(columns=div_cols),
            only_base_df.reindex(columns=div_cols),
        ],
        ignore_index=True,
    )

    # Métricas globais usando match total (exato + fuzzy)
    total_live = len(live_keys_exact)
    total_base = len(base_keys_exact)
    total_exact = len(inter_exact)
    total_fuzzy_only = len(base_keys_fuzzy & live_keys_fuzzy) - total_exact
    total_matched = total_exact + max(0, total_fuzzy_only)
    precision_global = (total_matched / total_live * 100.0) if total_live else 0.0
    recall_global = (total_matched / total_base * 100.0) if total_base else 0.0
    semaforo_global = _semaforo(precision_global)

    motivo_counts = (
        diverg_df["motivo"].value_counts(dropna=False).rename_axis("motivo").reset_index(name="qtd")
    )

    summary = {
        "periodo_comum_inicio": str(common_dates[0]),
        "periodo_comum_fim": str(common_dates[-1]),
        "dias_comuns": len(common_dates),
        "rodos_ativos": len(rodos),
        "total_live": int(total_live),
        "total_base": int(total_base),
        "match_exato": int(total_exact),
        "match_fuzzy_adicional": int(max(0, total_fuzzy_only)),
        "match_total": int(total_matched),
        "precision_total_pct": round(precision_global, 2),
        "recall_total_pct": round(recall_global, 2),
        "semaforo_global": semaforo_global,
        "fuzzy_threshold": FUZZY_SIMILARITY_THRESHOLD,
        "regra_semaforo": {
            "verde": ">= 90%",
            "amarelo": ">= 70% e < 90%",
            "vermelho": "< 70%",
        },
    }

    daily_path = OUT_DIR / "match_por_dia.csv"
    diverg_path = OUT_DIR / "divergencias_com_motivo.csv"
    motivos_path = OUT_DIR / "resumo_motivos.csv"
    summary_path = OUT_DIR / "semaforo_aderencia.json"

    daily_df.to_csv(daily_path, index=False)
    diverg_df.to_csv(diverg_path, index=False)
    motivo_counts.to_csv(motivos_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    print("=" * 68)
    print("AUDITORIA DE ADERENCIA DIARIA | LIVE (XLSX) x BASE (BACKTEST)")
    print("=" * 68)
    print(f"Periodo comum: {common_dates[0]} a {common_dates[-1]} ({len(common_dates)} dias)")
    print(f"Live total: {total_live} | Base total: {total_base}")
    print(f"Match exato:         {total_exact:3d}  ({total_exact/total_live*100:.1f}%)" if total_live else "")
    print(f"Match fuzzy adicional:{max(0,total_fuzzy_only):3d}  ({max(0,total_fuzzy_only)/total_live*100:.1f}%)" if total_live else "")
    print(f"Match total:         {total_matched:3d}  ({precision_global:.1f}%)")
    print(f"Recall base->live:   {recall_global:.1f}%")
    print(f"Semaforo global: {semaforo_global.upper()}")

    print("\nMotivos de divergencia (excluindo matches fuzzy):")
    real_div = motivo_counts[motivo_counts["motivo"] != "nome_diferente_fuzzy_match"]
    print(real_div.head(10).to_string(index=False))

    print("\nArquivos gerados:")
    print(f"- {daily_path}")
    print(f"- {diverg_path}")
    print(f"- {motivos_path}")
    print(f"- {summary_path}")


if __name__ == "__main__":
    main()
