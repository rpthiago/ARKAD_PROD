"""
Comparativo: jogos do dia (live API) vs base backtest (2026).

Uso:
    python _comparativo_hoje.py [--data 2026-04-29]

Por padrão usa a data de hoje.
Mostra lado a lado:
  - jogos que passam nos filtros E estão na base (OK)
  - jogos que passam nos filtros MAS não estão na base (atenção)
  - jogos da base que não vieram no live hoje (ausentes)
"""
from __future__ import annotations

import argparse
import json
import os
import unicodedata
from datetime import date
from difflib import SequenceMatcher
from pathlib import Path

import pandas as pd

BASE_CSV = Path("recalculo_sem_combos_usuario.csv")
CFG_PATH = Path("config_prod_v1.json")

# ── normalização de nomes ──────────────────────────────────────────────────
_PREFIXES = ["red bull ", "rb ", "fc ", "fk ", "sk ", "sc ", "ac ", "as ", "us ",
             "cd ", "cf ", "rc ", "ca ", "sd ", "ud ", "if ", "bk ", "gd ", "ss "]
_SUFFIXES = [" fc", " fk", " sc", " ac", " if", " bk", " cf", " rc", " ca", " sd",
             " ud", " ss", " sk", " us", " as"]
import re
_STATE = re.compile(r"\b(mg|rj|sp|sc|rs|pr|ba|ce|go|pe|am)\b")


def _norm(t: object) -> str:
    s = unicodedata.normalize("NFD", str(t or ""))
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = re.sub(r"[-_()\[\]]", " ", s.lower())
    s = _STATE.sub("", s)
    for p in _PREFIXES:
        if s.startswith(p):
            s = s[len(p):]
            break
    for suf in _SUFFIXES:
        if s.endswith(suf):
            s = s[: -len(suf)]
            break
    return re.sub(r"\s+", " ", s).strip()


def _similar(a: str, b: str) -> bool:
    if not a or not b:
        return False
    if a == b:
        return True
    r = SequenceMatcher(None, a, b).ratio()
    if r >= 0.80:
        return True
    if a in b or b in a:
        return True
    return False


def _split_jogo(j: object) -> tuple[str, str]:
    s = str(j or "")
    for sep in (" x ", " vs ", " - ", "|"):
        if sep in s:
            parts = s.split(sep, 1)
            return _norm(parts[0]), _norm(parts[1])
    return _norm(s), ""


# ── filtros ────────────────────────────────────────────────────────────────
def _passes_filters(row: pd.Series, filtros: dict) -> tuple[bool, str]:
    metodo = str(row.get("Metodo", "")).strip()
    liga = str(row.get("Liga", "")).strip()
    odd = pd.to_numeric(row.get("Odd_Base", None), errors="coerce")

    fm = filtros.get(metodo)
    if fm is None:
        return False, f"metodo_desconhecido({metodo})"

    ligas_ok: list[str] = fm.get("ligas_permitidas", [])
    if liga not in ligas_ok:
        return False, f"liga_fora_universo({liga})"

    odd_min = float(fm.get("odd_min", 0))
    odd_max = float(fm.get("odd_max", 999))
    if pd.isna(odd):
        return False, "odd_ausente"
    if not (odd_min <= odd <= odd_max):
        return False, f"odd_fora_faixa({odd:.2f} fora de [{odd_min},{odd_max}])"

    return True, "ok"


# ── carrega base ───────────────────────────────────────────────────────────
def _load_base(target_year: int) -> pd.DataFrame:
    base = pd.read_csv(BASE_CSV)
    base["Data"] = pd.to_datetime(base["Data_Arquivo"], errors="coerce").dt.date
    base = base[base["Data"].apply(lambda d: d.year if d else 0) == target_year].copy()
    base["Liga"] = base["Liga"].astype(str).str.strip()
    base["Metodo"] = base["Metodo"].astype(str).str.strip()
    base["Odd_Base"] = pd.to_numeric(base["Odd_Base"], errors="coerce")
    h, a = zip(*base["Jogo"].map(_split_jogo)) if not base.empty else ([], [])
    base["home_n"] = list(h)
    base["away_n"] = list(a)
    return base


# ── busca live da API ──────────────────────────────────────────────────────
def _fetch_live(target_date_iso: str, cfg: dict) -> pd.DataFrame:
    """Tenta buscar dados da API. Se falhar ou token ausente, retorna vazio."""
    try:
        from ingestao_tempo_real import load_live_dataframe
        df, msg = load_live_dataframe(target_date_iso, cfg)
        print(f"[live] {msg}")
        return df
    except Exception as e:
        print(f"[live] Erro ao buscar API: {e}")
        return pd.DataFrame()


# ── fuzzy match entre live e base ─────────────────────────────────────────
def _find_in_base(live_row: pd.Series, base: pd.DataFrame) -> bool:
    d = live_row.get("Data_Arquivo") or live_row.get("Data_Arquivo", "")
    liga = str(live_row.get("Liga", "")).strip()
    metodo = str(live_row.get("Metodo", "")).strip()

    cands = base[(base["Liga"] == liga) & (base["Metodo"] == metodo)]
    if cands.empty:
        return False

    lh, la = _split_jogo(live_row.get("Jogo", ""))
    for _, r in cands.iterrows():
        if _similar(lh, r["home_n"]) and _similar(la, r["away_n"]):
            return True
    return False


# ── main ───────────────────────────────────────────────────────────────────
def main(target_date_iso: str) -> None:
    cfg = json.loads(CFG_PATH.read_text(encoding="utf-8"))
    filtros = cfg.get("runtime_data", {}).get("filtros_metodo", {})

    target_date = date.fromisoformat(target_date_iso)

    print(f"\n{'='*66}")
    print(f"  COMPARATIVO DO DIA — {target_date_iso}")
    print(f"{'='*66}\n")

    # 1. base (só 2026)
    base = _load_base(target_date.year)
    base_hoje = base[base["Data"] == target_date].copy()

    # 2. live
    live_raw = _fetch_live(target_date_iso, cfg)

    if live_raw.empty:
        print("⚠  Nenhum dado live disponível — verificar token FUTPYTHON_TOKEN.\n")
    else:
        # normaliza Data_Arquivo
        if "Data_Arquivo" not in live_raw.columns:
            live_raw["Data_Arquivo"] = target_date_iso
        live_raw["Liga"] = live_raw.get("Liga", pd.Series(dtype=str)).astype(str).str.strip()
        live_raw["Metodo"] = live_raw.get("Metodo", pd.Series(dtype=str)).astype(str).str.strip()
        live_raw["Odd_Base"] = pd.to_numeric(live_raw.get("Odd_Base"), errors="coerce")

    # ── Seção A: jogos live que PASSAM nos filtros ──────────────────────────
    print("── A. Jogos live que PASSAM nos filtros ──────────────────────────")
    if live_raw.empty:
        print("  (sem dados live)\n")
        ok_live = pd.DataFrame()
    else:
        rows_ok, rows_rej = [], []
        for _, r in live_raw.iterrows():
            passed, motivo = _passes_filters(r, filtros)
            if passed:
                rows_ok.append(r)
            else:
                rows_rej.append((r.get("Liga", ""), r.get("Metodo", ""), r.get("Jogo", ""),
                                  r.get("Odd_Base", ""), motivo))

        ok_live = pd.DataFrame(rows_ok)

        if ok_live.empty:
            print("  Nenhum jogo passou nos filtros.\n")
        else:
            ok_live = ok_live[["Liga", "Metodo", "Jogo", "Odd_Base"]].copy()
            ok_live["na_base"] = ok_live.apply(
                lambda r: "✔ sim" if _find_in_base(r, base) else "✘ não", axis=1
            )
            print(ok_live[["Liga", "Metodo", "Jogo", "Odd_Base", "na_base"]].to_string(index=False))
            print()

        # ── Seção B: rejeitados e motivo ─────────────────────────────────
        if rows_rej:
            rej_df = pd.DataFrame(rows_rej, columns=["Liga", "Metodo", "Jogo", "Odd", "motivo_rejeicao"])
            motivos = rej_df["motivo_rejeicao"].value_counts()
            print("── B. Rejeitados pelos filtros ────────────────────────────────")
            print(f"  Total: {len(rej_df)}")
            for motivo, qtd in motivos.items():
                print(f"  {qtd:3d}x  {motivo}")
            print()

    # ── Seção C: jogos da BASE esperados hoje que NÃO vieram no live ───────
    print("── C. Base esperava HOJE mas NÃO veio no live ────────────────────")
    if base_hoje.empty:
        print("  (base não tem jogos para esta data)\n")
    else:
        if not ok_live.empty:
            ausentes = []
            for _, r in base_hoje.iterrows():
                bh, ba = r["home_n"], r["away_n"]
                found = any(
                    _similar(bh, _split_jogo(l["Jogo"])[0]) and _similar(ba, _split_jogo(l["Jogo"])[1])
                    for _, l in ok_live.iterrows()
                ) if not ok_live.empty else False
                if not found:
                    ausentes.append(r)
            if ausentes:
                adf = pd.DataFrame(ausentes)[["Data", "Liga", "Metodo", "Jogo", "Odd_Base"]]
                print(adf.to_string(index=False))
            else:
                print("  Todos os jogos da base estão cobertos pelo live.")
        else:
            cols = ["Data", "Liga", "Metodo", "Jogo", "Odd_Base"]
            print(base_hoje[[c for c in cols if c in base_hoje.columns]].to_string(index=False))
        print()

    # ── Seção D: jogos na base para 2026 — estatísticas ───────────────────
    meses_base = base["Data"].apply(lambda d: d.strftime("%Y-%m") if d else "?")
    print("── D. Distribuição da base em 2026 ───────────────────────────────")
    print(meses_base.value_counts().sort_index().to_string())
    print()
    print(f"  Base 2026 total: {len(base)} linhas | Hoje ({target_date_iso}): {len(base_hoje)} linhas")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default=date.today().isoformat(),
                        help="Data no formato YYYY-MM-DD (padrão: hoje)")
    args = parser.parse_args()
    main(args.data)
