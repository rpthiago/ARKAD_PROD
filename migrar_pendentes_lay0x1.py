# -*- coding: utf-8 -*-
"""
migrar_pendentes_lay0x1.py — consolida o paper do Lay 0x1 numa pasta so.

Le os 'jogos_pendentes_lay0x1_*.xlsx' da RAIZ (dois sistemas paralelos, contabilidade
furada), funde por (Data, Mandante, Visitante), e grava um arquivo por dia em
paper_trading_lay0x1/ no MESMO formato dos sinais (espelho do 0x0). Onde ja existe
'Placar Final', preenche 'Resultado' (GREEN/RED) automatico; onde nao, deixa em branco.

RED (lay 0x1 perde) = placar final EXATAMENTE 0-1. GREEN = qualquer outro placar.

Uso:  python migrar_pendentes_lay0x1.py
Idempotente: pode rodar de novo; regenera os arquivos 'sinais_lay0x1_migrado_*.xlsx'.
"""
import glob, os, re
import numpy as np, pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
PASTA = os.path.join(HERE, "paper_trading_lay0x1")
COLS = ["Data", "Horário", "Liga", "Mandante", "Visitante", "Odd Lay Betfair",
        "Probabilidade ML", "Estratégia", "Resultado", "Lucro (R$)", "Placar Final"]

# mapeia colunas dos arquivos-fonte -> schema da pasta
MAP = {
    "Date": "Data", "Data": "Data", "Horario": "Horário", "Horário": "Horário",
    "Liga": "Liga", "Home": "Mandante", "Mandante": "Mandante",
    "Away": "Visitante", "Visitante": "Visitante",
    "Odd_Lay": "Odd Lay Betfair", "Odd Lay Betfair": "Odd Lay Betfair",
    "Prob": "Probabilidade ML", "Probabilidade ML": "Probabilidade ML",
    "Metodo": "Estratégia", "Estratégia": "Estratégia",
    "Placar_Final": "Placar Final", "Placar Final": "Placar Final",
}


def _placar(p):
    if pd.isna(p):
        return None
    nums = re.findall(r"\d+", str(p))
    return (int(nums[0]), int(nums[1])) if len(nums) >= 2 else None


def _resultado(p):
    r = _placar(p)
    if r is None:
        return ""                       # sem placar -> voce preenche
    return "RED" if r == (0, 1) else "GREEN"


def _first_nonnull(s):
    for x in s:
        if pd.notna(x) and str(x).strip() != "":
            return x
    return np.nan


def main():
    fontes = glob.glob(os.path.join(HERE, "jogos_pendentes_lay0x1_*.xlsx"))
    if not fontes:
        print("Nenhum jogos_pendentes_lay0x1_*.xlsx na raiz."); return
    frames = []
    for f in fontes:
        d = pd.read_excel(f)
        d = d.rename(columns={k: v for k, v in MAP.items() if k in d.columns})
        keep = [c for c in COLS if c in d.columns]
        frames.append(d[keep])
        print(f"  lido {os.path.basename(f)}: {len(d)} linhas")
    raw = pd.concat(frames, ignore_index=True)
    raw["Data"] = pd.to_datetime(raw["Data"], errors="coerce").dt.strftime("%Y-%m-%d")
    raw = raw.dropna(subset=["Data", "Mandante", "Visitante"])

    # funde por jogo (pega 1o valor nao-nulo de cada campo -> preserva Horario + Placar)
    fused = raw.groupby(["Data", "Mandante", "Visitante"], as_index=False).agg(_first_nonnull)
    for c in COLS:
        if c not in fused.columns:
            fused[c] = np.nan
    fused["Resultado"] = fused["Placar Final"].apply(_resultado)
    fused = fused[COLS].sort_values(["Data", "Horário"], na_position="last")

    os.makedirs(PASTA, exist_ok=True)
    n_dias = 0
    com_res = int((fused["Resultado"] != "").sum())
    for dia, g in fused.groupby("Data"):
        out = os.path.join(PASTA, f"sinais_lay0x1_migrado_{dia}.xlsx")
        g.to_excel(out, index=False)
        n_dias += 1
    print(f"\nconsolidado: {len(fused)} jogos | {n_dias} arquivos diarios em paper_trading_lay0x1/")
    print(f"  com Resultado auto-preenchido (tem placar): {com_res}")
    print(f"  em BRANCO (falta voce por o placar/resultado): {len(fused) - com_res}")
    faltam = fused[fused["Resultado"] == ""][["Data", "Mandante", "Visitante"]]
    if len(faltam):
        print("\n  jogos SEM resultado (preencha 'Placar Final' ou 'Resultado'):")
        print(faltam.to_string(index=False))


if __name__ == "__main__":
    main()
