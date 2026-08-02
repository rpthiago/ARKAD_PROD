"""
registrar_mes_lay0x1.py — Acompanhamento MENSAL honesto do Lay 0x1 (paper).
Le os consolidados (sinais_lay0x1_*_consolidado.csv), resume por mes e acumula em
acompanhamento_lay0x1_mensal.csv. Objetivo: juntar amostra ao longo dos meses pra ver
se a margem fina sobre o break-even se sustenta (a auditoria FDR disse que nao; isto e
o teste ao vivo). Inclui checagem de integridade (taxa de 0-0) pra pegar auto-fetch bugado.

Uso: python registrar_mes_lay0x1.py
"""
import glob, re
import pandas as pd, numpy as np
from pathlib import Path

ROOT = Path(__file__).resolve().parent
TRACK = ROOT / "acompanhamento_lay0x1_mensal.csv"
COMM = 0.05
LIAB = 200.0   # responsabilidade fixa usada no paper (R$)


def resumir(df):
    df = df.copy()
    df["odd"] = pd.to_numeric(df["Odd_Lay"], errors="coerce")
    df["pnl_u"] = pd.to_numeric(df["PnL"], errors="coerce")          # PnL por unidade de stake
    df["pl"] = df["Placar_Final"].astype(str).str.replace(" ", "")
    df["red"] = df["Status"].astype(str).str.upper().str.contains("RED")
    df["mes"] = pd.to_datetime(df["Date"], errors="coerce").dt.to_period("M").astype(str)
    out = []
    for mes, g in df.groupby("mes"):
        n = len(g); r = int(g["red"].sum())
        om = g["odd"].median()
        be = (om - 1) / ((om - 1) + (1 - COMM))
        wr = 1 - r / n
        roi = g["pnl_u"].mean()
        pnl_liab = float((g["pnl_u"] * LIAB / (g["odd"] - 1)).sum())
        zero = g["pl"].isin(["0-0", "0x0"]).mean()
        out.append(dict(mes=mes, n=n, green=n - r, red=r,
                        win_rate=round(wr, 4), break_even=round(be, 4),
                        margem_pp=round((wr - be) * 100, 2),
                        roi_stake=round(roi, 4), pnl_liab200=round(pnl_liab, 2),
                        odd_med=round(float(om), 1), taxa_0a0=round(float(zero), 3),
                        integridade="OK" if zero <= 0.15 else "SUSPEITA(0-0 alto)"))
    return pd.DataFrame(out)


def main():
    arqs = sorted(glob.glob(str(ROOT / "sinais_lay0x1_*_consolidado.csv")))
    if not arqs:
        print("nenhum consolidado encontrado."); return
    partes = [resumir(pd.read_csv(a)) for a in arqs]
    novo = pd.concat(partes, ignore_index=True)

    if TRACK.exists() and TRACK.stat().st_size > 0:
        old = pd.read_csv(TRACK)
        allm = pd.concat([old, novo], ignore_index=True).drop_duplicates("mes", keep="last")
    else:
        allm = novo
    allm = allm.sort_values("mes").reset_index(drop=True)
    allm.to_csv(TRACK, index=False)

    print("=== ACOMPANHAMENTO MENSAL — Lay 0x1 (paper honesto) ===")
    print(allm.to_string(index=False))

    # veredito acumulado
    tot_n = allm["n"].sum(); tot_red = allm["red"].sum()
    wr_ac = 1 - tot_red / tot_n
    be_ac = ((allm["odd_med"].median() - 1) / ((allm["odd_med"].median() - 1) + (1 - COMM)))
    pnl_ac = allm["pnl_liab200"].sum()
    print(f"\n--- ACUMULADO: {len(allm)} mes(es) | {tot_n} apostas ---")
    print(f"  win rate acumulado: {wr_ac:.2%} | break-even ~{be_ac:.2%} | margem {(wr_ac-be_ac)*100:+.2f}pp")
    print(f"  P&L acumulado (liab R${LIAB:.0f}): R$ {pnl_ac:+.0f}")
    susp = allm[allm["integridade"] != "OK"]
    if len(susp):
        print(f"  !! {len(susp)} mes(es) com integridade SUSPEITA (taxa 0-0 > 15% = auto-fetch bugado): {list(susp['mes'])}")
    print("\n  Regra honesta: 1-2 meses NAO validam (margem fina = ruido). Acumular >= 6 meses")
    print("  e ver se a margem sobre o break-even se mantem > 0 de forma consistente.")
    print(f"  Salvo: {TRACK.name}")


if __name__ == "__main__":
    main()
