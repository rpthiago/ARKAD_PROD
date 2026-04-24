import json
from pathlib import Path

OUT = Path("Arquivados_Apostas_Diarias/Relatorios/Backtest_Abril_2026")
cenarios = [
    ("A_sem_filtro",   "Sem filtros - tudo apostado"),
    ("B_filtros",      "Filtros corretos (odd+blacklist)"),
    ("C_1x0_hist",     "Filtros + 1x0 somente SPAIN1/ITALY1"),
    ("D_somente_0x1",  "Somente 0x1 com filtros"),
]

# Mostra campos do summary
with open(OUT / "abril_2026_A_sem_filtro_summary.json") as f:
    s = json.load(f)
print("Campos do summary:")
for k, v in s.items():
    print(f"  {k}: {v}")

print()
print("-"*100)
header = "{:<42} {:>5} {:>5} {:>5} {:>7} {:>10} {:>10} {:>8}".format(
    "Cenário", "Ent", "G", "R", "WR%", "P&L", "DD abs", "DD%"
)
print(header)
print("-"*100)

for run_id, desc in cenarios:
    with open(OUT / f"abril_2026_{run_id}_summary.json") as f:
        s = json.load(f)
    ent = s.get("Entradas_Executadas", s.get("executed_rows", 0))
    wr  = s.get("Win_Rate_Executadas_%", s.get("win_rate_pct", s.get("win_rate", 0)))
    if wr is not None and wr < 2:
        wr = wr * 100
    pnl = s.get("Lucro_Final", s.get("lucro_acumulado", 0))
    dd  = s.get("Max_Drawdown_Abs", s.get("drawdown_abs", 0))
    ddp = s.get("Max_Drawdown_%", s.get("drawdown_pct", 0))
    # Wins/losses: derivar do WR se não disponível
    g   = round(ent * (wr or 0) / 100)
    r   = ent - g
    row = "{:<42} {:>5} {:>5} {:>5} {:>6.1f}% {:>+9.0f} {:>10.0f} {:>7.1f}%".format(
        desc, ent, g, r, wr or 0, pnl or 0, dd or 0, ddp or 0
    )
    print(row)
