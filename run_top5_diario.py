# -*- coding: utf-8 -*-
"""
run_top5_diario.py — Executável CLI para filtrar os TOP 5 Melhores Confrontos do Dia
Uso:
  python run_top5_diario.py              (pega a data de hoje)
  python run_top5_diario.py 2026-09-08   (pega data específica)
"""

import sys
from datetime import datetime, date
from pathlib import Path
import numpy as np
import pandas as pd

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from ciclos_alavancagem_engine import carregar_grade_e_filtrar


def main():
    target_date = sys.argv[1] if len(sys.argv) > 1 else date.today().strftime("%Y-%m-%d")
    print("=" * 85)
    print(f"  🎯 RADAR DE CICLOS & ALAVANCAGEM — TOP 5 JOGOS DO DIA ({target_date})")
    print("  Combinação: Saldo Menor (Top 3) + Dupla Chance 1X Super Fav + Over 0.5 FT")
    print("=" * 85)

    df_top5, df_all, msg = carregar_grade_e_filtrar(target_date, max_saldo_menor=3, total_top=5)

    if df_top5.empty:
        print(f"\n[!] {msg}")
        return

    print(f"\n[*] Total de candidatos avaliados na grade: {len(df_all)}")
    print(f"[*] Confrontos selecionados para o bilhete diário: {len(df_top5)}\n")

    print("-" * 85)
    print(f"{'ORDEM':<8} {'HORA':<6} {'MÉTODO':<25} {'JOGO':<30} {'SELEÇÃO':<20} {'ODD':<6}")
    print("-" * 85)

    odd_acumulada = 1.0
    for idx, row in df_top5.iterrows():
        ordem = row['Ordem_Ciclo']
        hora = row['Time']
        metodo = row['Metodo']
        jogo = row['Match']
        selecao = row['Selecao']
        odd = row['Odd']
        odd_acumulada *= odd
        print(f"{ordem:<8} {hora:<6} {metodo:<25} {jogo:<30} {selecao:<20} {odd:<6.3f}")

    print("-" * 85)
    ganho_dia_pct = (odd_acumulada - 1.0) * 100.0
    print(f"📊 Odd Acumulada do Dia: {odd_acumulada:.3f} | Ganho Potencial se bater os 5: +{ganho_dia_pct:.2f}%")
    print(f"💰 Se começar com R$ 100 hoje, fecha o dia em: R$ {100.0 * odd_acumulada:,.2f}")

    print("\n" + "=" * 85)
    print("  📋 BILHETE FORMATADO (COPIE PARA WHATSAPP / TELEGRAM)")
    print("=" * 85)
    print(f"🎯 DESAFIO DE CICLOS R$ 100 -> R$ 200 — {target_date}\n")
    for idx, row in df_top5.iterrows():
        print(f"⏰ {row['Time']} | {row['Match']}")
        print(f"   📌 {row['Metodo']} -> {row['Selecao']} @ {row['Odd']:.2f}")
    print(f"\n📈 Multiplicador do Dia: {odd_acumulada:.2f}x (+{ganho_dia_pct:.1f}%)")
    print("🛡️ Gestão: 1 jogo por vez. Ao atingir R$ 150, saque os R$ 100 e jogue com risco ZERO!")
    print("=" * 85)


if __name__ == "__main__":
    main()
