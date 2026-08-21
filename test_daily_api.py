import b365_data_utils
import lay_2x0_rf_v2_strategy as strat

date_str = "2026-08-15"
print("=== FETCH BETFAIR DAILY 15/08/2026 ===")
games = b365_data_utils.fetch_betfair_daily(date_str)
print(f"[+] Total de jogos retornados pela API em {date_str}: {len(games)}")

if games:
    print("\nExemplo dos 5 primeiros jogos:")
    for g in games[:5]:
        print("  -", g.get("League"), "|", g.get("Home"), "x", g.get("Away"), "| Odd Lay 2x0:", g.get("Odd_CS_2x0_Lay"))

    hist_df = b365_data_utils.load_b365_historical()
    res = strat.predict_and_evaluate_live(games, hist_df)
    print(f"\n[+] Total de avaliações pelo Lay 2x0: {len(res)}")
    aprovados = [g for g in res if g.get("Decision") == "APOSTA"]
    skips = [g for g in res if g.get("Decision") == "SKIP"]
    print(f"✅ APROVADOS: {len(aprovados)}")
    print(f"⛔ SKIPS: {len(skips)}")
    for g in skips:
        print(f"   [SKIP - {g.get('Reason')}] {g.get('League')} | {g.get('Home')} x {g.get('Away')}")
