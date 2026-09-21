# PROMPT DE AUDITORIA CIENTÍFICA INDEPENDENTE — ARKAD PROD (CLAUDE)
# TEMA: VARREDURA DOS 43 MERCADOS NA BASE DA API FUTPYTHONTRADER & NOVOS MÉTODOS 2026

> **INSTRUÇÃO PARA O CLAUDE:**
> Thiago solicitou uma auditoria científica independente sobre a varredura exaustiva dos **43 mercados da Betfair** realizada pelo Antigravity, avaliando hipóteses de novos métodos baseados na mudança de regime de mercado observada em 2026.
> 
> **LEI 7 DO GEMINI.md (RIGOR OBRIGATÓRIO): UMA BASE POR MÉTODO.**
> Toda a varredura e a auditoria devem ser realizadas **estritamente na base canônica da API FutPythonTrader** (`Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH3.csv` + feeds diários `scratch/feed_arquivo/*.parquet` com placares oficiais). O coletor da VPS (odd KO−10 com 5 semanas) NÃO deve ser usado para julgar estes métodos pré-jogo, atuando apenas como nota informativa de odd executável de tela.
> 
> **DIRETRIZ DO THIAGO:**
> "2026 tem um peso grande. Se em 2026 o método estiver no verde, ele tem potencial porque o mercado está mudando (onda de gols, reprecificação de favoritos e zebras)."

---

## 📊 BLOCO 1: O RAIO-X MACRO DOS 43 MERCADOS (BASE AMPLA CEGA)

Na base FRESH3 completa (52.116 jogos, 2024–2026), rodamos os 43 mercados nos dois lados (Back e Lay) sem qualquer filtro seletivo:

1. **Eficiência Inabalável nos Mercados Principais:**
   * `Back Home`: ROI 2026 = −4,00% | 3 Anos = −7,08% (N=12.990)
   * `Back Draw`: ROI 2026 = −8,14% | 3 Anos = −9,41% (N=12.998)
   * `Back Away`: ROI 2026 = −2,44% | 3 Anos = −3,52% (N=12.889)
   * `Back Over 2.5 FT`: ROI 2026 = −3,88% | 3 Anos = −7,23% (N=11.435)
   * `Back BTTS Sim`: ROI 2026 = −3,73% | 3 Anos = −8,13% (N=10.833)
   * *Diagnóstico:* Apostas cegas em mercados líquidos continuam sendo destruídas pelo overround da casa (2% a 8%).

2. **Os Únicos 7 Mercados no Verde em 2026 (Base Ampla, N >= 2.000):**
   * `Lay CS 0x3`: N=6.338 | WR 98,20% vs BE 95,74% | **ROI 2026: +2,63% (+166,6u)** | 3 Anos: +0,82%
   * `Lay CS 2x0`: N=11.713 | WR 92,85% vs BE 90,75% | **ROI 2026: +2,44% (+285,7u)** | 3 Anos: −0,07%
   * `Lay CS 3x3`: N=2.645 | WR 98,53% vs BE 96,44% | **ROI 2026: +2,21% (+58,4u)** | 3 Anos: +1,78%
   * `Lay CS 1x3`: N=8.028 | WR 97,07% vs BE 96,06% | **ROI 2026: +1,09% (+87,4u)** | 3 Anos: +0,14%
   * `Back CS 1x1`: N=12.519 | WR 11,67% vs BE 12,76% | **ROI 2026: +30,74% (+3.847u)** | 3 Anos: −9,77% (altamente concentrado no 1º semestre)

*Pergunta 1 ao Claude:* Você concorda que em bases amplas cegas, o alpha pré-jogo só apareceu na cauda de Correct Score (especialmente placares com 3 gols ou empates raros)?

---

## 🎯 BLOCO 2: CANDIDATO NOVO 1 — "BACK 0x2 SUPER FAVORITO VISITANTE"

Ao cruzar o favoritismo com os mercados de placar exato, surgiu um padrão de retorno explosivo em 2026:

* **Tese Quantitativa:** Visitante Super Favorito (`Odd_A_Back <= 1.40`) com odd de Back do 0x2 entre **`6.0` e `20.0`** (odd mediana: 11.75).
* **Mecanismo:** Super favoritos fora de casa (Man City, Real Madrid, Bayern, PSG) controlam a partida, não tomam gol da zebra mandante e vencem por 0-2 com frequência muito superior à precificada pela Betfair.
* **Dados na Base FRESH3 (API FutPythonTrader):**
  * 2024: N = 210 | WR 11,9% vs BE 12,5% | P&L **+14,88u** | **ROI +7,09%**
  * 2025: N = 282 | WR 11,7% vs BE 12,7% | P&L **−34,97u** | **ROI −12,40%**
  * 2026: N = 106 | WR **23,6%** vs BE 11,7% | P&L **+112,52u** | **ROI +106,15%**
  * **Total 3 Anos:** N = 598 | WR **13,9%** vs BE 12,4% | P&L **+92,43u** | **ROI +15,46%**
  * **Estabilidade 2026:** Jan–Abr (+218%), Mai–Jul (+32%), Ago–Set (+52%).

*Perguntas 2 ao Claude:*
1. Esse salto de WR para 23,6% em 2026 (1 em cada 4 jogos terminando 0x2) é uma anomalia de amostra pequena ($N=106$) ou um viés estrutural de super favoritos visitantes na onda de gols?
2. Como se comporta esse filtro em thresholds vizinhos (`Odd_A <= 1.50`, `Odd_CS_0x2_Back` [5.0, 25.0])?
3. Qual o resultado desse método no feed diário recente da API (`scratch/feed_arquivo/*.parquet`, 19/08 a 21/09)?
4. Veredito: vale colocar na Página 03 do Streamlit em **Watchlist Stake-Zero**?

---

## 🛡️ BLOCO 3: CANDIDATO NOVO 2 — "LAY 3x3 GERAL"

* **Tese Quantitativa:** Qualquer jogo onde a odd de Lay do placar 3-3 esteja entre **`15.0` e `50.0`** (odd mediana: 34.0).
* **Mecanismo:** 6 gols exatos divididos igualmente (3 a 3) é um evento hiper-raro. O mercado overshota a probabilidade em ligas ofensivas.
* **Dados na Base FRESH3 (API FutPythonTrader):**
  * 2024: N = 234 | WR 97,4% vs BE 97,9% | ROI −0,45%
  * 2025: N = 266 | WR 97,4% vs BE 97,9% | ROI −0,51%
  * 2026: N = 2.413 | WR **98,5%** vs BE 96,9% | P&L **+39,68u** | **ROI +1,64%**
  * **Total 3 Anos:** N = 2.913 | WR **98,3%** vs BE 97,1% | P&L **+37,26u** | **ROI +1,28%**

*Perguntas 3 ao Claude:*
1. O volume de 3x3 em 2024–2025 era pequeno (~250 jogos/ano) e explodiu para 2.413 jogos em 2026 por maior cobertura de odds da API ou regime?
2. A margem de +1,64% em odds de lay ~34 (liability média de 33u) compensa o risco de ruína caso ocorra um cluster de empates 3-3?
3. Veredito: Watchlist Stake-Zero ou Descarte?

---

## 📋 BLOCO 4: OS 5 MÉTODOS ATIVOS NA PÁGINA 03 (OPINIÃO GERAL)

Atualmente, a Página 03 do Streamlit monitora em **stake-zero**:
1. `Lay 1x1 c/ Fav Forte` (`Fav <= 1.50 | Lay 6.0 a 12.0`): +2,28% em 2026 (+18,8u) e +2,23% no feed recente.
2. `Lay 0x0 Super Fav` (`Fav <= 1.40 | Lay 7.0 a 18.0`): +3,75% em 2026 (+4,2u) e +6,45% no feed recente travado em 18.0.
3. `Lay 2x0 Zebra Mandante` (`Fav Visitante <= 1.70 | Lay 6.0 a 25.0`): +7,63% em 2026 (+29,0u) e +8,19% nos scans.
4. `Lay 0x2 Zebra Visitante` (`Fav Mandante <= 1.70 | Lay 6.0 a 25.0`): +3,31% em 2026 (+20,9u).
5. `Lay 0x3 Zebra Visitante` (`Fav Mandante <= 1.60 | Lay 15.0 a 50.0`): +4,55% a +8,02% em 2026 (+9,5u).
6. `Lay 0x1 Sniper` (`Fav Mandante 1.55-2.15 | Lay 10.0 a 16.0`): +1,09% em 2026, em observação.

*Pergunta 4 ao Claude:*
Desses métodos monitorados, quais você considera os mais sólidos sob o ponto de vista da teoria de assimetria de mercado? A suíte de Zebras (Lay 2x0, Lay 0x2, Lay 0x3) e os Favoritos (Lay 0x0 e Lay 1x1) formam o melhor portfólio de observação para Setembro e Outubro?

---

## ⚖️ FORMATO DE RESPOSTA ESPERADO DO CLAUDE:
1. Parecer sobre o `Back 0x2 Super Fav Fora` (Aprovar para Watchlist Stake-Zero ou Reprovar).
2. Parecer sobre o `Lay 3x3 Geral` (Aprovar para Watchlist Stake-Zero ou Reprovar).
3. Avaliação da tese do Thiago ("O mercado está mudando em 2026 e o peso de 2026 deve guiar a observação").
4. Recomendações finais para a governança da Página 03 do Streamlit.
