# AUDITORIA ARKAD: Avaliação do Filtro de Over 2.5 ≥ 1,60 no Lay Draw (Restrito a 2026)

> **De:** Thiago & Antigravity/Gemini  
> **Para:** Claude  
> **Tema:** Teste empírico da sugestão de refinar o Lay Draw filtrando apenas jogos com Odd_Over25 >= 1.60.  
> **Regime de Dados:** Estritamente **2026** (conforme acordado na auditoria anterior, eliminando o desajuste de spread de 2024-2025).

---

## 1. Contexto e Motivação da Auditoria

Claude, você havia sugerido anteriormente testar se o filtro de **Odd_Over25_FT >= 1.60** agregaria valor ao método **Lay Draw (Super Fav <= 1.40)**, sob a premissa de que evitaríamos jogos propensos a muitos gols ou buscaríamos um sweet spot de mercado.

Como estabelecemos na auditoria passada, **restringimos 100% da análise aos dados de 2026** (Date >= 2026-01-01), utilizando o spread mediano calibrado de **1,044x** da Betfair em 2026, comissão de 3% (OrbitX) e 5% (Betfair), governança de ligas ativa e a regra estrutural de copas (apenas mandante em copas).

Abaixo estão os números empíricos medidos diretamente na base de dados para sua avaliação crítica.

---

## 2. Resultados Empíricos Medidos em 2026

### 2.1 Consolidado de 2026 Completo (2.013 Partidas)

| Cenário em 2026 | N (Jogos) | Greens | Reds | Win Rate | Break-Even (c=3%) | Margem (Delta) | ROI / Liability | PnL Nominal (c=3%) |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **Base Ampla (Sem filtro Over)** | **2.013** | **1.714** | **299** | **85,15%** | **81,65%** | **+3,49 pp** | **+4,31%** | **+365,5 u** |
| **Com Over 2.5 >= 1.60** | **823** | **693** | **130** | **84,20%** | **80,58%** | **+3,63 pp** | **+4,52%** | **+147,4 u** |
| **Com Over 2.5 < 1.60 (Descartado)** | **1.190** | **1.021** | **169** | **85,80%** | **82,40%** | **+3,40 pp** | **+4,16%** | **+218,1 u** |

**Primeira Constatação:**
- O ganho de ROI sobre liability é marginal: de **+4,31%** para **+4,52%** (+0,21 pp).
- Em contrapartida, o filtro **elimina 59,1% do volume de jogos**, fazendo o PnL líquido em unidades despencar de **+365,5 u** para **+147,4 u** (perda de mais de 218 unidades de lucro).

---

### 2.2 Fatiamento Detalhado por Faixas de Over 2.5 em 2026

Ao abrir o comportamento em fatias finas da odd de Over 2.5, identificamos a física do mercado:

| Faixa de Odd Over 2.5 | N | Greens | Reds | Win Rate | Break-Even | Margem (Delta) | ROI / Liability | PnL Nominal |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **< 1.40** (Muito Over) | 337 | 294 | 43 | **87,24%** | 83,19% | **+4,05 pp** | **+4,92%** | +76,8 u |
| **[1.40 a 1.60)** | 853 | 727 | 126 | **85,23%** | 82,08% | **+3,14 pp** | **+3,86%** | +141,3 u |
| **[1.60 a 1.80)** *(O Sweet Spot)* | 529 | 452 | 77 | **85,44%** | 80,85% | **+4,59 pp** | **+5,73%** | +119,5 u |
| **>= 1.80** *(Under Pesado)* | 294 | 241 | 53 | **81,97%** | 80,09% | **+1,88 pp** | **+2,33%** | +27,9 u |

**Segunda Constatação:**
- Na faixa [1.60, 1.80), a odd do Lay do empate tende a ser ligeiramente mais baixa (~5.00 a 5.50), o que reduz o break-even exigido para 80,85% e gera um sweet spot (+5,73% ROI).
- Porém, quando o Over ultrapassa 1.80 (jogos de perfil truncado/under), a taxa de acerto despenca para **81,97%** e o ROI cai para **+2,33%**, porque o jogo acumula placares de 0x0 e 1x1.

---

### 2.3 Teste de Instabilidade Temporal em 2026 (Jan–Jul vs Ago–Out)

Quando dividimos o próprio ano de 2026 em duas metades:

| Período | Cenário | N | WR Real | Break-Even | Delta | ROI / Liability | PnL Nominal |
|---|---|:---:|:---:|:---:|:---:|:---:|:---:|
| **Jan–Jul/2026** | Base Ampla | 1.476 | 84,35% | 81,63% | +2,72 pp | +3,36% | +205,6 u |
| | **Over >= 1.60** | 658 | **84,95%** | 80,56% | **+4,40 pp** | **+5,48%** | +141,5 u |
| | Over < 1.60 | 818 | 83,86% | 82,49% | +1,37 pp | +1,66% | +64,1 u |
| **Ago–Out/2026** | Base Ampla | 537 | **87,34%** | 81,72% | **+5,62 pp** | **+6,90%** | +159,9 u |
| | **Over >= 1.60** | 165 | **81,21%** | 80,66% | **+0,55 pp** | **+0,66%** | **+5,9 u** |
| | Over < 1.60 | 372 | **90,05%** | 82,19% | **+7,86 pp** | **+9,67%** | +154,1 u |

**Terceira Constatação:**
- Nos últimos 2 meses e meio (Agosto a Outubro/2026), o filtro **inverteu completamente**:
  - Over >= 1.60 colapsou para apenas **+0,66% de ROI** (WR 81,21% colado no break-even de 80,66%).
  - Over < 1.60 disparou para **90,05% de Win Rate** e **+9,67% de ROI**.

---

### 2.4 Teste com Odd Real de Lay da Betfair (01/08 a 24/09/2026 — Base FRESH)

No cohort de jogos casados linha a linha com a odd de Lay real observada na Betfair Exchange:

* **Base Ampla Real (N=349):** WR = 84,53% | BE = 82,83% | **ROI = +1,94%** | PnL = **+43,4 u**
* **Com Over 2.5 >= 1.60 (N=141):** WR = **80,14%** | BE = 81,98% | **ROI = −2,33% 🔴** | PnL = **−11,4 u**
* **Com Over 2.5 < 1.60 (N=208):** WR = **87,50%** | BE = 83,41% | **ROI = +4,83% 🟢** | PnL = **+54,8 u**

Na execução em odd real recente, o filtro gerou resultado **negativo**, pois o empate aconteceu em quase 20% dos jogos do grupo com Over alto.

---

## 3. As 4 Perguntas Forenses para o Claude

1. **Trade-off de Volume vs Lucro Absoluto:**
   No ano completo de 2026, a base ampla gerou **+365,5 u** de lucro com ROI de +4,31%. O filtro Over >= 1.60 rendeu ROI de +4,52% (+0,21 pp a mais), mas apenas **+147,4 u** de lucro (perda de 218 unidades de lucro líquido). Vale a pena amputar 60% do portfólio por uma melhora de 0,2 pp no yield?
   
2. **A Física dos Placares 0x0 e 1x1:**
   Você concorda que forçar Over 2.5 >= 1.60 seleciona estruturalmente jogos com expectativa de menos gols, onde os placares de 0x0 e 1x1 são matematicamente mais frequentes, aumentando a incidência de empates e prejudicando o Lay Draw?

3. **Inversão Temporal e Overfitting de Limiar:**
   Como você interpreta a divergência entre Jan-Jul (+5,48% no filtro) e Ago-Out (+0,66% no filtro vs +9,67% no oposto)? Isso se enquadra na regra do GEMINI.md de que refinamento de odds públicas tende a ser variância de curto prazo e overfitting de limiar?

4. **Veredito:**
   Diante dos dados de 2026, você recomenda **manter a regra base ampla oficial** (Fav <= 1.40 + Lay <= 7.00 sem filtro de Over 2.5) ou você ainda enxerga justificativa matemática para aplicar o corte de Over 2.5?
