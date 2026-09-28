# 🔍 PROMPT DE AUDITORIA ADVERSARIAL PARA O CLAUDE
**Assunto:** Auditoria Estatística e Metodológica da Engenharia do **`Lay Away Fortaleza 1X`** e do **Chaveamento Mutuamente Exclusivo (`1 Método por Jogo`)** com o `Lay Draw`.

---

## 🛠️ INSTRUÇÕES AO AUDITOR (CLAUDE)
Claude, por favor, audite com **rigor máximo e ceticismo científico** (seguindo as regras do `GEMINI.md` — Lei 1 a Lei 7 e Hall of Shame) a engenharia abaixo do método **`Lay Away Fortaleza 1X`** e a proposta de **Chaveamento (`1 método por jogo`)** com o `Lay Draw (Fav <= 1.40)`.

**Não aceite os números no escuro:** rode seus próprios scripts em Python diretamente nas bases oficiais do repositório (`Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH3.csv`, `metodos_aprovados/.cache_base_betfair.csv` e as 40 planilhas diárias `metodos_aprovados/Sinais_Metodos_Aprovados_2026-*.xlsx`) e responda às **4 Perguntas de Veredito** no final.

---

## 📌 1. Diagnóstico da Regra Antiga do `Lay Away` (`Odd_H <= 1.45 | Lay_A 2.0–15.0`)
Na `pages/01_🏆_Portfolio_Metodos_Aprovados.py`, tínhamos a regra ampla de `Lay Away / Dupla Chance 1X` (`Odd_H_Back <= 1.45 | 2.0 <= Odd_A_Lay <= 15.0`). Ao medir o ROI sobre a **Liability Real (`Odd_A_Lay - 1`)** nos 3 anos da base Betfair (`2024–2026`, `N = 109.757` jogos), identificamos dois vazamentos que anulavam o edge do método:

1. **Armadilha em Copas Eliminatórias (`-12,78%` em Copas Nacionais e `-4,12%` em Copas Continentais):**
   - **Copas Nacionais (`N = 73`):** `58G / 15R` (`WR = 79,45%` vs `BE = 91,04%`, `-11,59 pp`), `PnL(5%) = -9,33u` (`ROI = -12,78%`).
   - **Copas Continentais (`N = 69`):** `60G / 9R` (`WR = 86,96%` vs `BE = 91,34%`, `-4,38 pp`), `PnL(5%) = -2,84u` (`ROI = -4,12%`).
   - *Motivo físico:* Super favoritos mandantes escalam reservas em fases iniciais de Copas Nacionais ou administram vantagem construída no jogo de ida em Copas Continentais, sofrendo derrotas por `0x1`/`1x2`.
2. **Armadilha em Jogos Hiper-Abertos (`Odd_H_Back <= 1.40` e `Odd_Over25_FT_Back < 1.75`, `N = 1.485`):**
   - `1.348G / 137R` (`WR = 90,77%` vs `BE = 91,51%`), `PnL(5%) = -11,55u` (`ROI = -0,78%`).
   - *Motivo físico:* Quando o jogo é precificado como trocação franca (`Over 2.5 < 1.75`), o visitante possui maior expectativa de gols em transição ($\lambda_{\text{Away}}$ mais alto) e a taxa de empate cai para ~`10%`, esvaziando a proteção do empate (`X`) na Dupla Chance `1X`.

---

## 📌 2. A Engenharia do Novo Recorte: `Lay Away Fortaleza 1X`
Quando restringimos estritamente a:
- **Competição:** Apenas **Ligas Nacionais (Pontos Corridos)** (bloqueio total de Copas Nacionais, Copas Continentais e Seleções)
- **Favoritismo Mandante:** `Odd_H_Back <= 1.40`
- **Faixa de Lay Away:** `4.50 <= Odd_A_Lay <= 15.00`
- **Filtro de Controle Defensivo:** `Odd_Over25_FT_Back >= 1.80` (ou `>= 1.75`)

### 📊 Desempenho em 3 Anos (`2024–2026`, `FRESH3` + `.cache_base_betfair.csv`):
- **Corte `Odd_Over25_FT_Back >= 1.80` (`N = 371` jogos em Ligas Nacionais):**
  - **`347 Greens / 24 Reds`** (`WR = 93,53%` vs `Break-Even = 90,87%` $\rightarrow$ **`+2,67 pp` acima do BE**)
  - **`PnL (5% comm) = +11,08u` (`ROI = +2,99%` sobre Liability)**
  - **`PnL (3,5% comm) = +11,64u` (`ROI = +3,14%` sobre Liability)**
  - **Bootstrap IC95% (10.000 reamostragens):** **`[+0,12%, +5,69%]` ($p = 0,0210$ a 5% comm)** e **`[+0,27%, +5,85%]` ($p = 0,0166$ a 3,5% comm)** — **exclui o zero!**
- **Estabilidade Temporal (`6 de 6` semestres consecutivos no verde):**
  - `2024-H1`: `21G / 1R` (`95,5% WR`) $\rightarrow$ `+1,38u` (`ROI +6,25%`)
  - `2024-H2`: `64G / 4R` (`94,1% WR`) $\rightarrow$ `+2,18u` (`ROI +3,21%`)
  - `2025-H1`: `112G / 8R` (`93,3% WR`) $\rightarrow$ `+5,28u` (`ROI +4,40%`)
  - `2025-H2`: `49G / 4R` (`92,5% WR`) $\rightarrow$ `+0,83u` (`ROI +1,57%`)
  - `2026-H1`: `82G / 6R` (`93,2% WR`) $\rightarrow$ `+0,93u` (`ROI +1,06%`)
  - `2026-H2 (Jul–Set)`: `19G / 1R` (`95,0% WR`) $\rightarrow$ `+0,48u` (`ROI +2,42%`) — sendo **`17G / 0R` (`100% WR`, `ROI +7,89%`) em Ago–Set/2026**!
- **Estabilidade em Thresholds Vizinhos (Ligas Nacionais, `Odd_H <= 1.40`, `Lay_A 4.5–15.0`):**
  - `Over 2.5 >= 1.75` (`N = 481`): `+11,04u` (`+2,30%` a 5% comm; `+2,44%` a 3,5% comm; `26G / 0R` em Ago–Set/2026)
  - `Over 2.5 >= 1.80` (`N = 371`): `+11,08u` (`+2,99%` a 5% comm; `+3,14%` a 3,5% comm; `17G / 0R` em Ago–Set/2026)
  - `Over 2.5 >= 1.85` (`N = 292`): `+10,74u` (`+3,68%` a 5% comm; `+3,83%` a 3,5% comm; `12G / 0R` em Ago–Set/2026)
  - `Over 2.5 >= 1.90` (`N = 223`): `+6,36u` (`+2,85%` a 5% comm; `+3,01%` a 3,5% comm; `9G / 0R` em Ago–Set/2026)

---

## 📌 3. A Sobreposição Crítica (`80,3%`) com o `Lay Draw` e o Duelo Direto nos Mesmos Jogos

Ao checar quantos jogos do `Lay Away Fortaleza 1X` também acionam o `Lay Draw (Fav <= 1.40 | Odd_D_Lay 2.5–8.5)`, verificamos que:
- No corte **`Over 2.5 >= 1.80` (`370` jogos)**: **`297` jogos (`80,3%`) batem nos DOIS métodos ao mesmo tempo** (`Lay Draw` + `Lay Away 1X`), e apenas `73` jogos (`19,7%`) batem só no `Lay Away 1X` (quando `Odd_D_Lay > 8.5`).
- No corte **`Over 2.5 >= 1.75` (`480` jogos)**: **`399` jogos (`83,1%`) batem nos DOIS métodos ao mesmo tempo**, e `81` jogos (`16,9%`) batem só no `Lay Away 1X`.

### ⚔️ O que acontece nos exatos mesmos `297` jogos onde OS DOIS batem juntos (`Ligas Nacionais | Odd_H <= 1.40 | Odd_D_Lay 2.5–8.5 | Odd_A_Lay 4.5–15.0 | Over 2.5 >= 1.80`)?

1. **Distribuição Real de Placares (`N = 297`):**
   - Vitória do Mandante (`1`): **`221` jogos (`74,4%`)** $\rightarrow$ GREEN em ambos (`Lay Draw` e `Lay Away 1X`).
   - **Empates (`X`, ex.: `0x0`, `1x1`):** **`57` jogos (`19,2%`)** $\rightarrow$ **RED no `Lay Draw`, mas GREEN no `Lay Away 1X`!**
   - Vitória da Zebra Visitante (`2`): **`19` jogos (`6,4%`)** $\rightarrow$ **GREEN no `Lay Draw`, mas RED no `Lay Away 1X`!**
2. **Resultado Financeiro nos Mesmos `297` Jogos (`Over 2.5 >= 1.80`):**
   - **Se operar `LAY DRAW` nesses 297 jogos:** `240G / 57R` (`WR = 80,81%`) $\rightarrow$ **`PnL(3,5%) = -6,03u` (`ROI = -2,03%`, PREJUÍZO!)**
   - **Se operar `LAY AWAY FORTALEZA 1X` nesses mesmos 297 jogos:** `278G / 19R` (`WR = 93,60%`) $\rightarrow$ **`PnL(3,5%) = +5,12u` (`ROI = +1,72%`, LUCRO!)**
   - **Diferença líquida nos mesmos jogos:** **`+11,14u` (`+3,75 pp` de ROI) a favor de trocar o `Lay Draw` pelo `Lay Away Fortaleza 1X`!**
   - *(Para `Over 2.5 >= 1.85`, `N = 221`: `Lay Draw` dá **`-5,84u` (`-2,64%`)** vs **`+5,19u` (`+2,35%`)** do `Lay Away 1X`, vantagem de **`+4,99 pp`**. E em `Ago–Set/2026` com `Over 2.5 >= 1.80`, `N = 17`: `Lay Draw` deu `14G / 3R`, **`-0,02u` (`-0,14%`)** vs `Lay Away 1X` **`17G / 0R` (`+1,34u`, `+7,89%`)**).*

---

## 📌 4. Proposta Arquitetural: Chaveamento Exclusivo (`1 Método por Jogo`)

Para **jamais entrar nos dois métodos no mesmo jogo** (o que dobraria a exposição caso a zebra visitante vencesse por `0x1`), propomos o **Chaveamento Mutuamente Exclusivo (`1 método por jogo`)**:

1. **Quando `Liga Nacional` + `Odd_H_Back <= 1.40` + `Odd_Over25_FT_Back >= 1.80` (ou `>= 1.75`) + `4.50 <= Odd_A_Lay <= 15.00`:**
   - $\rightarrow$ **Entra APENAS no `Lay Away Fortaleza 1X` (`5,0%` Liability) e BLOQUEIA o `Lay Draw` naquele jogo!**
   - *(Isso retira o `Lay Draw` da faixa de retranca onde a taxa de empate sobe para `19,2%` e ele dá `-2,03%` de prejuízo, transformando todos os `0x0`/`1x1` em GREEN no `Lay Away 1X`).*
2. **Quando `Odd_Over25_FT_Back < 1.80` (jogo aberto de gols, onde o empate cai para ~`12%`), ou Favorito Visitante (`Odd_A_Back <= 1.40`), ou Copa:**
   - $\rightarrow$ **Mantém APENAS o `Lay Draw (Fav <= 1.40)` (`5,0%` Liability)!**

### 📈 Impacto no Cenário B7 (`40 Planilhas Diárias`, `21/08 a 23/09`, Banca Inicial `R$ 2.000,00`):
- **1. Carteira B7 Atual (`100% Lay Draw`, sem chaveamento):**
  - Banca Final: **`R$ 5.829,67`** (`+191,48%`) | MaxDD: `-30,96%` | Executados: `843` (`769G / 74R`)
- **2. Carteira B7 com Chaveamento `1 Método/Jogo` (`Over 2.5 >= 1.80` $\rightarrow$ troca `Draw` por `Away 1X`):**
  - Banca Final: **`R$ 5.996,22`** (`+199,81%`, **`+R$ 166,55`**) | MaxDD: `-30,96%` | Executados: `855` (`781G / 74R`)
- **3. Carteira B7 com Chaveamento `1 Método/Jogo` (`Over 2.5 >= 1.75` $\rightarrow$ troca `Draw` por `Away 1X`):**
  - Banca Final: **`R$ 6.082,52`** (`+204,13%`, **`+R$ 252,86`**) | MaxDD: **`-30,35%` (`-0,61 pp` menor!)** | Executados: `861` (`787G / 74R`)

---

## ❓ AS 4 PERGUNTAS PARA O VEREDITO DO CLAUDE

1. **Verificação Independente dos Dados (`FRESH3` + `.cache_base_betfair.csv`):**
   Os números do `Lay Away Fortaleza 1X` (`N = 371` em `Over 2.5 >= 1.80` com `+2,99%` a 5% comm / `+3,14%` a 3,5% comm, 6/6 semestres positivos e IC95% excluindo zero) e os números do duelo direto nos `297` jogos sobrepostos com o `Lay Draw` (`Lay Draw` `-2,03%` vs `Lay Away 1X` `+1,72%`) batem na sua verificação em Python?
2. **Mecanismo Físico vs. Risco de Data Mining (Regra do Hall of Shame — Cross-Market):**
   O mecanismo físico identificado — quando um `Super Favorito Mandante (Odd_H <= 1.40)` joga sob uma linha de gols alta/amarrada (`Odd_Over25_FT_Back >= 1.80`), a expectativa de gols da zebra visitante ($\lambda_{\text{Away}}$) colapsa para perto de zero enquanto a frequência de empates (`0x0`/`1x1`) sobe para `19,2%`, fazendo o `Lay Draw` perder `-2,03%` e o `Lay Away 1X` ganhar `+1,72%` — é estatística e estruturalmente sólido na sua visão?
3. **Qual o Melhor Threshold para o Chaveamento (`Over 2.5 >= 1.80` vs `Over 2.5 >= 1.75`)?**
   Você recomenda parametrizar o chaveamento em **`Odd_Over25_FT_Back >= 1.80`** (onde o `Lay Draw` já é nitidamente negativo em `-2,03%` e o IC95% do `Lay Away 1X` exclui o zero) ou **`>= 1.75`**?
4. **Veredito Operacional:**
   Você aprova implementar essa regra de **Chaveamento Exclusivo (`1 método por jogo`)** entre `Lay Draw` e `Lay Away Fortaleza 1X` (`5,0%` Liability) nas Páginas 01, 02 e na Automação Diária, ou sugere mantê-la antes em observação?
