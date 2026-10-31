# PRÉ-REGISTRO — Lay Draw Favorito Pesado · forward STAKE-ZERO

**Data de congelamento:** 2026-10-31
**Autor:** Claude (camada de medição) · aprovado por Thiago Rodrigues
**Status:** forward **zerado** nesta data. Nenhum dado anterior a 31/10/2026 entra na amostra.
**Stake:** **0,00** em todos os registros. Este documento é um instrumento de medição, não de
execução, e não autoriza nem dimensiona aposta.

---

## 1. O que este documento congela

Tudo abaixo é fixo a partir de 31/10/2026. Qualquer alteração **encerra esta amostra** e exige novo
pré-registro com nova contagem a partir de zero. Isto inclui a tabela de conversão da seção 4 — ela
é **anexo fixo**, não parâmetro que o código recalcula. Recalculá-la depois de ver resultados é a
"Troca Silenciosa de Regra" do Hall of Shame.

## 2. Hipótese

No mercado `MATCH_ODDS`, a seleção `The Draw` em jogos com favorito pesado está **sobreprecificada**
o suficiente para que um lay supere o WR de equilíbrio, líquido de comissão.

H0: edge = 0. H1: edge > 0. Teste **unilateral**, alfa = 0,05.

## 3. Regra de sinal — congelada

Um jogo é sinal se, e somente se, **todas** as condições valerem:

1. `min(Odd_Home, Odd_Away) <= 1,40`, pelas colunas de exchange do football-data (`BFEH`, `BFEA`).
   **Exceção de copa:** em competições de copa/eliminatória, apenas `Odd_Home <= 1,40` qualifica.
2. Odd de **lay estimada** do empate `<= 7,00` (teto rígido; ver seção 4).
3. A faixa de odd de back do empate está na tabela da seção 4.
4. A liga consta das ligas do `football_data_odds.csv` (só elas são observáveis).
5. Instante de decisão: **KO−10**, usando a cotação pré-jogo publicada para a partida.

Jogo que falhe qualquer item recebe **SKIP**, registrado com o motivo. A taxa de SKIP e seus motivos
são parte do resultado, não descarte silencioso.

## 4. Fonte de preço e conversão back→lay — congelada

**Fonte única: `BFED`** (preço de back do empate na Betfair Exchange, via football-data). Verificado
contra o acervo em 1.319 jogos: correlação **0,9987**, diferença mediana **+0,000**, MAE 0,023.

**Proibido** usar `B365D`, `AvgD`, `PPD` ou qualquer odd de bookmaker, em qualquer circunstância,
inclusive como fallback. Sem `BFED` → **SKIP**.

**Tabela de conversão congelada** (medida no acervo, Fav <= 1,40, captura mais próxima do KO−10,
região operacional lay <= 7,00):

| faixa de `BFED` | razão `lay/back` | N | P(lay_size >= R$200) |
|---|---:|---:|---:|
| 4,50 – 6,00 | **1,0755** | 387 | 52% |
| 6,00 – 8,00 | **1,0616** | 238 | 65% |

`lay_estimado = BFED × razão_da_faixa`. Faixa fora da tabela → **SKIP**.
SHA256 da tabela: `1cbae42b32e118a11c7e3a3926d82229`

**Viés da conversão, medido fora da amostra** (treino até 23/09, teste depois, bootstrap por bloco de
dia), na região operacional:

| | viés | IC95 |
|---|---:|---|
| com teto lay <= 7,00 | **+0,377 pp** | [+0,184, +0,587] |
| teto + `lay_size` real >= R$200 | +0,351 pp | [+0,203, +0,467] |

O IC **exclui zero**: a conversão é enviesada. **A direção é conservadora** — o WR de equilíbrio
previsto sai *acima* do real, então o edge medido sai **subestimado** em ~0,2 a 0,6 pp.

**Consequência lógica, e é o ponto central deste pré-registro:** esta amostra pode **confirmar** um
edge, nunca **refutar** um. Resultado positivo e significativo vale *a fortiori*, por sobreviver a um
viés que o reduz. Resultado nulo é **inconclusivo**, não é reprovação.

## 5. Três coisas que NÃO foi possível implementar — e por quê

Declarado aqui porque o handoff de 31/10 as previa, e elas não existem.

1. **O gate `P(liq >= R$200) >= 70%` é inatingível e foi retirado.** Erro de origem meu: derivei os
   70% da calibração feita em **todos** os empates, onde a liquidez é boa, e propus o critério sem
   reverificá-lo no universo da regra. Com Fav <= 1,40 o empate é azarão e o livro é fino: na região
   operacional a melhor faixa alcança **65%**, a outra 52%. Nenhuma célula atinge 70%.
2. **A tabela liga × faixa não existe: 0 células com N >= 20.** O universo da regra tem 1.674 jogos
   no acervo; só 174 caem em ligas do football-data; divididos em 59 células, dão ~3 jogos cada. Não
   é limiar mal escolhido, é ausência de dado — e o acervo está congelado, então não há como aumentar.
3. **`lay_size` e spread não são observáveis para frente.** O football-data publica só o preço de
   back. Logo nem piso de liquidez nem corte de spread <= 1,15 podem ser gate forward. Eles
   sobrevivem apenas como característica da tabela da seção 4, e o corte de spread não é vinculante
   na região operacional (dos 98 jogos com lay <= 7,00, **todos** já tinham razão <= 1,15).

## 6. Liquidação

Placar pelas colunas `gh`, `ga` e `res` do **mesmo registro** do `football_data_odds.csv` que forneceu
o `BFED`. Preço e resultado vêm da mesma linha do mesmo arquivo: **não há casamento fuzzy**, e o modo
de falha de placar fabricado (que já custou 5 falsos GREEN em agosto) fica eliminado por construção.

- GREEN (lay vence) se `gh != ga`.
- RED se `gh == ga`.
- Sem placar publicado em 7 dias → `PENDENTE`, e nunca conta como GREEN.

## 7. Contabilidade

- Responsabilidade (liability) = **1 u** por sinal. **ROI sempre por liability**, nunca por stake.
- Comissão **c = 5%** (convenção do projeto, para comparabilidade histórica). Comissão menor em outro
  balcão só melhoraria o resultado; não se altera a convenção no meio da amostra.
- GREEN = `(1−c)/(lay_est−1)` · RED = `−1`.
- WR de equilíbrio: **`BE = (lay_est − 1)/(lay_est − c)`**.
- Edge = `WR_observado − BE_médio`, em pontos percentuais.

## 8. Poder — e o que um resultado nulo significará

Fluxo medido no acervo, restrito às ligas do football-data: **39,7 sinais/mês** com o teto aplicado.
A odd de lay típica é ~6,00, logo `BE ≈ 84,0%`.

| edge real | poder em N=250 | N p/ 80% de poder | meses a 39,7/mês |
|---:|---:|---:|---:|
| +3 pp | **36%** | 922 | 23 |
| +4 pp | 53% | 518 | 13 |
| +5 pp | 70% | 332 | 8 |
| +6 pp | **83%** | 230 | 6 |
| +7 pp | 92% | 169 | 4 |

**Efeito mínimo detectável declarado: +6 pp.** N-alvo **250**, aproximadamente **6,3 meses**.

Fica registrado *antes* de coletar: **um resultado nulo nesta amostra não descarta edge menor que
+6 pp.** Detectar +3 pp exigiria ~23 meses (cerca de 30 com o agrupamento por dia), o que é inviável.
Quem citar "deu nulo, logo não tem edge" estará lendo errado o próprio pré-registro.

## 9. Análise primária — pré-especificada, única

Executada **uma vez**, ao atingir N = 250 liquidados (`PENDENTE` não conta).

1. N, GREENs, REDs, WR observado, BE médio, edge em pp, ROI por liability.
2. IC95 do edge por **bootstrap de bloco de dia**, 4.000 reamostragens.
3. Reportar junto, obrigatoriamente: **"preço de lay estimado; viés conservador de +0,38 pp
   (IC95 [+0,18, +0,59])"**. ROI sem essa nota é resultado incompleto.
4. Taxa de SKIP por motivo.

**Sem análise interina.** Nenhuma decisão antes de N=250. Olhar o acumulado é permitido e esperado
(é stake-zero); **decidir** com base nele, não.

## 10. Regras de decisão

- **CONFIRMADO:** piso do IC95 do edge **> 0** com >= 10 REDs reais. Dado o viés conservador, o edge
  verdadeiro é >= o medido.
- **INCONCLUSIVO:** IC95 contém zero. Não é reprovação (seção 8). Status: *"não confirmado em N=250;
  edge < +6 pp não excluído"*.
- **REPROVADO:** teto do IC95 **< 0**, isto é, edge negativo mesmo com o viés que o favorece.
- **INVIÁVEL:** menos de 15 sinais/mês por 2 meses consecutivos → encerra e registra inviabilidade de
  fluxo.
- Guarda: célula com **menos de 5 REDs** não sustenta conclusão nenhuma.

## 11. Subgrupo — um só, e não decisório

Pré-declarado: faixa `4,5–6,0` vs `6,0–8,0`. **Exploratório e não decisório.** Com poder já marginal
em N=250, qualquer partição o reduz à metade. Reportar as duas células com N e REDs, e **não** usar
nenhuma delas para aprovar, reprovar ou refinar a regra. Nenhum outro corte — liga, dia, posição de
tabela, mês — entra nesta amostra.

## 12. Esquema do ledger (append-only)

`ledger_lay_draw_forward.csv`, uma linha por jogo avaliado, **inclusive SKIP**:

```
data_ko, hora_ko_brt, liga_fd, home, away,
bfeh, bfed, bfea, fav_odd, faixa, razao_aplicada, lay_est,
elegivel (SIM/SKIP), motivo_skip,
stake (sempre 0.00), liability (1.0 se elegivel),
gh, ga, res, resultado (GREEN/RED/PENDENTE), pnl_u,
versao_preregistro (2026-10-31), sha_tabela (1cbae42b...)
```

Gravar `razao_aplicada` e `sha_tabela` em **cada linha** é o que permite auditar depois se a tabela
foi trocada no meio do caminho.

## 13. Rotina de auditoria forward

- **Diária:** anexar os jogos do dia ao ledger, com SKIP e motivo. Stake 0,00.
- **Semanal:** liquidar os `PENDENTE` com placar publicado; reportar N acumulado, SKIP por motivo e
  sinais/mês. **Sem** veredito.
- **Mensal:** checar a regra de inviabilidade (menos de 15 sinais/mês por 2 meses).
- **Em N=250:** rodar a análise da seção 9, uma vez, e registrar o veredito no `worklog.md`.

## 14. O que este pré-registro não pode fazer

1. Não mede preço executado. Mede preço **estimado** a partir do back da exchange.
2. Não serve para decidir uma aposta isolada: o erro por aposta é MAE 0,60 pp contra edge de ordem
   1 pp. Serve para o agregado, onde o erro cai com raiz de N.
3. Não refuta edge pequeno (seção 8), e não refuta edge nenhum se der nulo.
4. Não se estende a CS, O/U, BTTS, HT nem in-play. Esses mercados não têm preço de exchange no feed
   e seguem sem rota forward.
5. Não cobre ligas fora do football-data — e, como o acervo congelou em 30/10, **nenhuma liga nova
   poderá ser calibrada**.

---

Reprodução: `extrai_mo3.py`, `tabela_celulas.py`, `diag_celula.py`, `fluxo_mapeado.py` (em
`ARKAD_PROD`), sobre o backup local do acervo. Base da conversão: `CALIBRACAO_BACK_LAY_claude.md`.
