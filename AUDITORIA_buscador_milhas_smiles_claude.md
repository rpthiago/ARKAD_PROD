# AUDITORIA — Buscador & Radar de Milhas Smiles — Claude, 2026-10-02

Lido: `smiles_scanner.py`, `main.py`, `config_smiles.json`, `historico_smiles.json` (97 chaves),
`execucoes_smiles.log`, e os caches de origem (`buscador_voos/historico_precos.json`,
`buscador_voos/config.json`).

## Resumo

A engenharia está boa: separação de módulos, cooldown, ranking por economia, deep-links, execução em
<0,2s por não tocar na Smiles. O conserto do caso JPA foi correto — indexar à tarifa real matou o falso
green. **Mas o módulo hoje não é um radar de oportunidade: é um conversor de preço em dinheiro para
milhas, com alerta disparado por um gatilho que quase sempre está verdadeiro.** Abaixo, o que encontrei,
do mais grave para o menos.

---

## 1. Falhas que mudam o resultado

### 1.1 As milhas internacionais são uma CONSTANTE — o alerta não vem delas
`milhas = milhas_base_trecho × 1,92`. Para GRU→MAD isso é `98.400 × 1,92 = 188.928` **sempre**, em
qualquer data. O histórico confirma: três datas diferentes (10/05, 20/05 e 05/06 de 2027) gravadas com
`menor_milhas: 188928` e `custo_reais: 3752.38` idênticos.

Consequência: o teto (195.000 para MAD) está sempre satisfeito, e a única variável do gatilho é o preço
em dinheiro do Google Flights. **O módulo internacional é um monitor de tarifa paga com roupa de radar de
milhas.** Ele nunca pode detectar uma promoção de milhas, porque não observa milhas.

### 1.2 O multiplicador 1,92 para ida e volta não tem origem
Emissão award não dá 4% de desconto no round-trip; normalmente é 2× o trecho, ou tabela própria. O mesmo
vale para o `preco * 0.55` usado para estimar "somente ida" no nacional. São dois números mágicos que
entram direto na conta de economia.

### 1.3 O modelo de R$ 18,00/milheiro foi calibrado em UMA observação
Pelo próprio relato: observou-se JPA com tarifa R$ 1.940–2.425 e Smiles cobrando 53.000–83.500 milhas por
trecho (106k–167k ida e volta). O modelo devolve `1.940/18×1000 = 107.777` — acerta a borda inferior e
erra a superior em até **55%**. Um parâmetro ajustado em n=1 é exatamente o erro que o GEMINI.md combate
("bootstrap de 1 bloco aprova sozinho"). Hoje não existe nenhum registro do erro do modelo.

### 1.4 Não há verdade de campo: o sistema nunca confere o que a Smiles cobrou
Esta é a falha estrutural. Todo alerta é uma **previsão**, e nenhuma previsão é confrontada com a
realidade. É o mesmo padrão do false-green do coletor: um pipeline que só confirma a si mesmo. Sem isso,
não dá para dizer se o conserto do JPA melhorou a precisão — só que calou aquele alerta específico.

### 1.5 Acoplamento por coincidência de datas (bug latente, hoje inativo)
`obter_preco_google_flights` para o Nordeste pega `consultas[-1]` do cache e valida **apenas a idade
(<24h)**. O campo `data` dessa consulta é o carimbo de quando a busca rodou ("2026-10-02 12:30"), **não a
data da viagem**. Funciona hoje porque `buscador_voos/config.json` monitora exatamente 21/12→28/12. No dia
em que alguém mudar a data daquele monitor, este módulo passa a precificar milhas do Réveillon com tarifa
de outra data — **sem erro, sem aviso**. O internacional não tem o problema (a chave inclui `data_ida`).

### 1.6 O limite de 6 alertas não valeu em duas execuções
O log mostra **56 alertas enviados** às 14:46 e **35** às 15:06, com `max_alertas_por_execucao = 6`. Só a
execução das 15:29 respeitou o limite. Vale investigar se o corte `candidatos_alerta[:max_alertas]` está
sendo aplicado depois de algum loop por origem/data que reenvia.

### 1.7 Preço inventado no fallback
`return float(int(res_gf.menor_geral.price * 1.25)), "GOL", ...` — quando não há voo GOL, o código assume
que a GOL custaria 25% a mais que o menor do mercado. Esse número vira milhas, vira economia e vira
alerta. Se não há GOL, o certo é não cotar.

---

## 2. Respostas diretas aos seus 5 eixos

### (1) O modelo R$ 18,00/milheiro
Não recomendo curva não-linear agora — recomendo **medir antes de modelar**. Uma curva com 2 ou 3
patamares ajustada nos mesmos zero dados só adiciona parâmetros. O caminho honesto:
- registrar, para cada alerta aberto, **as milhas que a Smiles realmente pediu** (uma linha, na mão);
- com 20–30 pares (tarifa, milhas reais) dá para ver se a relação é linear e qual o R$/milheiro real;
- só então decidir entre constante e patamares.
Suspeita a testar: a Smiles costuma ter **piso** (tarifa baixa não cai proporcionalmente) e **teto por
disponibilidade**, o que produziria uma curva achatada nas pontas — mas isso é hipótese, não conclusão.

### (2) Os sweet spots internacionais
Dois problemas de domínio:
- **Air France para LHR** não existe saindo do Brasil. Seria GRU→CDG→LHR, e a Smiles tarifa **por região**
  (Europa), não por companhia. O mesmo vale para Ethiopian→NRT (GRU→ADD→NRT, tarifa "Ásia").
- Mais importante: a Smiles **não publica tabela award fixa** para parceiras hoje; o custo varia por
  disponibilidade de classe award. Uma tabela estática de 98.400/108.900/112.000 não tem como estar certa
  em todas as datas — e, como mostrei em 1.1, ela nunca muda.
Sugestão: tratar o internacional como **faixa**, não ponto (ex.: "Europa econômica: 90k–150k por trecho"),
e alertar só quando o preço em dinheiro cair abaixo de um limiar — deixando claro na mensagem que as
milhas são estimativa não verificada.

### (3) Resiliência e coleta
Indexar pelo cache local é a decisão certa para não apanhar do Imperva. Não tenho um endpoint público da
Smiles para recomendar, e tentar burlar WAF é caminho ruim (bloqueio de IP e termos de uso). A alternativa
que funciona sem risco é **humana e barata**: quando um alerta interessar, você abre o deep-link de
qualquer jeito — basta registrar o que viu. Isso resolve o item 1.4 de graça.

### (4) Mensagem do Telegram
A mensagem está completa, mas vende certeza que o sistema não tem. Três ajustes:
- marcar explicitamente o que é **estimado** vs **verificado** (as milhas são estimadas; o preço em
  dinheiro é observado);
- trocar "Economia R$" por **"economia SE você tiver as milhas a R$ 15,50/milheiro"** — hoje é uma conta
  contábil, não dinheiro no bolso;
- incluir **data da cotação em dinheiro** usada, para você saber se o número é de hoje ou de ontem.

### (5) Próximos passos, na ordem em que eu faria
1. **Log de verdade de campo** (`observacoes_smiles.csv`: data, rota, milhas estimadas, milhas reais,
   taxas reais) e um relatório semanal de erro — viés e erro médio. Sem isso, nada mais vale.
2. **Corrigir o acoplamento de datas** (1.5): gravar `data_ida`/`data_volta` na consulta do cache e exigir
   que batam. Uma linha, elimina um falso green silencioso futuro.
3. **Fazer o internacional variar** ou admitir que é monitor de tarifa: enquanto as milhas forem
   constantes, desligar o alerta por milhas e manter só o de preço em dinheiro.
4. **Medir disponibilidade de assento award**, que hoje não existe no modelo. Em 21–28/12 é o fator
   dominante: preço baixo sem assento é alerta inútil. Métrica simples: dos alertas abertos, quantos
   tinham assento.
5. Só depois disso, refinar a curva de milhas da GOL.

---

## 3. O que está bom e deve ser mantido
- Não bater na Smiles (sem scraping, sem risco de bloqueio) e rodar em <0,2s.
- Cooldown de 12h com quebra por queda de 5.000 milhas — regra sensata.
- Ranking por economia financeira e deep-links duplos (I/V e só ida).
- O conserto do JPA: indexar à tarifa real foi a decisão certa e matou um falso green real.

## 4. O ponto de filosofia (GEMINI.md)
O repositório aprendeu, com dinheiro, que **pipeline que só se confirma a si mesmo infla resultado** —
foi assim com o false-green do coletor e com a odd de CS inflada no paper log. Este módulo está nesse
estágio: ele produz números plausíveis que ninguém nunca conferiu. A diferença é que aqui o custo do erro
não é perder aposta, é perder uma viagem de Réveillon — o que, para a casa, é pior.
