# ✈️ ARKAD — Buscador & Monitor de Voos BH (CNF) ➔ Nordeste

Monitor automatizado de tarifas aéreas com integração ao **Google Flights** e alertas diretos no **Telegram**.

Desenvolvido especificamente para buscar oportunidades reais de voos saindo de **Belo Horizonte (CNF)** para as principais capitais e praias do **Nordeste** no período de alta temporada (**21/12 a 28/12** - Natal e Réveillon).

---

## 🎯 O que o buscador faz:

1. **Varre todas as cidades do Nordeste em tempo real**:
   - 🏖️ **Porto Seguro (BPS)**
   - 🏛️ **Salvador (SSA)**
   - 🌊 **Maceió (MCZ)**
   - 🌴 **Fortaleza (FOR)**
   - ☀️ **Recife (REC)**
   - 🥥 **Ilhéus (IOS)**
   - ☕ **Vitória da Conquista (VDC)**
   - ⛵ **João Pessoa (JPA)**
   - 🦀 **São Luís (SLZ)**
   - 🏖️ **Natal (NAT)**
   - 🦐 **Aracaju (AJU)**
   - 🍇 **Petrolina (PNZ)**
   - ☀️ **Teresina (THE)**

2. **Filtra com inteligência**:
   - Prioriza voos com **no máximo 1 conexão** para evitar viagens exaustivas de 15h–20h com 2+ paradas.
   - Identifica e compara opções de **voos diretos (sem escalas)** vs voos com 1 escala.
   - Coleta a avaliação estatística de preço do Google Flights (`BAIXO`, `NORMAL`, `ALTO`).

3. **Gatilhos para Alertas de "Preço Bom" no Telegram**:
   - 🎯 **Preço Alvo / Teto**: Se a tarifa cair abaixo da meta estipulada para a cidade no `config.json`.
   - 🟢 **Classificação Google "Baixo"**: Se o algoritmo do Google detectar que a tarifa está incomumente barata para o período.
   - 📉 **Queda Súbita**: Se o preço cair R$ 150+ em relação à última cotação registrada.
   - 🛡️ **Anti-Spam com Cooldown**: Evita repetições do mesmo preço; só volta a alertar se o preço cair ainda mais ou após 12 horas.

4. **Link Direto**:
   - Todo alerta no Telegram vem com o botão/link oficial do Google Flights pronto para visualização e compra.

---

## 🚀 Como Executar

### 1. Enviar Resumo / Ranking Completo para o Telegram
Varre todas as cidades e envia a tabela ordenada por menor preço:
```bash
python -m buscador_voos.main --resumo
```
*(ou dê dois cliques em `buscador_voos/executar_resumo.bat`)*

### 2. Checagem Única (Alerta Silencioso)
Executa a varredura e só dispara mensagem no Telegram se encontrar uma oportunidade real:
```bash
python -m buscador_voos.main --check
```
*(ou dê dois cliques em `buscador_voos/executar_check.bat`)*

### 3. Monitoramento Contínuo em Segundo Plano
Fica rodando em loop e checa automaticamente a cada 3 horas (configurável):
```bash
python -m buscador_voos.main --monitor
```
*(ou dê dois cliques em `buscador_voos/executar_monitor.bat`)*

### 4. Consultar Apenas um Destino
```bash
python -m buscador_voos.main --destino SSA
python -m buscador_voos.main --destino BPS
```

### 5. Ver Histórico Salvo no Terminal
```bash
python -m buscador_voos.main --status
```

---

## ⚙️ Configuração Personalizada (`config.json`)

Você pode editar `buscador_voos/config.json` a qualquer momento para:
- Alterar datas (`data_ida`, `data_volta`)
- Mudar preços-alvo de cada destino (`preco_alvo`)
- Ajustar tempo de cooldown e intervalo de monitoramento
