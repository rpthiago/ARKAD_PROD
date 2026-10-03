# ✈️ Radar de Milhas Smiles & Cias Parceiras

Módulo autônomo de inteligência e monitoramento de passagens aéreas utilizando **Milhas Smiles** para voos operados pela **GOL** e por **Companhias Aéreas Parceiras Internacionais** (Air Europa, TAP Air Portugal, Air France, KLM, American Airlines, Ethiopian Airlines, Turkish, etc.).

---

## 🎯 Objetivo & Vantagem Estratégica

Em voos internacionais, a emissão em dinheiro pode custar caro mesmo em promoção (R$ 4.300 a R$ 6.000+). No entanto, companhias aéreas disponibilizam **assentos na tarifa "Award" (tabelada)** através de programas parceiros. 

O **Radar de Milhas Smiles**:
1. **Identifica Sweet Spots de Milhas:** Detecta quando o resgate atinge o patamar agressivo de milhas (ex: 95.000 a 100.000 milhas o trecho para Europa ou 160.000 milhas para o Japão).
2. **Calcula o CPM Real:** Converte as milhas com base no custo de aquisição do milheiro (`CPM R$ 15,50` por padrão) somado às taxas de embarque oficiais em R$.
3. **Compara com a Tarifa Pagante (Cash):** Informa se vale a pena emitir com milhas ou se compensa pagar em dinheiro.
4. **Dispara Deep Links Diretos:** Envia o link oficial da Smiles (`/mfe/emissao-passagem`) com rota, classe e datas pré-preenchidas para emissão imediata com 1 clique no Telegram.

---

## 🗺️ Rotas Monitoradas & Tetos Agressivos

| Destino | País | Cias Parceiras | Teto Trecho (Ida) | Teto Ida e Volta | Taxa Embarque Ref. |
| :--- | :--- | :--- | :---: | :---: | :---: |
| **MAD** (Madri) 🇪🇸 | Espanha | Air Europa, Iberia | 100.000 milhas | 195.000 milhas | R$ 412,00 |
| **LIS** (Lisboa) 🇵🇹 | Portugal | TAP, Air Europa | 110.000 milhas | 210.000 milhas | R$ 448,00 |
| **LHR** (Londres) 🇬🇧 | Reino Unido | Air France, KLM | 115.000 milhas | 220.000 milhas | R$ 710,00 |
| **IST** (Istambul) 🇹🇷 | Turquia | Turkish, Qatar | 125.000 milhas | 240.000 milhas | R$ 530,00 |
| **NRT** (Tóquio) 🇯🇵 | Japão | Ethiopian, Qatar, ANA | 160.000 milhas | 310.000 milhas | R$ 660,00 |
| **MIA** (Miami) 🇺🇸 | EUA | American Airlines, GOL | 85.000 milhas | 165.000 milhas | R$ 365,00 |
| **SSA** (Salvador) 🏖️ | Brasil | GOL | 16.000 milhas | 30.000 milhas | R$ 68,00 |

*Aeroportos de Origem:* **GRU** (São Paulo), **CNF** (Belo Horizonte), **GIG** (Rio de Janeiro).

---

## 🚀 Como Executar

### 1. Teste de Envio no Telegram
```powershell
python -m buscador_milhas_smiles.main --test
```

### 2. Simulação sem Notificações (Dry Run)
```powershell
python -m buscador_milhas_smiles.main --dry-run
```

### 3. Envio de Resumo Geral
```powershell
python -m buscador_milhas_smiles.main --resumo
```

---

## 🤖 Integração com Telegram

Todos os alertas são direcionados de forma desacoplada para o bot:
- **Bot:** `@thiago_radar_voos_bot`
- **Grupo:** Alertas de Voos ✈️ (`-5476328797`)
- **Configuração:** `telegram_voos_config.json`
