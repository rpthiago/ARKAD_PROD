# 🌍 ARKAD — Monitor de Voos Internacionais Agressivos (10 Dias)
## 🇪🇸 Espanha | 🇵🇹 Portugal | 🇬🇧 Inglaterra | 🇹🇷 Turquia | 🇯🇵 Japão

Monitor inteligente de tarifas aéreas com varredura contínua de viagens de **10 dias** saindo de **São Paulo (GRU)**, **Belo Horizonte (CNF)** e **Rio de Janeiro (GIG)** com alertas no **Telegram**.

---

### 🎯 Destinos Monitorados e Tetos de Preço Agressivo:

| País / Região | Destinos | Teto Agressivo (Ida e Volta com taxas) |
| :--- | :---: | :---: |
| 🇪🇸 **Espanha** | Madri (`MAD`) e Barcelona (`BCN`) | **≤ R$ 3.200** |
| 🇵🇹 **Portugal** | Lisboa (`LIS`) e Porto (`OPO`) | **≤ R$ 3.200** |
| 🇬🇧 **Inglaterra** | Londres (`LHR`) | **≤ R$ 3.500** |
| 🇹🇷 **Turquia** | Istambul (`IST`) | **≤ R$ 4.500** |
| 🇯🇵 **Japão / Ásia** | Tóquio (`NRT` / `HND`) | **≤ R$ 5.500** |

---

### 🧠 Como Funciona o Monitor:

1. **Amostragem de 10 dias ao longo de 6 meses:**
   - Varre janelas de 10 dias (ex: dias 10 a 20 e 20 a 30) em cada mês nos próximos 6 a 8 meses.
2. **Conexões e Escalas Permitidas:**
   - Analisa tanto voos diretos quanto voos com conexões vantajosas (ex.: LATAM, Air Europa, Iberia, TAP, British Airways, Turkish Airlines, Emirates, Qatar, Ethiopian, United).
3. **Filtro Severo Anti-Ruído:**
   - Se o preço estiver na faixa comum (ex: R$ 5.000 para Madri ou R$ 8.500 para o Japão), o robô permanece em silêncio absoluto.
   - Só acorda e manda notificação no Telegram se alguma tarifa **romper o teto agressivo** ou se houver um desconto gigante.
4. **Link Direto:**
   - O alerta no Telegram vem com o link direto no Google Flights pronto para visualização e compra.

---

### 💻 Como Usar

* **Checagem Única (Alerta no Telegram se houver promoção):**
  ```powershell
  python -m buscador_voos_internacional.main --check
  ```
  *(ou clique duas vezes em `executar_check.bat`)*

* **Enviar Ranking Atualizado de Todos os Destinos para o Telegram:**
  ```powershell
  python -m buscador_voos_internacional.main --resumo
  ```
  *(ou clique duas vezes em `executar_resumo.bat`)*

* **Ver Histórico Salvo no Terminal:**
  ```powershell
  python -m buscador_voos_internacional.main --status
  ```
