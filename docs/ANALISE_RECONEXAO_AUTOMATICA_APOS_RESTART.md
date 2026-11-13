# Análise — Reconexão automática dos carregadores após restart do CSMS

Data: 2026-11-13
Escopo: CSMS OCPP 1.6J (Node.js/TypeScript, Fastify 5 + `ws`)
Tipo: análise técnica / especificação (nenhum arquivo de código foi alterado)

> Status do documento: **análise por leitura de código**. Nenhum cenário descrito aqui foi executado.
> Todos os comportamentos citados derivam da leitura do código-fonte do repositório.

---

## 1. Resposta direta

**Sim, é possível restabelecer automaticamente a conexão após reiniciar o CSMS sem reinserir a `websocketURL` — mas quem reconecta é o carregador, nunca o CSMS.**

No OCPP 1.6 o **Central System (CSMS) é o servidor WebSocket** e o **Charge Point (carregador) é o cliente**. O carregador é quem disca para o CSMS, e a URL de destino (`websocketURL`) fica armazenada no firmware/config do próprio carregador. Portanto:

- Reiniciar o CSMS **não apaga** a `websocketURL` do carregador — ela nunca esteve no CSMS.
- Ao subir novamente, o CSMS apenas volta a escutar e **aceita** o próximo `upgrade` que o carregador iniciar.
- O carregador, com sua rotina de reconexão, redisca sozinho e reenvia `BootNotification` + `Heartbeat`.

**Conclusão prática:** o CSMS já está tecnicamente pronto para aceitar a reconexão. O que falta **não é a reconexão em si**, e sim a capacidade de o CSMS *reconhecer* a reconexão e *saber quem não voltou* — porque hoje todo o estado é volátil (em memória).

---

## 2. Evidências no código

| Fato | Evidência |
|---|---|
| CSMS é servidor WebSocket (`noServer: true`); única entrada é `handleUpgrade` (evento `upgrade`) | [ocppWebSocketServer.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/ocppWebSocketServer.ts#L60-L76) |
| Rota fixa `/ocpp/CentralSystemService/{chargePointId}` e identidade extraída da URL do cliente | [ocppWebSocketServer.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/ocppWebSocketServer.ts#L33-L127) |
| Roteamento de `upgrade` por path em `/ocpp/*` | [app.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/app.ts#L109-L130) |
| `BootNotification` sempre responde `Accepted` e define o intervalo de heartbeat | [handlers.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/ocpp/handlers.ts#L53-L63) |
| `Heartbeat` apenas responde `currentTime` | [handlers.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/ocpp/handlers.ts#L65) |
| Estado do CSMS 100% em memória | [connectionManager.ts#L62](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/connectionManager.ts#L62), [chargerState.ts#L60](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/chargers/chargerState.ts#L60), [heartbeatMonitor.ts#L35](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/heartbeat/heartbeatMonitor.ts#L35) |
| Chave de identidade é o `chargePointId` (reconexão não duplica registro) | [connectionManager.ts#L9-L13](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/connectionManager.ts#L9-L13) |
| `connectionId` é novo (UUID) por socket; `reconnectCount` só incrementa se já houve `connectedAt` | [connectionManager.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/connectionManager.ts#L182-L221) |
| Detecção de queda: watchdog de inatividade + ping/pong + `close` | [chargerSession.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/chargerSession.ts#L129-L172) |
| Persistência é somente-observabilidade, opcional e **sem leitura** (só `upsert`) | [persistenceService.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/persistence/persistenceService.ts#L103-L202), [supabaseBatchWriter.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/persistence/supabaseBatchWriter.ts#L80-L97) |
| Sonda de disponibilidade só atua em carregadores **já conectados** | [availabilityProbe.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/availability/availabilityProbe.ts#L212-L216) |

### Efeito do restart hoje

Ao reiniciar, o CSMS perde `records`, `chargers` e `heartbeat` (todos em memória). O primeiro `onConnected` pós-restart cria o registro via `ensure()`, iniciando `reconnectCount: 0` ([connectionManager.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/connectionManager.ts#L91-L119)). Ou seja:

- Do ponto de vista do **socket**, a reconexão automática acontece (o carregador redisca).
- Do ponto de vista do **sistema de registro/supervisão**, o CSMS trata a reconexão como se fosse um carregador novo e não sabe quem faltou.

---

## 3. Condições para a reconexão automática funcionar

1. O endereço da `websocketURL` continua alcançável (DNS estável ou IP inalterado).
2. O carregador possui rotina de reconexão automática habilitada (o simulador do projeto tem: `autoReconnect`, ver [ocpp-simulator.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/scripts/ocpp-simulator.ts#L126-L145)).
3. O caminho de rede está aberto (firewall/NAT/porta) e o CSMS escuta na mesma interface/porta (`HOST`/`PORT`, [env.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/config/env.ts#L66-L71)).
4. Perfil de segurança inalterado: em `wss`, o certificado continua válido/confiável (o CSMS já aceita a variante `socpp1.6` de firmware Teison sob TLS — [ocppWebSocketServer.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/ocppWebSocketServer.ts#L36-L49)).
5. O carregador está ligado e percebe a queda.

---

## 4. Cenários em que NÃO há reconexão automática sem intervenção manual

Nestes casos o CSMS **não pode corrigir remotamente**, porque (a) o CSMS não disca para o carregador e (b) comandos OCPP 1.6 só trafegam em sessão já existente.

| Cenário | Por que não reconecta sozinho | Corrigível pelo CSMS? |
|---|---|---|
| CSMS muda de endereço/porta e a `websocketURL` usa IP/porta fixos | O carregador disca para o endereço antigo | Não |
| Hostname DNS aponta para IP antigo ou LB/proxy mudou de VIP | Resolução DNS leva ao destino errado | Não |
| Firewall/NAT/roteamento bloqueia a porta; isolamento de Wi-Fi | O TCP não chega ao CSMS | Não |
| Carregador sem reconexão (ou desabilitada), ou que só conecta no boot | O cliente não redisca | Não |
| TLS/certificado expirado, cadeia não confiável, pinning incompatível | Handshake TLS falha antes do WebSocket | Parcial (o CSMS renova o certificado, mas o carregador precisa confiar) |
| Carregador desligado / sem energia / sem enlace | Não há cliente ativo | Não |
| Reinício abrupto (SIGKILL) sem FIN limpo | O carregador pode demorar a perceber e só rediscar após sua própria supervisão | Parcial |

O shutdown gracioso do CSMS fecha as sessões com `close(1001)` ([ocppWebSocketServer.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/ocppWebSocketServer.ts#L209-L212), chamado em [app.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/app.ts#L154-L161)), o que **ajuda** o carregador a perceber a queda. Em queda abrupta não existe esse FIN.

---

## 5. Requisitos técnicos (lado do CSMS)

### 5.1 Classificação

| Requisito | Classificação |
|---|---|
| Manter o listener, porta/interface e TLS estáveis entre restarts | NECESSÁRIO |
| Preservar identidade/histórico para *reconhecer* a reconexão (em vez de tratar como novo) | NECESSÁRIO |
| Registro de frota + reconciliação pós-restart | RECOMENDADO |
| Persistir identidade de conexão (última conexão/heartbeat, vendor/model/firmware, intervalo, reconnectCount) | RECOMENDADO |
| Superfície de API/UI/SSE para "conhecido que não reconectou" | RECOMENDADO |
| Ajustar janelas de detecção (watchdog/ping/pong) ao objetivo de recuperação | RECOMENDADO |
| Métricas `prom-client` para perda/atraso de reconexão | OPCIONAL |
| Sonda de disponibilidade induzir reconexão | NÃO É POSSÍVEL (só atua em conectados) |

### 5.2 O que vale (e o que NÃO vale) persistir

**Vale persistir (histórico/identidade — sempre semeado como desconectado):**
- `chargePointId`, `vendor`, `model`, `serialNumber`, `firmwareVersion` (registrados no Boot — [handlers.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/ocpp/handlers.ts#L53-L59)).
- Última conexão (`connectedAt`), último boot (`lastBootAt`), último heartbeat (`lastHeartbeatAt`), último pong (`lastPongAt`), motivo da última desconexão ([connectionManager.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/connectionManager.ts#L30-L50)).
- Intervalo negociado de heartbeat por estação ([heartbeatMonitor.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/heartbeat/heartbeatMonitor.ts#L70-L81)) — hoje é sempre o default do Boot ([handlers.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/ocpp/handlers.ts#L60-L62)).
- `reconnectCount` acumulado (hoje volátil).
- Configuração de conectores como **última observação** ([chargerState.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/chargers/chargerState.ts#L6-L12)).
- Transações abertas **apenas como evidência** para reconciliação ([chargerState.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/chargers/chargerState.ts#L37)).

**NÃO vale persistir/reidratar como estado ativo:**
- `transport: CONNECTED`, `operational: ONLINE` — é física do runtime; após restart deve ser `DISCONNECTED/OFFLINE` até novo `upgrade`.
- `connectionId` corrente, `pendingCommands` e sessões em memória.
- A frota esperada **nunca** deve nascer `ONLINE`.

### 5.3 Papel dos heartbeats (esclarecimento)

- **Heartbeat NÃO dispara reconexão pelo CSMS.** O CSMS apenas responde `currentTime` ([handlers.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/ocpp/handlers.ts#L65)); no OCPP 1.6 não existe mensagem do CSMS que force o carregador a reconectar.
- **Supervisão de conexão** (perceber que o CSMS sumiu e rediscar) é responsabilidade do **carregador**. A **detecção no CSMS** usa socket + heartbeat apenas para *classificar* ([heartbeatMonitor.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/heartbeat/heartbeatMonitor.ts#L126-L134)).
- Regra preservada: **nunca** marcar OFFLINE por ausência isolada de heartbeat ([heartbeatMonitor.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/heartbeat/heartbeatMonitor.ts#L5-L9)); OFFLINE depende do estado do socket, ligado em [app.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/app.ts#L33-L51).
- Uso correto para reconexão: **medir a janela de retorno** — se um carregador conhecido não reconectou em `N × intervalo_negociado + tolerância`, alertar.

### 5.4 Detecção de queda — camadas existentes e lacunas

Existentes:
- Watchdog de inatividade (`OCPP_WATCHDOG_TIMEOUT_MS`, default 300000) → `watchdog_timeout` → `terminate()` ([chargerSession.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/chargerSession.ts#L129-L142)).
- Keepalive ping/pong (`OCPP_WS_PING_INTERVAL_MS`/`OCPP_WS_PONG_TIMEOUT_MS`) → `pong_timeout` → `terminate()` ([chargerSession.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/chargerSession.ts#L144-L172)).
- Evento `close` do socket e `onClosed` por `connectionId` ([chargerSession.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/chargerSession.ts#L363-L375), [connectionManager.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/connectionManager.ts#L277-L292)).

Lacunas:
- **Socket meio-aberto (sem FIN/RST):** o TCP pode permanecer "estabelecido"; mitigado por ping/pong, dependente de parametrização curta o suficiente.
- **Socket vivo sem tráfego OCPP:** coberto pelo watchdog, mas com default longo (300 s) frente ao heartbeat de 60 s.
- **Detecção de "não voltou":** inexistente. Nada diferencia "socket caiu agora" de "carregador conhecido que sumiu no restart".

### 5.5 Reconcilição pós-restart e superfície

- **Frota esperada:** hoje não existe registro de frota nem reconciliação. Proposta: carregar a lista de carregadores conhecidos (config/arquivo de frota e/ou leitura *read-only* das tabelas já existentes), semear cada um como `DISCONNECTED/OFFLINE` e, após uma janela de tolerância, marcar como "conhecido que não retornou" + alertar.
- **Superfície:** enriquecer `/api/overview` (e/ou nova rota de frota) com o estado "aguardando reconexão há X"; publicar evento no SSE `/api/stream`; exibir na UI ([routes.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/http/routes.ts#L117-L135), [#L258-L318](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/http/routes.ts#L258-L318)).

---

## 6. Passos de implementação (incrementais e reversíveis)

Cada passo é reversível e não altera contratos OCPP.

1. **FleetRegistry (feat. flag desligável).** Novo `src/fleet/fleetRegistry.ts`. Fonte de verdade: (A) config/arquivo de frota; ou (B) leitura *read-only* de `charger_heartbeat_monitor`/`charger_connection_events` — o que exige adicionar um caminho de **leitura** (hoje só há escrita em [supabaseBatchWriter.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/persistence/supabaseBatchWriter.ts#L80-L97)). Alterar [components.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/components.ts) e [app.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/app.ts#L53-L82) para construir/carregar antes do `listen`.
2. **Semear identidade sem marcar ONLINE.** Em [connectionManager.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/connectionManager.ts), método de *seed* que pré-cria o registro com `transport: DISCONNECTED`, `operational: OFFLINE`, preservando `lastBootAt/lastHeartbeatAt/vendor/model/firmwareVersion/reconnectCount`. Decidir explicitamente se `connectedAt` é semeado (afeta a semântica de `reconnectCount`). Em [heartbeatMonitor.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/heartbeat/heartbeatMonitor.ts), semear `negotiatedIntervalSeconds` e contadores com `connectionOnline=false`.
3. **ReconnectionWatcher.** Novo `src/fleet/reconnectionWatcher.ts`: timer que compara frota × conectados (`connectedChargePointIds()`, [ocppWebSocketServer.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/ocppWebSocketServer.ts#L205-L207)) e emite `known_charger_awaiting_reconnection` após `N × intervalo_negociado + tolerância`. Ligar start/stop em [app.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/app.ts#L136-L162).
4. **Superfície de leitura (implementado).** `/api/overview` e `/api/chargers` passaram a expor `fleetSummary`; nova rota `GET /api/fleet`; `GET /api/chargers/:id` inclui `fleet`; sondagem direcionada `POST /api/chargers/:id/probe-status`; evento SSE `fleet-status`; UI ([public/app.js](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/public/app.js)) com badge "aguardando reconexão", KPI de frota e botão "Sondar status".
5. **Persistência de identidade (implementado — nova tabela).** Migration [002_charger_fleet.sql](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/db/migrations/002_charger_fleet.sql): `orchestrator.charger_fleet` (identidade + status corrente) e a trilha append-only `orchestrator.charger_fleet_status_events`, alimentada por **gatilhos SQL** (ver §10.1). A **leitura** foi adicionada ao PersistenceService (`loadFleet()`) para semear a frota no startup; a gravação reaproveita o `SupabaseBatchWriter` existente com `onConflict: charge_point_id`. Mantém-se **não crítica**: falha de gravação/leitura nunca derruba o OCPP ([supabaseBatchWriter.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/persistence/supabaseBatchWriter.ts#L5-L9)).
6. **Ajuste de janelas de detecção (implementado).** Defaults aproximados do intervalo de heartbeat (60 s): `OCPP_WATCHDOG_TIMEOUT_MS` 180 s, `OCPP_WS_PING_INTERVAL_MS` 25 s, `OCPP_WS_PONG_TIMEOUT_MS` 10 s ([env.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/config/env.ts#L88-L96)). Reversível por env.
7. **Testes (implementado).** `tests/fleetRegistry.test.ts`, `tests/reconnectionWatcher.test.ts` e `tests/reconnect.integration.test.ts` (inclui o cenário C10 de flapping) — ver §7.
8. **Sincronização runtime → FleetRegistry + estado operacional (implementado).** Novo `src/fleet/fleetStateSyncer.ts`: observador puro que copia, a cada evento de conexão (`attempt/accepted/rejected/closed/watchdog/pong/error`) e a cada mudança de estado do carregador (Boot/Status/Meter), a identidade e o estado operacional observados para o `FleetRegistry` — a persistência apenas lê `fleet.snapshot()`. Sem isso, o registro só era escrito no startup (`seedFleet`) e o `reconnectCount` **real** nunca era gravado. Complementos:
   - `FleetEntry` e `ChargerStateService` ganharam **estado operacional** (`bootAt`, `lastStatusAt`, `connectors`), reidratado no restart por `seedFleet`/`seedIdentity` (sem marcar ONLINE e sem reidratar `activeTransactions`); colunas adicionadas pela migration [003_charger_operational_state.sql](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/db/migrations/003_charger_operational_state.sql).
   - O `PersistenceService` passou a chamar `fleetStateSyncer.sync` antes de gravar e a assinar **também** `chargers.subscribe` (o `StatusNotification` chega DEPOIS do evento de conexão; sem isso a linha da frota ficava defasada até o snapshot periódico).
   - O `SupabaseBatchWriter` ganhou coalescência opcional por chave (`dedupeKey`): várias mudanças do mesmo carregador entre dois flushes geram **uma única linha** no lote, evitando chave de conflito duplicada no upsert do PostgreSQL.

---

## 7. Matriz de testes de validação

Convenções de evidência: `E)` = `charger_connection_events` · `M)` = `ocpp_message_logs` · `C)` = campos de conexão (`connectionId`, `reconnectCount`, estados) · `H)` = heartbeat · `O)` = `/api/overview`.

Parâmetros de referência (defaults de produção — [env.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/config/env.ts#L75-L91)): heartbeat 60 s; tolerância 15 s; critical 120 s; ping 30 s; pong 15 s; watchdog 300 s; probe 60 s / 5000 ms. Nos testes, usar [makeTestConfig](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/tests/helpers.ts#L6-L38) com override para acelerar.

### C1 — Restart GRACIOSO do CSMS (SIGTERM/stop)
- **Setup:** CSMS como processo real (porta fixa, ex. 3100) + simulador conectado e `ACCEPTED`.
- **Gatilho:** `SIGTERM` no CSMS; aguardar subir na mesma porta.
- **E):** `closed` com `disconnectReason='1001:server shutting down'`; após restart, `attempt` → `accepted`.
- **M):** Boot + Heartbeat presentes após restart; ausentes durante a janela desligada.
- **C):** `connectionId` muda; `reconnectCount=0` no novo processo.
- **H):** `SEM_DADOS` → `NORMAL`; `connectionStatus=ONLINE`.
- **O):** HTTP 200; `connected=true`, `transport=CONNECTED`, `registration=ACCEPTED`, `classification=NORMAL`.
- **Sucesso:** reconexão em ≤ 2 × intervalo de heartbeat após o CSMS voltar; zero sessões órfãs.
- **Falha:** não reconecta no orçamento; registro duplicado; `CONNECTED` preso a `connectionId` antigo.

### C2 — Restart ABRUPTO do CSMS (SIGKILL)
- **Gatilho:** `SIGKILL`; reiniciar na mesma porta.
- **E):** **sem** evento `closed` do drop (CSMS estava morto — comportamento correto); após restart `attempt` → `accepted`.
- **M/C/H/O):** equivalentes a C1.
- **Sucesso:** reconexão no orçamento; nenhum `closed` fantasma.
- **Falha:** eventos "impossíveis"; sessão fantasma ao subir.

### C3 — Queda de rede do lado do carregador e retorno
- **Gatilho:** (a) drop com RST/timeout; (b) drop silencioso (ver C5).
- **E):** (a) `closed`; (b) nada imediato (detectado por pong/watchdog). **C):** `connectionId` muda.
- **H):** (a) OFFLINE no close; (b) permanece ONLINE até `pong_timeout`/`watchdog_timeout`.
- **Sucesso:** retomada completa; sem "meia-conexão" permanente; `pendingCorrelations=0`.
- **Falha:** CSMS mantém `CONNECTED` indefinidamente com fluxo parado.

### C4 — Power cycle do carregador
- **Gatilho:** desligar/religar o dispositivo sem reconfigurar `websocketURL`.
- **E):** `closed` ao desligar (se houve FIN) → `attempt`/`accepted` ao religar. **M):** novo BootNotification; Heartbeat retomado. **C):** novo `connectionId`; `reconnectCount` incrementa.
- **Sucesso:** reconexão automática **sem** reinserir `websocketURL`.
- **Falha:** exige reprovisionamento manual; CSMS não registra boot.

### C5 — Socket meio-aberto / sem FIN (queda silenciosa)
- **Gatilho:** cliente completa o handshake e para de responder a pings, mantendo o TCP aberto (config de ping/pong curta por override).
- **E):** `pong_timeout` (preferencial, ~`pingIntervalMs+pongTimeoutMs`) → `terminate()` → `closed`; senão `watchdog_timeout`.
- **Sucesso:** socket morto encerrado em ≤ `pingIntervalMs + pongTimeoutMs`; reconexão ocorre.
- **Falha:** socket permanece `CONNECTED` para sempre.

### C6 — Carregador "vivo" mas PARADO de enviar Heartbeat
- **Gatilho:** sessão aberta que envia Boot + 1 heartbeat e silencia, **respondendo** a pings.
- **E):** nenhum `closed`/timeout (socket vivo). **H):** NORMAL → **ATRASADO** → **CRITICO**, mantendo `connectionStatus=ONLINE`; **nunca OFFLINE**.
- **C):** `transport` permanece `CONNECTED`; `connectionId` inalterado.
- **Sucesso:** evolução ATRASADO/CRITICO observável com `connectionStatus=ONLINE`.
- **Falha:** OFFLINE com socket aberto (viola a regra do monitor).

### C7 — Conexão duplicada do MESMO chargePointId
- **Gatilho:** segunda conexão do mesmo id.
- **E):** `accepted` (com `reconnectCount`) + `replaced`; socket antigo fechado.
- **C):** `connectionId` muda; **um único** registro por id.
- **Sucesso:** 1 registro, 1 sessão ativa; sem duplicidade.
- **Falha:** 2 registros/2 sessões ativas.

### C8 — Reinício do CSMS durante transação aberta
- **Gatilho:** abrir transação (`StartTransaction`) e reiniciar (gracioso e abrupto).
- **M):** Boot/Heartbeat retomados; `StopTransaction` com `transactionId` desconhecido **não deve lançar** ([chargerState.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/chargers/chargerState.ts#L169-L185)).
- **Estado:** `activeTransactions` **perdidas** no restart (volátil); o CSMS **não** recupera a transação automaticamente — OCPP 1.6 não reenvia `StartTransaction`.
- **Sucesso (definível hoje):** CSMS não cai; reconexão + Boot/Heartbeat OK; nenhum protocol error.
- **Lacuna de requisito:** continuidade de transação **não é garantida** pelas três tabelas atuais.

### C9 — Indisponibilidade longa do CSMS (> intervalo de reconexão)
- **Gatilho:** manter o CSMS desligado por 3–5 min; religar.
- **E):** nada durante o downtime; após religar `attempt`/`accepted`. **C):** novo `connectionId`; `reconnectCount=0`.
- **Sucesso:** reconexão em ≤ `maxReconnectDelayMs` + handshake; backoff respeitado (sem tempestade).
- **Falha:** simulador desiste; reconexão em intervalo fixo.

### C10 — Flapping + verificação de vazamento
- **Gatilho:** N ciclos de queda+reconexão com backoff curto.
- **E):** N pares `closed`/`accepted`; `reconnectCount` final = N. **M):** `pendingCorrelations=0` após settle.
- **C):** após settle, **1** registro em `/api/connections`.
- **Métricas:** nº de listeners e de sessões estáveis; heap estável.
- **Falha:** sessões/listeners/heap crescendo; `pendingCorrelations>0`.

### C11 — Teste negativo: carregador apontando para URL/porta errada
- **Gatilho:** porta inexistente / caminho inválido / subprotocolo inválido com fallback desligado.
- **E):** porta errada → **nada** no CSMS; caminho inválido → 404; subprotocolo inválido com fallback off → `rejected` `invalid_subprotocol_1.6` ([ocppWebSocketServer.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/ocppWebSocketServer.ts#L129-L142)).
- **Sucesso:** CSMS não cria sessão para config errônea.
- **Nota:** corrigir a `websocketURL` exige intervenção manual no firmware — documentar como limitação.

### C12 — Skew de relógio entre carregador e CSMS
- **Gatilho:** timestamps do carregador deslocados (±30 min).
- **Evidência:** classificação e latência usam relógio do servidor ([chargerSession.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/chargerSession.ts#L260-L264), [heartbeatMonitor.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/heartbeat/heartbeatMonitor.ts#L94-L114)) → skew não deve gerar ATRASADO/CRITICO falso.
- **Sucesso:** skew não provoca OFFLINE/CRITICO falso nem rejeição de handshake.

### Níveis de teste

| Nível | O que cobre | Automatizar |
|---|---|---|
| Unitário | Classificação de heartbeat, thresholds, `reconnectCount`, `onClosed` por `connectionId` | Sim |
| Integração in-process | `createApp` + simulador: queda por `close()`, supersede, reconexão, pong/watchdog, flapping | Sim |
| E2E de processo | CSMS como processo filho + simulador; `SIGTERM`/`SIGKILL`; reinício na mesma porta | Sim (harness mais pesado) |
| E2E/manual com hardware | Power cycle real, queda de rede real, URL errada no firmware, outages longos, skew real | Não (exige ensaio real) |

### Casos de teste sugeridos (especificação; não escritos)

Não duplicar nomes já cobertos em [transport.integration.test.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/tests/transport.integration.test.ts).

- `tests/reconnect.integration.test.ts`: reconexão com novo `connectionId`, novo Boot e Heartbeat retomado sem erros; heartbeat volta a NORMAL sem OFFLINE indevido; flapping sem vazamento de listeners/timers; `StopTransaction` desconhecido não lança.
- `tests/transport.guards.test.ts` (override de config curto): `pong_timeout` termina socket sem pong; `watchdog_timeout` por inatividade; segunda sessão substitui a primeira com evento `replaced`.
- `tests/serverRestart.e2e.test.ts` (processo filho): restart gracioso (SIGTERM); restart abrupto (SIGKILL); indisponibilidade longa com backoff saturado.
- `tests/heartbeatClassification.test.ts`: skew não afeta classificação; mensagens não-heartbeat atualizam `lastOcppMessageAt` sem falsificar `lastHeartbeatAt`.

---

## 8. Padrões de qualidade e critérios de aceite (mensuráveis)

- **Recuperação:** após o CSMS voltar/falhar, o carregador deve voltar a `CONNECTED`+`ACCEPTED` e registrar Boot + ≥1 Heartbeat em **≤ 2 × intervalo de heartbeat negociado** (produção: ≤ 120 s). O backoff do carregador nunca deve exceder `maxReconnectDelayMs`.
- **Zero sessões órfãs:** após settle, `/api/connections` tem exatamente 1 registro por id ativo, com `connectionId` igual ao da sessão viva; `connectedChargePointIds().length` == nº de conectados.
- **Nenhuma correlação pendente:** `messageLog.stats().pendingCorrelations == 0` após cada cenário.
- **Sem `failed`/`parse_error` indevido:** em reconexão normal, `onlyErrors` = 0.
- **Detecção de socket morto:** em ≤ `pingIntervalMs + pongTimeoutMs` (pong) ou, no pior caso, ≤ `watchdogTimeoutMs`; nunca indefinidamente.
- **Classificação correta:** OFFLINE **somente** via estado de conexão.
- **Continuidade de transação:** nenhum crash em `StopTransaction`/`MeterValues` órfãos; lacuna de persistência documentada.
- **Ausência de vazamento:** após ≥50 ciclos de flapping, sessões/timers/listeners e heap estáveis.
- **Carga:** com N carregadores simultâneos, todos `ACCEPTED`, heartbeat NORMAL, `pendingCorrelations=0`, sem erros de protocolo.

---

## 9. Evidências por execução

Coletar (`/api/*` protegidas por `CSMS_API_KEY` quando configurada — [routes.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/http/routes.ts#L140-L149)):

- **Logs:** stdout do CSMS e stderr do simulador (`[SIM] conexao encerrada (...)`, tentativas).
- **Memória:** `/api/connections`, `/api/heartbeat`, `/api/messages/stats`, `/api/messages/export?format=json|csv`, `/api/chargers/:id/diagnosis`, `/api/stream` (SSE), `/health`.
- **Persistência (quando habilitada):** `orchestrator.charger_connection_events`, `orchestrator.ocpp_message_logs`, `orchestrator.charger_heartbeat_monitor` ([001_ocpp_observability.sql](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/db/migrations/001_ocpp_observability.sql)); `orchestrator.charger_fleet` — identidade + estado operacional (conectores) + status de reconexão — e a trilha `orchestrator.charger_fleet_status_events` ([002_charger_fleet.sql](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/db/migrations/002_charger_fleet.sql), [003_charger_operational_state.sql](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/db/migrations/003_charger_operational_state.sql)).
- **Dispositivo (hardware):** terminal do firmware e captura de rede quando aplicável.

### Template de relatório de evidência

```
ID do cenário:            [C1..C12]
Ambiente:                 [in-process | processo real | hardware]
Versão/commit:            [hash + firmware]
Config relevante:         [watchdog/ping/pong/tolerância/critical/heartbeat; persistência on/off]
Pré-condições:            [ids, sessões aceitas, transação aberta?]
Gatilho + horário:        [ação, timestamp]
Entrada/observado:
  - charger_connection_events: [eventos com timestamps e connectionId]
  - ocpp_message_logs:          [Boot? Heartbeat? failed/parse_error? uniqueIds]
  - reconexão:                  [connectionId anterior -> novo; reconnectCount; delta de tempo]
  - heartbeat:                  [classificação antes/depois; connectionStatus]
  - /api/overview:              [HTTP status; connected/transport/registration]
Resultado esperado:       [...]
Resultado obtido:         [...]
Métricas:                 [tempo de reconexão; pendingCorrelations; listeners/sessões; latência]
Veredito:                 [APROVADO | FALHA | BLOQUEADO]
Defeitos abertos:         [ID + descrição + severidade + teste de regressão proposto]
Observações/limites:      [ex.: evidência de hardware ausente]
```

---

## 10. Achados consolidados

| # | Achado | Classificação |
|---|---|---|
| 1 | CSMS é servidor WS; carregador é cliente que disca; CSMS não disca para o carregador | NECESSÁRIO (fato de protocolo) |
| 2 | A reconexão do socket já ocorre por iniciativa do carregador, nas condições da seção 3 | NECESSÁRIO (habilitador) |
| 3 | Estado 100% em memória; restart perde histórico | NECESSÁRIO corrigir para *reconhecer* a reconexão |
| 4 | `chargePointId` é a chave de identidade (já não duplica) | NECESSÁRIO (já existe) |
| 5 | Registro de frota + reconciliação pós-restart | RECOMENDADO |
| 6 | Persistir identidade de conexão | RECOMENDADO |
| 7 | Persistência atual é só-escrita, opcional e de observabilidade | RECOMENDADO (adicionar leitura se virar fonte de frota) |
| 8 | Heartbeat não dispara reconexão; serve para classificação/detecção | NECESSÁRIO (esclarecer) |
| 9 | Detecção de queda por watchdog + ping/pong + close | NECESSÁRIO (já existe) |
| 10 | Janelas default longas frente ao intervalo de heartbeat | RECOMENDADO ajustar |
| 11 | Superfície API/UI para "conhecido que não reconectou" | RECOMENDADO |
| 12 | Sonda de disponibilidade só atua em conectados (não induz reconexão) | OPCIONAL (limitação conhecida) |
| 13 | Fechamento gracioso envia `close(1001)` (ajuda o carregador a perceber) | OPCIONAL (já existe) |
| 14 | Métricas `prom-client` para perda/atraso de reconexão | OPCIONAL |
| 15 | Cenários de não-reconexão (URL/IP/DNS/firewall/TLS/carregador off) fora do alcance do CSMS | NECESSÁRIO reconhecer |
| 16 | Gatilhos SQL mantêm `updated_at`/`first_seen_at` e registram **cada transição de status** da frota em trilha append-only | RECOMENDADO |
| 17 | Sondagem OCPP **direcionada** (`TriggerMessage` → `StatusNotification`) para consultar o status sob demanda | RECOMENDADO |

### 10.1 Gatilhos SQL e status do carregador (implementado)

A migration [002_charger_fleet.sql](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/db/migrations/002_charger_fleet.sql) responde à pergunta "qual o status do carregador e quando mudou" **sem depender de log da aplicação**:

| Objeto | Tipo | Papel |
|---|---|---|
| `orchestrator.charger_fleet` | tabela | identidade + status corrente (`CONNECTED`/`AWAITING_RECONNECTION`/`UNKNOWN`) por `charge_point_id` |
| `charger_fleet_touch` | gatilho BEFORE INSERT/UPDATE | mantém `updated_at` (e `first_seen_at` no INSERT) |
| `charger_fleet_status_change` | gatilho AFTER INSERT/UPDATE OF `status` | grava a transição (`previous_status` → `new_status`) em `charger_fleet_status_events` |
| `charger_fleet_status_events` | tabela append-only | trilha de auditoria das transições (com `metadata` e `changed_at`) |
| `charger_fleet_status` | view | deriva o status por tempo (`last_message_at >= now() - intervalo×3`) |
| `prune_charger_fleet_status_events(keep_days)` | função | retenção da trilha (default 90 dias) |

Complemento em tempo real, do lado OCPP: a rota `POST /api/chargers/:id/probe-status` pede **somente** `StatusNotification` via `TriggerMessage` e devolve o `outcome` + os estados dos conectores. As duas vias são complementares — o gatilho registra o que o runtime observou; a sondagem obtém o estado reportado **agora** pelo carregador.

### 10.2 Estado da implementação

**IMPLEMENTADO** (código/schema alterado): passos 1–8 da §6, gatilhos SQL da §10.1, superfície de frota (API + SSE + UI) e sondagem direcionada. Inclui o elo que faltava para a persistência ser útil de fato: o `FleetStateSyncer` (runtime → `FleetRegistry`), o estado operacional persistido (`bootAt`/`lastStatusAt`/`connectors`, migration 003) e a coalescência por chave no batch writer.

**VALIDADO** (comportamento verificado por execução): `npm run typecheck` (0 erros) e `npm test` (**57/57**). Além de `fleetRegistry`, `reconnectionWatcher` e do cenário **C10 (flapping)**, cobrem esta entrega:
- `tests/chargerState.test.ts` — seed do estado operacional e não-regressão de campos conhecidos.
- `tests/fleetStateSyncer.test.ts` — as reconexões **reais** (`reconnectCount`) chegam ao `FleetRegistry`; o estado operacional propaga; `syncAll`/`stop`.
- `tests/persistence.test.ts` — coalescência por chave (`dedupeKey`) mantém uma linha por `chargePointId` no lote.
- `tests/persistenceRestart.integration.test.ts` — ciclo real **grava → encerra → reinicia → lê a frota** com cliente Supabase falso em memória: após o restart o carregador conhecido é reidratado (`reconnectCount` 2 preservado, conectores reidratados, status `DISCONNECTED`), é classificado como `AWAITING_RECONNECTION` quando não retorna, e ao reconectar é **aceito automaticamente** continuando o histórico (2 → 3). Cobre também desconexão abrupta (`terminate()`) e múltiplas reconexões com registro único.

**NÃO VALIDADO**: os cenários que exigem hardware real (C4/C11/C12), o restart de **processo** com sinal (C1/C2/C9, `tests/serverRestart.e2e.test.ts` ainda proposto) e a persistência real de ponta a ponta contra um Supabase provisionado — o teste de reinício usa um cliente falso em memória, portanto **não** exercita a PostgREST real nem os gatilhos SQL das migrations 002/003.

> Diferença preservada: **IMPLEMENTADO** = código/schema alterados; **VALIDADO** = comportamento verificado nesta sessão; **PUBLICADO** = implantado em ambiente (não houve publicação).


### Riscos / trade-offs
- **Reidratar cedo demais** pode criar "estado fantasma" (ONLINE sem socket) → semear sempre `OFFLINE/DISCONNECTED`.
- **Interação com `ensure()`**: `onAttempt/onConnected` chamam `ensure()` ([connectionManager.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/connectionManager.ts#L91-L119)); sem cuidado, um `onAttempt` de id inválido pode poluir a frota.
- **`reconnectCount` após restart**: decisão de produto — continuar do acumulado (exige persistir) ou reiniciar (perde histórico).
- **Transações**: reidratar como "viva" pode gerar inconsistência → usar só como evidência.
- **Orçamento de memória**: `maxRecords=5000` com evição ([connectionManager.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/connectionManager.ts#L121-L127)); frota grande consome esse orçamento.
- **Persistência opcional**: sem credenciais Supabase o serviço fica desabilitado ([persistenceService.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/persistence/persistenceService.ts#L103-L108)) → a frota precisa de fallback por config.

---

## 11. Incertezas e encaminhamentos

- **Nada foi executado** nesta análise; os passos e testes são propostas.
- O comportamento real de reconexão do firmware Teison físico **não é comprovado** por este repositório; apenas o simulador implementa `autoReconnect`. A validação com hardware exige janela própria.
- A frota/identidade persistida depende de credenciais Supabase; sem elas, é necessária fonte alternativa (config/arquivo).
- **Encaminhamentos sugeridos:** Database Specialist (decisão de schema para identidade de conexão/transação), DevOps/Infra (estabilidade de `HOST/PORT`/proxy TLS entre restarts), Backend/Frontend (passos 1–5 e superfície), QA (cenários da seção 7).
