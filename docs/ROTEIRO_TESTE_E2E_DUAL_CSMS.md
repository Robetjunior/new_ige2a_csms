# Roteiro de Teste E2E — Dual CSMS (NOVO ⇄ ANTIGO)

Escopo: fechar o ciclo **start real → meter → stop → cobrança pós-paga** com o
CSMS **NOVO** (borda OCPP, `NEW_IGE2A_CSMS`, `:3100`) e o CSMS **ANTIGO**
(negócio/pagamento, `IGE2A_CSMS`, `:3000`).

> Este roteiro é para execução **manual na VM GCP** (`ige2a-charger-v2`, Debian 13).
> Todos os passos são read-only ou idempotentes, exceto onde indicado.

## 0. Pré-requisitos

- NOVO em `/home/ige2a_plataforma/new_ige2a-csms` (PM2 `csms-next`, `:3100`).
- ANTIGO em `/opt/ige2a-csms` (PM2 `csms`, `:3000`).
- Acesso ao Supabase (schema `orchestrator`) de cada CSMS.

### 0.1 Migrations

| CSMS | Arquivo | Estado |
| --- | --- | --- |
| ANTIGO | `supabase/migrations/20261117_integration_event_inbox.sql` | já aplicado (inbox) |
| ANTIGO | `supabase/migrations/20261117_integration_session_correlation.sql` | **aplicar agora** (correlação F5) |
| NOVO | `db/migrations/006_charge_meter_sample.sql` | aplicar (série temporal F4) |

### 0.2 Variáveis de ambiente (segredo HMAC compartilhado)

O `INTEGRATION_HMAC_SECRET` deve ser **idêntico** nos dois CSMS.

NOVO (`/home/ige2a_plataforma/new_ige2a-csms/.env`):

```
INTEGRATION_PUSH_ENABLED=true
INTEGRATION_TARGET_URL=http://127.0.0.1:3000
INTEGRATION_HMAC_SECRET=<segredo-compartilhado>
INTEGRATION_PUSH_INTERVAL_MS=5000
INTEGRATION_PUSH_MAX_ATTEMPTS=8
INTEGRATION_PUSH_CIRCUIT_FAILURE_THRESHOLD=5
INTEGRATION_PUSH_CIRCUIT_OPEN_MS=30000
```

ANTIGO (`/opt/ige2a-csms/.env`):

```
INTEGRATION_HMAC_SECRET=<mesmo-segredo-compartilhado>
INTEGRATION_EVENT_MAX_AGE_MS=0
INTEGRATION_EVENT_MAX_FUTURE_SKEW_MS=300000
```

> `INTEGRATION_TARGET_URL` aponta para o ANTIGO. Se os dois CSMS estiverem na
> mesma VM, use `http://127.0.0.1:3000`; caso contrário, o host/IP do ANTIGO.

### 0.3 Chaves de integração (delegação de comandos ANTIGO → NOVO)

O ANTIGO **não é mais dono do socket OCPP**: ao iniciar/encerrar uma recarga ele
delega o comando ao NOVO (rota `new_csms`). Isso exige credenciais alinhadas
entre os dois CSMS:

| Uso | NOVO (`.env`) | ANTIGO (`.env`) | Regra |
| --- | --- | --- | --- |
| Comando start/stop | `INTEGRATION_API_KEY` | `NEW_CSMS_INTEGRATION_KEY` | **devem ser idênticas** |
| Leitura do roster | `CSMS_API_KEY` | `NEW_CSMS_API_KEY` | **devem ser idênticas** |
| URL do NOVO | — | `NEW_CSMS_BASE_URL` | ex.: `http://127.0.0.1:3100` |

> `INTEGRATION_API_KEY` vazio no NOVO ⇒ a rota F2 responde **503
> `integration_disabled`**; divergente ⇒ **401 `unauthorized`**. O ANTIGO
> traduz 401/503 para o erro `new_csms_unauthorized` e 409 `not_connected`
> para `charger_offline`.

## 1. Deploy

O deploy automático do ANTIGO está **quebrado** (GitHub Actions): o deploy é
**manual** até ser corrigido.

```bash
# ANTIGO (manual)
cd /opt/ige2a-csms
git pull
npm ci
npm run build          # tsc
pm2 reload csms

# NOVO
cd /home/ige2a_plataforma/new_ige2a-csms
git pull
npm ci
npm run build
pm2 reload csms-next
```

## 2. Smoke de subida

```bash
pm2 describe csms-next | grep -E 'status|restarts'
pm2 logs csms-next --lines 40 | grep -i integration   # esperado: "modulo iniciado (push=on)"
pm2 logs csms     --lines 40 | grep -i integration
```

Assinatura/roteamento do receptor (deve responder 401 sem `x-signature`):

```bash
curl -s -o /dev/null -w '%{http_code}\n' -X POST \
  -H 'content-type: application/json' \
  -d '{"eventId":"probe"}' \
  http://127.0.0.1:3000/v1/integration/ocpp/events
# esperado: 401
```

## 3. Fluxo E2E — start real

1. No app / rota de start pós-pago do ANTIGO, iniciar uma sessão em um
   carregador **conectado** no NOVO.
2. O ANTIGO resolve a rota do comando (`resolveCommandRoute`): sem socket local
   nem owner distribuído, e estando o carregador conectado no NOVO, a rota é
   `new_csms`. O start é **delegado** ao NOVO via
   `POST {NEW_CSMS_BASE_URL}/integration/v1/sessions/start` (header
   `x-integration-key`), com `integrationSessionId` derivado da `idempotencyKey`
   (`is-<sha256>`); a resposta traz `commandRoute: 'new_csms'`.
3. O NOVO envia `RemoteStartTransaction` (dono do socket OCPP) e, quando o
   carregador responde `StartTransaction`, emite `session.started` com o
   `ocppTransactionId` (autoridade de fio do NOVO, ADR-02).
4. O ANTIGO, no handler F5, **cria a sessão de negócio** (o ANTIGO não recebe
   mais `StartTransaction`): `sessions.transaction_id = ocppTransactionId`,
   recuperando `userId`/`billingModel` do contexto do remoteStart pós-pago; em
   seguida grava a correlação `state='started'`.

Verificar a delegação (o start saiu do ANTIGO como comando ao NOVO):

```bash
pm2 logs csms --lines 60 | grep -i 'new_csms\|sessions/start'
# esperado: registro de start delegado (commandRoute new_csms)
```

Verificar a correlação no Supabase do ANTIGO:

```sql
select integration_session_id, charge_point_id, ocpp_transaction_id,
       business_session_id, state, meter_start_wh, started_at
from orchestrator.integration_session_correlation
order by started_at desc
limit 5;
-- esperado: state='started', ocpp_transaction_id preenchido, business_session_id preenchido
```

Verificar a série temporal no Supabase do NOVO:

```sql
select count(*) from orchestrator.charge_meter_sample;   -- deve crescer durante a sessão
```

## 4. Fluxo E2E — stop + cobrança

1. Encerrar a sessão (app / parada local do carregador).
2. O NOVO emite `session.stopped` com `meterStop` (verdade de energia, ADR-02).
3. O ANTIGO (`integrationSessionEventHandler`) resolve a sessão de negócio,
   chama `forceCompleteTransactionIfStillActive` (com `meterStopWh` do NOVO) e
   dispara a cobrança pós-paga.
4. A correlação passa para `state='finalized'`.

```sql
select integration_session_id, state, meter_stop_wh, stopped_at
from orchestrator.integration_session_correlation
order by updated_at desc
limit 5;
-- esperado: state='finalized', meter_stop_wh preenchido
```

Cobrança (não deve haver pendência):

```sql
select id, transaction_id, status, meter_stop_wh, energy_kwh, ended_at
from sessions
where transaction_id = <ocpp_transaction_id>
order by started_at desc;
-- esperado: status='completed', ended_at preenchido
```

Resumo do pagamento: `billingModel=postpaid` e `status=succeeded`.

## 5. Critérios de aceite

- [ ] O start do ANTIGO é **delegado** ao NOVO (`commandRoute='new_csms'`); o ANTIGO não envia `RemoteStartTransaction` diretamente.
- [ ] A sessão de negócio (`sessions`) é **criada no `session.started`** com `transaction_id` = `ocpp_transaction_id` do NOVO.
- [ ] `session.started` cria a correlação (`state='started'`).
- [ ] `meter.sampled` popula `charge_meter_sample` no NOVO.
- [ ] `session.stopped` encerra a sessão (`status='completed'`) e gera cobrança.
- [ ] Correlação termina em `state='finalized'` com `meter_stop_wh` do NOVO.
- [ ] Nenhum `eventId` fica preso no inbox (todos `processed`).

## 6. Resiliência (F6)

Com o ANTIGO parado (`pm2 stop csms`):

- O outbox do NOVO acumula eventos e o circuit breaker abre após
  `INTEGRATION_PUSH_CIRCUIT_FAILURE_THRESHOLD` falhas.
- Ao religar (`pm2 start csms`), o circuito sonda (`half_open`) e reenvia;
  nenhum evento é perdido (idempotência por `eventId` no inbox).

```sql
-- NOVO: fila do outbox
select state, count(*) from orchestrator.integration_outbox group by state;
```

## 7. Troubleshooting

| Sintoma | Verificar |
| --- | --- |
| `{"error":"integration_disabled"}` no NOVO | `INTEGRATION_PUSH_ENABLED=true` e reload do `csms-next` |
| 401 no receptor | `INTEGRATION_HMAC_SECRET` divergente entre os CSMS |
| 503 `INTEGRATION_NOT_CONFIGURED` | `INTEGRATION_HMAC_SECRET` ausente no ANTIGO |
| Correlação sem `business_session_id` | sessão de negócio não estava ativa no momento do stop |
| Sessão não encerrou | `ocppService` indisponível no ANTIGO (log `encerramento adiado`); o outbox reenvia |

## 8. Pendências conhecidas

- Deploy automático do ANTIGO (GitHub Actions) quebrado → deploy manual.
- Rotacionar `CSMS_API_KEY` / `INTEGRATION_API_KEY` (SEC-1/SEC-2).
