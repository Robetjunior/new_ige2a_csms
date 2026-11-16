-- =============================================================================
-- IGE2A CSMS Next Generation - F3: telemetria NOVO -> ANTIGO (PUSH com outbox)
--
-- Integracao com o CSMS ANTIGO (docs/ARQUITETURA_INTEGRADA_DUAL_CSMS.md, secoes
-- 4.2, 5.3 e 12/F3). Cria uma tabela nova no MESMO schema usado pelo CSMS
-- antigo (`orchestrator`), sem alterar nenhuma tabela existente. Todas as
-- instrucoes sao idempotentes (`if not exists`) e podem ser reaplicadas.
--
-- Semantica (padrao outbox):
--   - o NOVO grava o evento ANTES de envia-lo (at-least-once);
--   - `event_id` e a chave de idempotencia: o ANTIGO deduplica por ele, e a
--     unicidade aqui evita enfileirar o mesmo evento duas vezes;
--   - `payload` e o CORPO exato do webhook (o envelope de telemetria), para que
--     o reenvio assine exatamente o mesmo bytes enviado;
--   - `status` pending -> sent (sucesso) | failed (terminal, apos o teto de
--     tentativas); `next_attempt_at` agenda o backoff exponencial.
--
-- As tabelas charge_meter_sample (serie temporal) pertencem a F4 e NAO fazem
-- parte desta etapa.
-- =============================================================================

create schema if not exists orchestrator;

create table if not exists orchestrator.integration_outbox (
  id                     uuid primary key,
  event_id               text        not null unique,   -- dedupe/at-least-once
  event_type             text        not null,
  integration_session_id text,
  payload                jsonb       not null,          -- corpo exato do webhook
  status                 text        not null default 'pending'
                                     check (status in ('pending', 'sent', 'failed')),
  attempts               integer     not null default 0,
  next_attempt_at        timestamptz not null default now(),
  last_error             text,
  created_at             timestamptz not null default now(),
  updated_at             timestamptz not null default now()
);

comment on table orchestrator.integration_outbox is
  'Outbox de telemetria NOVO->ANTIGO (F3). O NOVO grava o evento antes de enviar; reenvio garante at-least-once.';
comment on column orchestrator.integration_outbox.event_id is
  'Chave de idempotencia do evento (o ANTIGO deduplica por ela). Unica nesta tabela.';
comment on column orchestrator.integration_outbox.payload is
  'Envelope de telemetria (corpo exato do POST /v1/integration/ocpp/events). Reenviado com HMAC sobre o mesmo bytes.';
comment on column orchestrator.integration_outbox.next_attempt_at is
  'Instante a partir do qual o evento pode ser reenviado (backoff exponencial).';

-- Fila de reenvio: recupera apenas o que esta pendente e vencido.
create index if not exists integration_outbox_dispatch_idx
  on orchestrator.integration_outbox (status, next_attempt_at);

-- Correlacao/observabilidade por sessao de negocio.
create index if not exists integration_outbox_session_idx
  on orchestrator.integration_outbox (integration_session_id);
