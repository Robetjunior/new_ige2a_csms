-- =============================================================================
-- IGE2A CSMS Next Generation - Observabilidade OCPP (spec §6)
--
-- Cria as tres tabelas exigidas no mesmo schema usado pelo CSMS antigo
-- (`orchestrator`), sem alterar nenhuma tabela existente. Todas as instrucoes
-- sao idempotentes (`if not exists`) e podem ser reaplicadas com seguranca.
--
-- Idempotencia de escrita: o writer em lote usa UPSERT. `ocpp_message_logs` tem
-- como chave o `id` unico do registro; o mesmo registro pode ser reemitido com
-- `latency_ms`/`responded_at` preenchidos apos a resposta correlacionada chegar.
-- =============================================================================

create schema if not exists orchestrator;

-- -----------------------------------------------------------------------------
-- 1) ocpp_message_logs - 100% das mensagens OCPP (ambas as direcoes)
-- -----------------------------------------------------------------------------
create table if not exists orchestrator.ocpp_message_logs (
  id              text primary key,
  charge_point_id text        not null,
  connection_id   text,
  direction       text        not null check (direction in ('received', 'sent')),
  message_type_id integer     not null,
  message_type    text        not null,
  action          text,
  unique_id       text,
  payload         jsonb,
  observed_at     timestamptz not null,
  observed_at_local text,
  processed_at    timestamptz,
  responded_at    timestamptz,
  latency_ms      integer,
  status          text        not null,
  error_code      text,
  error_message   text,
  message_bytes   integer,
  parse_error     text,
  created_at      timestamptz not null default now()
);

create index if not exists ocpp_message_logs_cp_time_idx
  on orchestrator.ocpp_message_logs (charge_point_id, observed_at desc);
create index if not exists ocpp_message_logs_unique_idx
  on orchestrator.ocpp_message_logs (unique_id);
create index if not exists ocpp_message_logs_status_idx
  on orchestrator.ocpp_message_logs (status);
create index if not exists ocpp_message_logs_action_idx
  on orchestrator.ocpp_message_logs (action);
create index if not exists ocpp_message_logs_observed_idx
  on orchestrator.ocpp_message_logs (observed_at desc);

-- -----------------------------------------------------------------------------
-- 2) charger_heartbeat_monitor - estado corrente do monitor por carregador
-- -----------------------------------------------------------------------------
create table if not exists orchestrator.charger_heartbeat_monitor (
  charge_point_id            text primary key,
  negotiated_interval_seconds integer     not null,
  last_heartbeat_at          timestamptz,
  next_expected_at           timestamptz,
  last_ocpp_message_at       timestamptz,
  heartbeat_count            bigint      not null default 0,
  delayed_count              bigint      not null default 0,
  average_latency_ms         integer,
  max_latency_ms             integer,
  connection_status          text        not null,
  classification             text        not null,
  seconds_since_last_heartbeat integer,
  updated_at                 timestamptz not null,
  created_at                 timestamptz not null default now()
);

create index if not exists charger_heartbeat_monitor_class_idx
  on orchestrator.charger_heartbeat_monitor (classification);

-- -----------------------------------------------------------------------------
-- 3) charger_connection_events - trilha de conexoes/reconexoes (append-only)
-- -----------------------------------------------------------------------------
create table if not exists orchestrator.charger_connection_events (
  id              uuid primary key,
  charge_point_id text        not null,
  connection_id   text,
  event_type      text        not null,
  event_timestamp timestamptz not null,
  disconnect_reason text,
  metadata        jsonb       not null default '{}'::jsonb,
  created_at      timestamptz not null default now()
);

create index if not exists charger_connection_events_cp_time_idx
  on orchestrator.charger_connection_events (charge_point_id, event_timestamp desc);
create index if not exists charger_connection_events_type_idx
  on orchestrator.charger_connection_events (event_type);

-- -----------------------------------------------------------------------------
-- Retencao opcional (spec §6): mantem a trilha auditavel, mas permite poda
-- periodica. A funcao NAO e executada automaticamente; chame conforme politica.
-- -----------------------------------------------------------------------------
create or replace function orchestrator.prune_ocpp_message_logs(keep_days integer default 30)
returns integer
language plpgsql
as $$
declare
  removed integer;
begin
  delete from orchestrator.ocpp_message_logs
   where observed_at < now() - make_interval(days => greatest(keep_days, 1));
  get diagnostics removed = row_count;
  return removed;
end;
$$;
