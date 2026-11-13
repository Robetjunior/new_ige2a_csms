-- =============================================================================
-- IGE2A CSMS Next Generation - Frota conhecida + status de reconexao
--
-- Complementa a migration 001 sem alterar nenhuma tabela existente.
-- Objetivo (plano de reconexao automatica, passo 5 + gatilhos SQL):
--   - persistir a IDENTIDADE dos carregadores ja vistos (vendor/model/firmware,
--     ultima conexao/boot/heartbeat, intervalo negociado, reconnectCount), para
--     que um restart do CSMS reconheca a reconexao em vez de tratar como novo;
--   - manter o STATUS de reconexao (CONNECTED / AWAITING_RECONNECTION / UNKNOWN)
--     e registrar automaticamente cada TRANSICAO via gatilhos SQL.
--
-- Regras respeitadas:
--   - a frota NUNCA e gravada como CONNECTED sem socket real: o CSMS semeia e
--     reconhece; o gatilho apenas registra o que o runtime observou;
--   - todas as instrucoes sao idempotentes (podem ser reaplicadas).
-- =============================================================================

create schema if not exists orchestrator;

-- -----------------------------------------------------------------------------
-- 1) charger_fleet - identidade + status corrente por carregador conhecido
-- -----------------------------------------------------------------------------
create table if not exists orchestrator.charger_fleet (
  charge_point_id             text primary key,
  vendor                      text,
  model                       text,
  serial_number               text,
  firmware_version            text,
  negotiated_protocol         text,
  negotiated_interval_seconds integer,
  connected_at                timestamptz,
  last_boot_at                timestamptz,
  last_heartbeat_at           timestamptz,
  last_pong_at                timestamptz,
  last_message_at             timestamptz,
  last_disconnect_reason      text,
  reconnect_count             integer     not null default 0,
  status                      text        not null default 'UNKNOWN'
    check (status in ('CONNECTED', 'AWAITING_RECONNECTION', 'UNKNOWN')),
  first_seen_at               timestamptz not null default now(),
  updated_at                  timestamptz not null default now()
);

create index if not exists charger_fleet_status_idx
  on orchestrator.charger_fleet (status);
create index if not exists charger_fleet_last_message_idx
  on orchestrator.charger_fleet (last_message_at desc);

-- -----------------------------------------------------------------------------
-- 2) charger_fleet_status_events - trilha append-only das transicoes de status
--    (alimentada por gatilho; nao e escrita pelo CSMS)
-- -----------------------------------------------------------------------------
create table if not exists orchestrator.charger_fleet_status_events (
  id              uuid primary key default gen_random_uuid(),
  charge_point_id text        not null,
  previous_status text,
  new_status      text        not null,
  changed_at      timestamptz not null default now(),
  metadata        jsonb       not null default '{}'::jsonb
);

create index if not exists charger_fleet_status_events_cp_idx
  on orchestrator.charger_fleet_status_events (charge_point_id, changed_at desc);
create index if not exists charger_fleet_status_events_status_idx
  on orchestrator.charger_fleet_status_events (new_status);

-- -----------------------------------------------------------------------------
-- Gatilho 1: mantem `updated_at` e `first_seen_at` automaticamente.
-- -----------------------------------------------------------------------------
create or replace function orchestrator.charger_fleet_touch()
returns trigger
language plpgsql
as $$
begin
  new.updated_at := now();
  if tg_op = 'INSERT' then
    new.first_seen_at := coalesce(new.first_seen_at, now());
  end if;
  return new;
end;
$$;

drop trigger if exists charger_fleet_touch on orchestrator.charger_fleet;
create trigger charger_fleet_touch
before insert or update on orchestrator.charger_fleet
for each row execute function orchestrator.charger_fleet_touch();

-- -----------------------------------------------------------------------------
-- Gatilho 2: registra toda TRANSICAO de status (INSERT ou UPDATE de `status`).
-- Assim a pergunta "quando este carregador entrou/saiu de AWAITING_RECONNECTION"
-- e respondida pelo proprio banco, sem depender de log da aplicacao.
-- -----------------------------------------------------------------------------
create or replace function orchestrator.charger_fleet_status_change()
returns trigger
language plpgsql
as $$
begin
  if tg_op = 'INSERT' then
    insert into orchestrator.charger_fleet_status_events
      (charge_point_id, previous_status, new_status, metadata)
    values
      (new.charge_point_id, null, new.status,
       jsonb_build_object('reason', new.last_disconnect_reason, 'kind', 'insert'));
    return new;
  end if;

  if new.status is distinct from old.status then
    insert into orchestrator.charger_fleet_status_events
      (charge_point_id, previous_status, new_status, metadata)
    values
      (new.charge_point_id, old.status, new.status,
       jsonb_build_object('reason', new.last_disconnect_reason, 'kind', 'transition'));
  end if;
  return new;
end;
$$;

drop trigger if exists charger_fleet_status_change on orchestrator.charger_fleet;
create trigger charger_fleet_status_change
after insert or update of status on orchestrator.charger_fleet
for each row execute function orchestrator.charger_fleet_status_change();

-- -----------------------------------------------------------------------------
-- 3) Visao de status DERIVADO por tempo (sem agendador).
--    Complementa o gatilho: responde "qual o status agora" combinando o status
--    persistido com a ultima atividade observada e a janela de tolerancia.
-- -----------------------------------------------------------------------------
create or replace view orchestrator.charger_fleet_status as
select
  f.charge_point_id,
  f.vendor,
  f.model,
  f.firmware_version,
  f.negotiated_interval_seconds,
  f.connected_at,
  f.last_heartbeat_at,
  f.last_message_at,
  f.last_disconnect_reason,
  f.reconnect_count,
  f.status                                        as persisted_status,
  case
    when f.connected_at is null then 'UNKNOWN'
    when f.last_message_at is not null
      and f.last_message_at >= now()
        - make_interval(secs => greatest(coalesce(f.negotiated_interval_seconds, 60), 1) * 3)
      then 'CONNECTED'
    when f.last_message_at is null then 'UNKNOWN'
    else 'AWAITING_RECONNECTION'
  end                                             as derived_status,
  now() - coalesce(f.last_message_at, f.connected_at) as since_last_activity,
  f.updated_at
from orchestrator.charger_fleet f;

-- -----------------------------------------------------------------------------
-- Retencao opcional da trilha de eventos de status.
-- -----------------------------------------------------------------------------
create or replace function orchestrator.prune_charger_fleet_status_events(keep_days integer default 90)
returns integer
language plpgsql
as $$
declare
  removed integer;
begin
  delete from orchestrator.charger_fleet_status_events
   where changed_at < now() - make_interval(days => greatest(keep_days, 1));
  get diagnostics removed = row_count;
  return removed;
end;
$$;
