/**
 * Fabrica do modulo de integracao. Monta o cliente Supabase EXATAMENTE como
 * `persistenceService.ts` faz (construcao preguicosa, nunca lanca) e devolve os
 * componentes das fases entregues:
 *   - F1: repositorio do inbox + correlacionador de sessao;
 *   - F2: repositorio consumido pelas rotas HTTP (`routes.ts`);
 *   - F3: outbox + produtor de telemetria + dispatcher de push (HMAC).
 *
 * Caracteristicas:
 *   - sem `SUPABASE_URL`/`SUPABASE_SERVICE_ROLE_KEY` o modulo fica DESABILITADO
 *     (`enabled: false`), com os componentes nulos e start/stop no-op;
 *   - o push (F3) so e habilitado quando `config.integration.pushEnabled` e
 *     verdadeiro E `targetUrl`/`hmacSecret` estao preenchidos; caso contrario
 *     `dispatcher` e `pushEnabled` ficam nulos/false, mas o outbox continua
 *     sendo populado localmente;
 *   - NAO altera a composicao da aplicacao nem o OCPP (src/transport, src/ocpp).
 *   - `deps.createClient` e um seam de injecao para testes.
 */
import { createClient } from '@supabase/supabase-js';
import type { AppConfig } from '../config/env';
import type { AnySupabaseClient } from '../persistence/supabaseBatchWriter';
import { IntegrationInboxRepository } from './inboxRepository';
import { IntegrationOutboxRepository } from './outboxRepository';
import { IntegrationPushClient } from './pushClient';
import { OutboxDispatcher } from './outboxDispatcher';
import { TelemetryProducer, type TelemetryMessages } from './telemetryProducer';
import { SessionCorrelator, type SessionCorrelatorChargers } from './sessionCorrelator';

export interface IntegrationModuleDeps {
  config: AppConfig;
  chargers: SessionCorrelatorChargers;
  /**
   * Log OCPP. Tipado pela superficie mais completa (`TelemetryMessages`), pois o
   * mesmo emissor alimenta o `SessionCorrelator` (que so usa `chargePointId`/
   * `action`) e o `TelemetryProducer` (que usa `payload`/`processedAt`/...).
   */
  messages: TelemetryMessages;
  log?: (msg: string, meta?: unknown) => void;
  /** Seam de teste (default: `createClient` do supabase-js). */
  createClient?: (url: string, key: string, options?: unknown) => AnySupabaseClient;
}

export interface IntegrationModule {
  readonly enabled: boolean;
  readonly repository: IntegrationInboxRepository | null;
  readonly correlator: SessionCorrelator | null;
  /** Outbox de telemetria (F3). Presente sempre que o modulo esta habilitado. */
  readonly outbox: IntegrationOutboxRepository | null;
  /** Produtor de telemetria (F3). Presente sempre que o modulo esta habilitado. */
  readonly producer: TelemetryProducer | null;
  /** Dispatcher de push (F3). Nulo quando o push nao esta configurado. */
  readonly dispatcher: OutboxDispatcher | null;
  /** Indica se o push para o ANTIGO esta ativo. */
  readonly pushEnabled: boolean;
  start(): void;
  stop(): void;
}

function createDisabled(): IntegrationModule {
  return {
    enabled: false,
    repository: null,
    correlator: null,
    outbox: null,
    producer: null,
    dispatcher: null,
    pushEnabled: false,
    start() {
      /* noop */
    },
    stop() {
      /* noop */
    },
  };
}

export function createIntegrationModule(deps: IntegrationModuleDeps): IntegrationModule {
  const { persistence, integration } = deps.config;
  const log =
    deps.log ??
    ((message: string, meta?: unknown): void => {
      try {
        console.warn(message, meta ?? '');
      } catch {
        /* noop */
      }
    });

  const hasCreds = persistence.supabaseUrl.length > 0 && persistence.supabaseServiceKey.length > 0;
  if (!hasCreds) {
    return createDisabled();
  }

  let client: AnySupabaseClient;
  try {
    const build = deps.createClient ?? createClient;
    client = build(persistence.supabaseUrl, persistence.supabaseServiceKey, {
      auth: { persistSession: false, autoRefreshToken: false },
      db: { schema: persistence.supabaseSchema },
    });
  } catch (err) {
    log('[integration] cliente Supabase invalido; modulo desabilitado', err);
    return createDisabled();
  }

  const repository = new IntegrationInboxRepository(client, { schema: persistence.supabaseSchema, log });
  const correlator = new SessionCorrelator({ chargers: deps.chargers, messages: deps.messages, repository, log });
  const outbox = new IntegrationOutboxRepository(client, { schema: persistence.supabaseSchema, log });
  const producer = new TelemetryProducer({
    chargers: deps.chargers,
    messages: deps.messages,
    sessions: repository,
    outbox,
    log,
  });

  let dispatcher: OutboxDispatcher | null = null;
  let pushEnabled = false;
  if (integration.pushEnabled && integration.targetUrl.length > 0 && integration.hmacSecret.length > 0) {
    const pushClient = new IntegrationPushClient({
      targetUrl: integration.targetUrl,
      hmacSecret: integration.hmacSecret,
      timeoutMs: integration.pushTimeoutMs,
    });
    dispatcher = new OutboxDispatcher({
      store: outbox,
      client: pushClient,
      intervalMs: integration.pushIntervalMs,
      batchSize: integration.pushBatchSize,
      maxAttempts: integration.pushMaxAttempts,
      backoffBaseMs: integration.pushBackoffBaseMs,
      backoffMaxMs: integration.pushBackoffMaxMs,
      log,
    });
    pushEnabled = true;
  } else if (integration.pushEnabled) {
    log('[integration] push habilitado sem targetUrl/hmacSecret; telemetria fica apenas no outbox');
  }

  return {
    enabled: true,
    repository,
    correlator,
    outbox,
    producer,
    dispatcher,
    pushEnabled,
    start(): void {
      correlator.start();
      producer.start();
      dispatcher?.start();
      log(`[integration] modulo iniciado (push=${pushEnabled ? 'on' : 'off'})`);
    },
    stop(): void {
      dispatcher?.stop();
      producer.stop();
      correlator.stop();
    },
  };
}

export { IntegrationInboxRepository } from './inboxRepository';
export { IntegrationOutboxRepository } from './outboxRepository';
export { IntegrationPushClient, signBody } from './pushClient';
export { OutboxDispatcher } from './outboxDispatcher';
export { TelemetryProducer } from './telemetryProducer';
export { SessionCorrelator } from './sessionCorrelator';
export type { IntegrationOutboxRepositoryOptions, OutboxClaimedEvent } from './outboxRepository';
export type { PushFetchLike, PushFetchResponse, PushResult } from './pushClient';
export type { EventPusher, OutboxStore } from './outboxDispatcher';
export type {
  TelemetryChargers,
  TelemetryMessage,
  TelemetryMessages,
  TelemetryOutbox,
  TelemetryProducerDeps,
  TelemetrySessionLookup,
} from './telemetryProducer';
export type { IntegrationInboxRepositoryOptions, IntegrationSessionUpsert } from './inboxRepository';
export type { SessionCorrelatorChargers, SessionCorrelatorDeps, SessionCorrelatorMessage, SessionCorrelatorMessages, SessionCorrelatorRepository } from './sessionCorrelator';
export type {
  InboxClaimInput,
  InboxClaimResult,
  IntegrationEvent,
  IntegrationEventType,
  IntegrationMeterSample,
  IntegrationSessionRow,
  IntegrationSessionState,
} from './types';
