/**
 * Persistencia de observabilidade OCPP (spec §6). Encaixa o CSMS nas tabelas do
 * schema `orchestrator`:
 *   - ocpp_message_logs        (sink do motor de log - 100% das mensagens);
 *   - charger_heartbeat_monitor (snapshot periodico do monitor de heartbeat);
 *   - charger_connection_events (trilha append-only das conexoes/reconexoes);
 *   - charger_fleet            (identidade da frota + status de reconexao; os
 *     gatilhos SQL dessa tabela mantem `updated_at` e registram transicoes em
 *     charger_fleet_status_events).
 *
 * A persistencia e totalmente opcional: sem `SUPABASE_URL`/`SUPABASE_SERVICE_ROLE_KEY`
 * (ou com `OCPP_LOG_PERSIST_ENABLED=false`) o servico fica desabilitado e o CSMS
 * segue operando apenas em memoria. Em nenhuma hipotese a falha de gravacao
 * interrompe o protocolo OCPP.
 */
import { randomUUID } from 'node:crypto';
import { createClient } from '@supabase/supabase-js';
import type { AppConfig } from '../config/env';
import type { CsmsComponents } from '../components';
import type { ConnectionEvent, HeartbeatStatus, OcppMessageLog } from '../domain/types';
import type { MessageLogSink } from '../logging/messageLogEngine';
import type { FleetConnectorState, FleetEntry, FleetSeed } from '../fleet/fleetRegistry';
import type { FleetConnectionStatus } from '../fleet/reconnectionWatcher';
import { SupabaseBatchWriter, type AnySupabaseClient, type BatchWriterStats } from './supabaseBatchWriter';

/** Intervalo entre snapshots do monitor de heartbeat (estado corrente, upsert). */
const HEARTBEAT_SNAPSHOT_INTERVAL_MS = 5000;
/** Intervalo entre snapshots da frota (identidade + status, upsert). */
const FLEET_SNAPSHOT_INTERVAL_MS = 10_000;

/** Seam de injecao para testes (default: `createClient` do supabase-js). */
export interface PersistenceDependencies {
  createClient?: (url: string, key: string, options?: unknown) => AnySupabaseClient;
}

export interface PersistenceService {
  readonly enabled: boolean;
  /** Sink a registrar no motor de log (null quando desabilitado). */
  readonly messageSink: MessageLogSink | null;
  start(): void;
  stop(): Promise<void>;
  stats(): Record<string, BatchWriterStats>;
  /** Le a frota conhecida persistida (read-only). Nunca lanca; [] se indisponivel. */
  loadFleet(): Promise<FleetSeed[]>;
}

function createDisabled(): PersistenceService {
  return {
    enabled: false,
    messageSink: null,
    start() {
      /* noop */
    },
    async stop() {
      /* noop */
    },
    stats() {
      return {};
    },
    async loadFleet() {
      return [];
    },
  };
}

function toMessageLogRow(entry: OcppMessageLog): Record<string, unknown> {
  return {
    id: entry.id,
    charge_point_id: entry.chargePointId,
    connection_id: entry.connectionId,
    direction: entry.direction,
    message_type_id: entry.messageTypeId,
    message_type: entry.messageType,
    action: entry.action,
    unique_id: entry.uniqueId,
    payload: entry.payload,
    observed_at: entry.observedAt,
    observed_at_local: entry.observedAtLocal,
    processed_at: entry.processedAt,
    responded_at: entry.respondedAt,
    latency_ms: entry.latencyMs,
    status: entry.status,
    error_code: entry.errorCode,
    error_message: entry.errorMessage,
    message_bytes: entry.messageBytes,
    parse_error: entry.parseError,
  };
}

function toHeartbeatRow(status: HeartbeatStatus): Record<string, unknown> {
  return {
    charge_point_id: status.chargePointId,
    negotiated_interval_seconds: status.negotiatedIntervalSeconds,
    last_heartbeat_at: status.lastHeartbeatAt,
    next_expected_at: status.nextExpectedAt,
    last_ocpp_message_at: status.lastOcppMessageAt,
    heartbeat_count: status.heartbeatCount,
    delayed_count: status.delayedCount,
    average_latency_ms: status.averageLatencyMs,
    max_latency_ms: status.maxLatencyMs,
    connection_status: status.connectionStatus,
    classification: status.classification,
    seconds_since_last_heartbeat: status.secondsSinceLastHeartbeat,
    updated_at: status.updatedAt,
  };
}

function toConnectionEventRow(event: ConnectionEvent): Record<string, unknown> {
  return {
    id: randomUUID(),
    charge_point_id: event.chargePointId,
    connection_id: event.connectionId,
    event_type: event.eventType,
    event_timestamp: event.eventTimestamp,
    disconnect_reason: event.disconnectReason,
    metadata: event.metadata,
  };
}

/** Mapeia a identidade da frota + estado operacional + status para `charger_fleet`. */
function toFleetRow(entry: FleetEntry, status: FleetConnectionStatus): Record<string, unknown> {
  return {
    charge_point_id: entry.chargePointId,
    vendor: entry.vendor,
    model: entry.model,
    serial_number: entry.serialNumber,
    firmware_version: entry.firmwareVersion,
    negotiated_protocol: entry.negotiatedProtocol,
    negotiated_interval_seconds: entry.negotiatedIntervalSeconds,
    connected_at: entry.lastConnectedAt,
    last_boot_at: entry.lastBootAt,
    last_heartbeat_at: entry.lastHeartbeatAt,
    last_pong_at: entry.lastPongAt,
    last_message_at: entry.lastMessageAt,
    last_disconnect_reason: entry.lastDisconnectReason,
    reconnect_count: entry.reconnectCount,
    boot_at: entry.bootAt,
    last_status_at: entry.lastStatusAt,
    connectors: entry.connectors,
    status,
  };
}

function asString(value: unknown): string | null {
  return typeof value === 'string' && value.trim().length > 0 ? value.trim() : null;
}

function asNumber(value: unknown): number | null {
  const parsed = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

/** Normaliza a coluna jsonb `connectors` no formato esperado pelo dominio. */
function asConnectors(value: unknown): FleetConnectorState[] {
  if (!Array.isArray(value)) return [];
  const connectors: FleetConnectorState[] = [];
  for (const item of value) {
    if (item === null || typeof item !== 'object') continue;
    const record = item as Record<string, unknown>;
    const connectorId = asNumber(record.connectorId);
    if (connectorId === null) continue;
    connectors.push({
      connectorId,
      status: asString(record.status) ?? 'Unknown',
      errorCode: asString(record.errorCode) ?? '',
      info: asString(record.info),
      updatedAt: asString(record.updatedAt) ?? new Date().toISOString(),
    });
  }
  return connectors;
}

/** Converte uma linha de `charger_fleet` na semente de identidade do FleetRegistry. */
function fromFleetRow(row: Record<string, unknown>): FleetSeed | null {
  const chargePointId = asString(row.charge_point_id);
  if (!chargePointId) return null;
  return {
    chargePointId,
    vendor: asString(row.vendor),
    model: asString(row.model),
    serialNumber: asString(row.serial_number),
    firmwareVersion: asString(row.firmware_version),
    negotiatedProtocol: asString(row.negotiated_protocol),
    negotiatedIntervalSeconds: asNumber(row.negotiated_interval_seconds),
    lastConnectedAt: asString(row.connected_at),
    lastBootAt: asString(row.last_boot_at),
    lastHeartbeatAt: asString(row.last_heartbeat_at),
    lastPongAt: asString(row.last_pong_at),
    lastMessageAt: asString(row.last_message_at),
    lastDisconnectReason: asString(row.last_disconnect_reason),
    reconnectCount: asNumber(row.reconnect_count) ?? 0,
    bootAt: asString(row.boot_at),
    lastStatusAt: asString(row.last_status_at),
    connectors: asConnectors(row.connectors),
  };
}

export function createPersistenceService(
  config: AppConfig,
  components: CsmsComponents,
  deps: PersistenceDependencies = {},
): PersistenceService {
  const { persistence } = config;
  const hasCreds = persistence.supabaseUrl.length > 0 && persistence.supabaseServiceKey.length > 0;
  if (!persistence.logPersistEnabled || !hasCreds) {
    return createDisabled();
  }

  const log = (message: string, meta?: unknown): void => {
    // Nunca propaga: persistencia e observabilidade, nao caminho critico.
    try {
      console.warn(message, meta ?? '');
    } catch {
      /* noop */
    }
  };

  let client: AnySupabaseClient;
  try {
    const build = deps.createClient ?? createClient;
    client = build(persistence.supabaseUrl, persistence.supabaseServiceKey, {
      auth: { persistSession: false, autoRefreshToken: false },
      db: { schema: persistence.supabaseSchema },
    });
  } catch (err) {
    log('[persistence] cliente Supabase invalido; persistencia desabilitada', err);
    return createDisabled();
  }

  const writerOptions = {
    schema: persistence.supabaseSchema,
    batchSize: persistence.logBatchSize,
    flushIntervalMs: persistence.logFlushIntervalMs,
    maxQueue: persistence.logMaxQueue,
    log,
  } as const;

  const messageLogs = new SupabaseBatchWriter<Record<string, unknown>>(client, {
    ...writerOptions,
    table: 'ocpp_message_logs',
    onConflict: 'id',
    // A mesma mensagem e reemitida com o mesmo `id` (publish -> markProcessed ->
    // correlacao do response). Coalescer por `id` mantem uma unica linha por
    // mensagem no lote: sem isso o `ON CONFLICT DO UPDATE` do PostgreSQL rejeita
    // o lote inteiro ("cannot affect row a second time") e as mensagens se perdem.
    dedupeKey: (row) => String(row.id ?? ''),
  });
  const heartbeats = new SupabaseBatchWriter<Record<string, unknown>>(client, {
    ...writerOptions,
    table: 'charger_heartbeat_monitor',
    onConflict: 'charge_point_id',
  });
  const connectionEvents = new SupabaseBatchWriter<Record<string, unknown>>(client, {
    ...writerOptions,
    table: 'charger_connection_events',
    onConflict: 'id',
  });
  const fleet = new SupabaseBatchWriter<Record<string, unknown>>(client, {
    ...writerOptions,
    table: 'charger_fleet',
    onConflict: 'charge_point_id',
    // Um carregador pode mudar varias vezes entre dois flushes (attempt,
    // accepted, boot, status). Coalescer por chargePointId mantem uma unica
    // linha por carregador no lote e evita chave de conflito duplicada.
    dedupeKey: (row) => String(row.charge_point_id ?? ''),
  });

  const messageSink: MessageLogSink = {
    name: 'supabase:ocpp_message_logs',
    write(entry) {
      messageLogs.enqueue(toMessageLogRow(entry));
    },
  };

  let unsubscribe: (() => void) | null = null;
  let unsubscribeChargers: (() => void) | null = null;
  let snapshotTimer: NodeJS.Timeout | null = null;
  let fleetTimer: NodeJS.Timeout | null = null;

  /**
   * Sincroniza runtime -> FleetRegistry e grava a linha da persistencia. E
   * chamada a cada evento de conexao/estado (sincronizacao "em tempo real",
   * O(1) por evento) e periodicamente para toda a frota.
   */
  const snapshotFleetMember = (chargePointId: string): void => {
    components.fleetStateSyncer.sync(chargePointId);
    const entry = components.fleet.get(chargePointId);
    if (!entry) return;
    const status = components.reconnectionWatcher.getStatus(chargePointId).status;
    fleet.enqueue(toFleetRow(entry, status));
  };

  const snapshotFleet = (): void => {
    // Garante que a identidade/estado operacional estejam atualizados antes de
    // persistir (o FleetRegistry e a fonte lida aqui e no restart).
    components.fleetStateSyncer.syncAll();
    for (const entry of components.fleet.snapshot()) {
      const status = components.reconnectionWatcher.getStatus(entry.chargePointId).status;
      fleet.enqueue(toFleetRow(entry, status));
    }
  };

  return {
    enabled: true,
    messageSink,

    start(): void {
      if (!unsubscribe) {
        unsubscribe = components.connections.subscribe((event) => {
          connectionEvents.enqueue(toConnectionEventRow(event));
          // Persiste imediatamente a transicao (conexao/desconexao/reconexao).
          if (event.chargePointId) snapshotFleetMember(event.chargePointId);
        });
      }
      if (!unsubscribeChargers) {
        // Estado operacional (BootNotification, StatusNotification, MeterValues)
        // chega DEPOIS do evento de conexao: sem esta assinatura a linha da
        // frota so seria atualizada no snapshot periodico, ficando defasada.
        unsubscribeChargers = components.chargers.subscribe((chargePointId) => {
          if (chargePointId) snapshotFleetMember(chargePointId);
        });
      }
      if (!snapshotTimer) {
        snapshotTimer = setInterval(() => {
          for (const status of components.heartbeat.snapshot()) {
            heartbeats.enqueue(toHeartbeatRow(status));
          }
        }, HEARTBEAT_SNAPSHOT_INTERVAL_MS);
        if (typeof snapshotTimer.unref === 'function') snapshotTimer.unref();
      }
      if (!fleetTimer) {
        // Grava a frota uma vez no startup (persiste a identidade semeada) e
        // depois periodicamente (identidade + status de reconexao).
        snapshotFleet();
        fleetTimer = setInterval(snapshotFleet, FLEET_SNAPSHOT_INTERVAL_MS);
        if (typeof fleetTimer.unref === 'function') fleetTimer.unref();
      }
    },

    async stop(): Promise<void> {
      if (snapshotTimer) {
        clearInterval(snapshotTimer);
        snapshotTimer = null;
      }
      if (fleetTimer) {
        clearInterval(fleetTimer);
        fleetTimer = null;
      }
      unsubscribe?.();
      unsubscribe = null;
      unsubscribeChargers?.();
      unsubscribeChargers = null;
      // Em nome da spec §6: drena a fila no shutdown sem jamais lancar.
      await Promise.allSettled([messageLogs.stop(), heartbeats.stop(), connectionEvents.stop(), fleet.stop()]);
    },

    stats() {
      return {
        ocpp_message_logs: messageLogs.stats(),
        charger_heartbeat_monitor: heartbeats.stats(),
        charger_connection_events: connectionEvents.stats(),
        charger_fleet: fleet.stats(),
      };
    },

    async loadFleet(): Promise<FleetSeed[]> {
      try {
        const { data, error } = await client.schema(persistence.supabaseSchema).from('charger_fleet').select('*');
        if (error) {
          log(`[persistence] falha ao ler charger_fleet: ${error.message}`);
          return [];
        }
        const rows = Array.isArray(data) ? (data as Record<string, unknown>[]) : [];
        const seeds: FleetSeed[] = [];
        for (const row of rows) {
          const seed = fromFleetRow(row);
          if (seed) seeds.push(seed);
        }
        return seeds;
      } catch (err) {
        log('[persistence] erro inesperado ao ler charger_fleet', err);
        return [];
      }
    },
  };
}
