/**
 * Configuracao imutavel, lida estritamente de variaveis de ambiente.
 * Nenhum modulo le `process.env` diretamente: tudo passa por aqui.
 */
import * as dotenv from 'dotenv';

dotenv.config();

function readString(key: string, fallback = ''): string {
  const raw = process.env[key];
  return raw === undefined ? fallback : String(raw).trim();
}

function readNumber(key: string, fallback: number): number {
  const raw = readString(key);
  if (!raw) return fallback;
  const parsed = Number(raw);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function readBool(key: string, fallback: boolean): boolean {
  const raw = readString(key).toLowerCase();
  if (!raw) return fallback;
  return ['1', 'true', 'yes', 'on'].includes(raw);
}

export interface AppConfig {
  readonly env: string;
  readonly port: number;
  readonly host: string;
  readonly logLevel: string;
  readonly apiKey: string;
  /**
   * Chave dedicada do modulo de integracao com o CSMS ANTIGO. Distinta de
   * `apiKey`; exigida no header `x-integration-key` das rotas /integration/v1/*.
   * Vazia desabilita a integracao (respondem 503).
   */
  readonly integrationKey: string;

  /**
   * Integracao de telemetria NOVO -> ANTIGO (F3). O push so e habilitado quando
   * `pushEnabled` e verdadeiro E `targetUrl`/`hmacSecret` estao preenchidos.
   */
  readonly integration: {
    readonly pushEnabled: boolean;
    /** Base do ANTIGO, ex.: `http://127.0.0.1:3000`. */
    readonly targetUrl: string;
    /** Segredo compartilhado do HMAC (`x-signature`), distinto de `integrationKey`. */
    readonly hmacSecret: string;
    readonly pushIntervalMs: number;
    readonly pushBatchSize: number;
    readonly pushTimeoutMs: number;
    readonly pushMaxAttempts: number;
    readonly pushBackoffBaseMs: number;
    readonly pushBackoffMaxMs: number;
    /** F6: falhas consecutivas que abrem o circuit breaker do push. */
    readonly pushCircuitFailureThreshold: number;
    /** F6: tempo (ms) com o circuito aberto antes de sondar o ANTIGO. */
    readonly pushCircuitOpenMs: number;
    /**
     * Uma sessao `PENDING_ACK` que nunca recebeu `StartTransaction` (start
     * aceito pelo carregador mas a transacao nao iniciou) e cancelada apos este
     * tempo (ms). Encerra a sessao local e emite `session.stopped`
     * (`reason: start_timeout`). `0` desabilita o watcher.
     */
    readonly sessionStartTimeoutMs: number;
    /** Periodo (ms) do sweep que cancela sessoes de start expiradas. */
    readonly sessionTimeoutSweepIntervalMs: number;
  };

  readonly ocpp: {
    readonly lenientProtocolFallback: boolean;
    readonly defaultHeartbeatIntervalSeconds: number;
    readonly watchdogTimeoutMs: number;
    readonly pingIntervalMs: number;
    readonly pongTimeoutMs: number;
    readonly maxMessageBytes: number;
    readonly heartbeatToleranceSeconds: number;
    readonly heartbeatCriticalSeconds: number;
    readonly statusProbeEnabled: boolean;
    readonly statusProbeIntervalMs: number;
    readonly statusProbeTimeoutMs: number;
    readonly statusProbeConnectorId: number | null;
    /** Frota conhecida: registro de identidade para reconhecer a reconexao. */
    readonly fleetEnabled: boolean;
    /** Arquivo JSON (opcional) com a frota esperada; '' desabilita a fonte local. */
    readonly fleetFile: string;
    /** IDs cujo status operacional minimo pode ser consultado sem API key. */
    readonly publicStatusChargePointIds: readonly string[];
    /** N ciclos de heartbeat sem retorno antes de alertar "aguardando reconexao". */
    readonly reconnectionGraceMultiplier: number;
    /** Tolerancia adicional (s) somada a janela de reconexao. */
    readonly reconnectionToleranceSeconds: number;
    /** Periodo de verificacao do ReconnectionWatcher. */
    readonly reconnectionCheckIntervalMs: number;
  };

  readonly persistence: {
    readonly supabaseUrl: string;
    readonly supabaseServiceKey: string;
    readonly supabaseSchema: string;
    readonly logPersistEnabled: boolean;
    readonly logBatchSize: number;
    readonly logFlushIntervalMs: number;
    readonly logMaxQueue: number;
  };
}

export function loadConfig(): AppConfig {
  const apiKey = readString('CSMS_API_KEY') || readString('API_KEY');
  const probeConnectorRaw = readString('OCPP_STATUS_PROBE_CONNECTOR_ID');
  const probeConnectorId = probeConnectorRaw ? Number(probeConnectorRaw) : null;
  const publicStatusChargePointIds = Array.from(
    new Set(
      readString('OCPP_PUBLIC_STATUS_IDS')
        .split(',')
        .map((id) => id.trim())
        .filter(Boolean),
    ),
  );

  return Object.freeze({
    env: readString('NODE_ENV', 'development'),
    port: readNumber('PORT', 3100),
    // 0.0.0.0 = aceita conexoes de qualquer interface (necessario para os
    // carregadores da rede local). Em producao, atras de proxy reverso na
    // mesma VM, defina HOST=127.0.0.1 explicitamente para restringir ao loopback.
    host: readString('HOST', '0.0.0.0'),
    logLevel: readString('LOG_LEVEL', 'info'),
    apiKey,
    integrationKey: readString('INTEGRATION_API_KEY'),

    integration: Object.freeze({
      pushEnabled: readBool('INTEGRATION_PUSH_ENABLED', false),
      targetUrl: readString('INTEGRATION_TARGET_URL'),
      hmacSecret: readString('INTEGRATION_HMAC_SECRET'),
      pushIntervalMs: Math.max(500, readNumber('INTEGRATION_PUSH_INTERVAL_MS', 5000)),
      pushBatchSize: Math.max(1, readNumber('INTEGRATION_PUSH_BATCH_SIZE', 50)),
      pushTimeoutMs: Math.max(250, readNumber('INTEGRATION_PUSH_TIMEOUT_MS', 5000)),
      pushMaxAttempts: Math.max(1, readNumber('INTEGRATION_PUSH_MAX_ATTEMPTS', 8)),
      pushBackoffBaseMs: Math.max(100, readNumber('INTEGRATION_PUSH_BACKOFF_BASE_MS', 2000)),
      pushBackoffMaxMs: Math.max(1000, readNumber('INTEGRATION_PUSH_BACKOFF_MAX_MS', 300_000)),
      pushCircuitFailureThreshold: Math.max(1, readNumber('INTEGRATION_PUSH_CIRCUIT_FAILURE_THRESHOLD', 5)),
      pushCircuitOpenMs: Math.max(1000, readNumber('INTEGRATION_PUSH_CIRCUIT_OPEN_MS', 30_000)),
      // 0 desabilita o watcher; default 120 s (janela folgada para firmwares lentos).
      sessionStartTimeoutMs: Math.max(0, readNumber('INTEGRATION_SESSION_START_TIMEOUT_MS', 120_000)),
      sessionTimeoutSweepIntervalMs: Math.max(5_000, readNumber('INTEGRATION_SESSION_TIMEOUT_SWEEP_INTERVAL_MS', 30_000)),
    }),

    ocpp: Object.freeze({
      lenientProtocolFallback: readBool('OCPP_LENIENT_PROTOCOL_FALLBACK', true),
      defaultHeartbeatIntervalSeconds: Math.max(5, readNumber('OCPP_DEFAULT_HEARTBEAT_INTERVAL_SECONDS', 60)),
      // Etapa 6 (calibracao das janelas de deteccao): os defaults foram
      // aproximados do intervalo de heartbeat (60 s) para detectar socket morto
      // rapidamente, sem agressividade que quebre firmwares lentos.
      // watchdog  3x60 s = 180 s (era 300 s);
      // ping+pong 25 s+10 s = 35 s (era 30 s+15 s = 45 s).
      // Todos reversiveis por variavel de ambiente.
      watchdogTimeoutMs: Math.max(1000, readNumber('OCPP_WATCHDOG_TIMEOUT_MS', 180_000)),
      pingIntervalMs: Math.max(1000, readNumber('OCPP_WS_PING_INTERVAL_MS', 25_000)),
      pongTimeoutMs: Math.max(1000, readNumber('OCPP_WS_PONG_TIMEOUT_MS', 10_000)),
      maxMessageBytes: Math.max(1024, readNumber('OCPP_MAX_MESSAGE_BYTES', 262144)),
      heartbeatToleranceSeconds: Math.max(0, readNumber('OCPP_HEARTBEAT_TOLERANCE_SECONDS', 15)),
      heartbeatCriticalSeconds: Math.max(1, readNumber('OCPP_HEARTBEAT_CRITICAL_SECONDS', 120)),
      statusProbeEnabled: readBool('OCPP_STATUS_PROBE_ENABLED', true),
      statusProbeIntervalMs: Math.max(1000, readNumber('OCPP_STATUS_PROBE_INTERVAL_MS', 60_000)),
      statusProbeTimeoutMs: Math.max(250, readNumber('OCPP_STATUS_PROBE_TIMEOUT_MS', 5000)),
      statusProbeConnectorId:
        probeConnectorId !== null && Number.isInteger(probeConnectorId) && probeConnectorId >= 0
          ? probeConnectorId
          : null,
      fleetEnabled: readBool('OCPP_FLEET_ENABLED', true),
      fleetFile: readString('OCPP_FLEET_FILE'),
      publicStatusChargePointIds: Object.freeze(publicStatusChargePointIds),
      reconnectionGraceMultiplier: Math.max(1, readNumber('OCPP_RECONNECTION_GRACE_MULTIPLIER', 2)),
      reconnectionToleranceSeconds: Math.max(0, readNumber('OCPP_RECONNECTION_TOLERANCE_SECONDS', 30)),
      reconnectionCheckIntervalMs: Math.max(1000, readNumber('OCPP_RECONNECTION_CHECK_INTERVAL_MS', 15_000)),
    }),

    persistence: Object.freeze({
      supabaseUrl: readString('SUPABASE_URL'),
      supabaseServiceKey: readString('SUPABASE_SERVICE_ROLE_KEY'),
      supabaseSchema: readString('SUPABASE_SCHEMA', 'orchestrator'),
      logPersistEnabled: readBool('OCPP_LOG_PERSIST_ENABLED', true),
      logBatchSize: Math.max(1, readNumber('OCPP_LOG_BATCH_SIZE', 200)),
      logFlushIntervalMs: Math.max(100, readNumber('OCPP_LOG_FLUSH_INTERVAL_MS', 1500)),
      logMaxQueue: Math.max(100, readNumber('OCPP_LOG_MAX_QUEUE', 20000)),
    }),
  });
}
