/**
 * Tipos de dominio do CSMS. Independentes de transporte (ws/fastify) e de persistencia.
 */

/** Direcao da comunicacao OCPP. */
export type Direction = 'received' | 'sent';

/** Tipos de frame OCPP-J (messageTypeId). */
export const OcppMessageTypeId = {
  CALL: 2,
  CALL_RESULT: 3,
  CALL_ERROR: 4,
} as const;
export type OcppMessageTypeIdValue = (typeof OcppMessageTypeId)[keyof typeof OcppMessageTypeId];

/** Status de processamento de uma mensagem. */
export type MessageStatus = 'received' | 'succeeded' | 'failed';

/** Frame OCPP-J ja interpretado. */
export interface OcppFrame {
  readonly messageTypeId: OcppMessageTypeIdValue | number;
  readonly uniqueId: string;
  readonly action: string | null;
  readonly payload: unknown;
  readonly raw: string;
  readonly error?: { code: string; description: string; details?: unknown };
}

export type ParseResult =
  | { ok: true; frame: OcppFrame }
  | { ok: false; reason: 'json' | 'structure' | 'unsupported_type'; raw: string; uniqueId: string | null };

/** Registro de uma mensagem OCPP para auditoria/observabilidade. */
export interface OcppMessageLog {
  /** ID unico e estavel do registro (chave de upsert na persistencia). */
  readonly id: string;
  readonly chargePointId: string;
  readonly connectionId: string | null;
  readonly direction: Direction;
  readonly messageTypeId: number;
  readonly messageType: 'CALL' | 'CALLRESULT' | 'CALLERROR' | 'UNKNOWN';
  readonly action: string | null;
  readonly uniqueId: string | null;
  /** Payload sanitizado (sem dados sensiveis). */
  readonly payload: unknown;
  /** Momento em que a mensagem foi observada (ISO UTC). */
  readonly observedAt: string;
  /** Mesmo instante em representacao local do servidor (ISO 8601 com offset). */
  readonly observedAtLocal: string;
  /** Momento em que o processamento da mensagem terminou (ISO UTC). */
  readonly processedAt: string | null;
  /** Momento em que a resposta correlacionada foi enviada (ISO UTC). */
  readonly respondedAt: string | null;
  /** Latencia request/response em milissegundos (mapeada por uniqueId). */
  readonly latencyMs: number | null;
  readonly status: MessageStatus;
  readonly errorCode: string | null;
  readonly errorMessage: string | null;
  readonly messageBytes: number;
  /** Motivo quando o frame bruto nao pode ser interpretado (nunca descartado). */
  readonly parseError: string | null;
}

/** Fases do ciclo de vida de um socket. */
export type TransportState = 'CONNECTING' | 'CONNECTED' | 'DISCONNECTING' | 'DISCONNECTED';

export interface ConnectionEvent {
  readonly chargePointId: string;
  readonly connectionId: string | null;
  readonly eventType:
    | 'attempt'
    | 'accepted'
    | 'rejected'
    | 'replaced'
    | 'closed'
    | 'watchdog_timeout'
    | 'pong_timeout'
    | 'error';
  readonly eventTimestamp: string;
  readonly disconnectReason: string | null;
  readonly metadata: Record<string, unknown>;
}

/** Classificacao do monitor de heartbeat. */
export type HeartbeatClassification = 'NORMAL' | 'ATRASADO' | 'CRITICO' | 'OFFLINE' | 'SEM_DADOS';

export interface HeartbeatStatus {
  readonly chargePointId: string;
  readonly negotiatedIntervalSeconds: number;
  readonly lastHeartbeatAt: string | null;
  readonly nextExpectedAt: string | null;
  readonly lastOcppMessageAt: string | null;
  readonly heartbeatCount: number;
  readonly delayedCount: number;
  readonly averageLatencyMs: number | null;
  readonly maxLatencyMs: number | null;
  readonly connectionStatus: 'ONLINE' | 'OFFLINE';
  readonly classification: HeartbeatClassification;
  readonly secondsSinceLastHeartbeat: number | null;
  readonly updatedAt: string;
}
