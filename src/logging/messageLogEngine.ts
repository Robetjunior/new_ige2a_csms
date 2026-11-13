/**
 * Motor de log OCPP: ponto unico por onde passa 100% das mensagens recebidas e
 * enviadas (spec §3). Cada registro e distribuido para:
 *   - sinks de persistencia (assincronos, nunca bloqueiam o protocolo);
 *   - um buffer circular em memoria (historico consultavel pela UI);
 *   - assinantes em tempo real (SSE/WebSocket).
 *
 * Garantias:
 *   - NUNCA descarta mensagem: frames invalidos viram registro com payload
 *     limitado e `parseError` preenchido;
 *   - preserva a ordem cronologica de observacao (buffer append-only);
 *   - correlaciona request/response por `uniqueId` e calcula a latencia;
 *   - o motor nunca lanca: falha de observabilidade nao derruba o OCPP.
 */
import { randomUUID } from 'node:crypto';
import type { Direction, MessageStatus, OcppFrame, OcppMessageLog } from '../domain/types';
import { boundedRawPayload, sanitizePayload } from './sanitize';

/** Destino de persistencia. `write` deve ser nao-bloqueante (fila interna). */
export interface MessageLogSink {
  readonly name: string;
  /**
   * Deve ser idempotente por `entry.id` (upsert): o mesmo registro pode ser
   * reemitido com `latencyMs`/`respondedAt` preenchidos apos a resposta chegar.
   */
  write(entry: OcppMessageLog): void;
}

export interface MessageLogQuery {
  chargePointId?: string;
  connectionId?: string;
  direction?: Direction;
  action?: string;
  messageType?: string;
  status?: string;
  onlyErrors?: boolean;
  search?: string;
  from?: string;
  to?: string;
  limit?: number;
}

export interface LogFrameInput {
  chargePointId: string;
  connectionId?: string | null;
  direction: Direction;
  /** Frame interpretado; ausente quando o conteudo bruto nao pode ser parseado. */
  frame?: OcppFrame | null;
  /** Conteudo exatamente como trafegou no socket. */
  raw: string;
  /** Motivo do parse falhar (quando `frame` nao foi fornecido). */
  parseError?: string | null;
  /** Estado inicial; default: 'received' (entrada) ou 'succeeded' (saida). */
  status?: MessageStatus;
  errorCode?: string | null;
  errorMessage?: string | null;
  /** Relogio injetavel para testes deterministicos. */
  nowMs?: number;
}

/** Representacao interna mutavel: permite preencher latencia apos a resposta. */
interface InternalRecord {
  id: string;
  chargePointId: string;
  connectionId: string | null;
  direction: Direction;
  messageTypeId: number;
  messageType: OcppMessageLog['messageType'];
  action: string | null;
  uniqueId: string | null;
  payload: unknown;
  observedAtMs: number;
  processedAtMs: number | null;
  respondedAtMs: number | null;
  latencyMs: number | null;
  status: MessageStatus;
  errorCode: string | null;
  errorMessage: string | null;
  messageBytes: number;
  parseError: string | null;
  published: boolean;
}

const DEFAULT_BUFFER_SIZE = 5000;
const CALL = 2;
const CALL_RESULT = 3;
const CALL_ERROR = 4;

/** Converte epoch (ms) em ISO local com offset explicito (ex.: 2026-11-13T10:00:00.000+08:00). */
function toLocalIso(ms: number): string {
  const date = new Date(ms);
  const offsetMinutes = -date.getTimezoneOffset();
  const sign = offsetMinutes >= 0 ? '+' : '-';
  const abs = Math.abs(offsetMinutes);
  const hh = String(Math.floor(abs / 60)).padStart(2, '0');
  const mm = String(abs % 60).padStart(2, '0');
  const shifted = new Date(ms + offsetMinutes * 60_000);
  return `${shifted.toISOString().slice(0, 23)}${sign}${hh}:${mm}`;
}

function toMessageType(messageTypeId: number): OcppMessageLog['messageType'] {
  if (messageTypeId === CALL) return 'CALL';
  if (messageTypeId === CALL_RESULT) return 'CALLRESULT';
  if (messageTypeId === CALL_ERROR) return 'CALLERROR';
  return 'UNKNOWN';
}

function toDto(rec: InternalRecord): OcppMessageLog {
  return Object.freeze({
    id: rec.id,
    chargePointId: rec.chargePointId,
    connectionId: rec.connectionId,
    direction: rec.direction,
    messageTypeId: rec.messageTypeId,
    messageType: rec.messageType,
    action: rec.action,
    uniqueId: rec.uniqueId,
    payload: rec.payload,
    observedAt: new Date(rec.observedAtMs).toISOString(),
    observedAtLocal: toLocalIso(rec.observedAtMs),
    processedAt: rec.processedAtMs === null ? null : new Date(rec.processedAtMs).toISOString(),
    respondedAt: rec.respondedAtMs === null ? null : new Date(rec.respondedAtMs).toISOString(),
    latencyMs: rec.latencyMs,
    status: rec.status,
    errorCode: rec.errorCode,
    errorMessage: rec.errorMessage,
    messageBytes: rec.messageBytes,
    parseError: rec.parseError,
  });
}

export class OcppMessageLogEngine {
  private readonly sinks: MessageLogSink[] = [];
  private readonly listeners = new Set<(entry: OcppMessageLog) => void>();
  private readonly buffer: InternalRecord[] = [];
  private readonly byId = new Map<string, InternalRecord>();
  /** CALLs aguardando CALLRESULT/CALLERROR, chave `chargePointId|uniqueId` (qualquer direcao). */
  private readonly pending = new Map<string, InternalRecord>();
  private readonly bufferSize: number;
  private readonly idPrefix: string;
  private sequence = 0;
  private totalReceived = 0;
  private totalSent = 0;
  private totalErrors = 0;

  constructor(bufferSize = DEFAULT_BUFFER_SIZE, idPrefix?: string) {
    this.bufferSize = bufferSize;
    this.idPrefix = idPrefix ?? randomUUID().slice(0, 8);
  }

  addSink(sink: MessageLogSink): void {
    this.sinks.push(sink);
  }

  /** Assinante de tempo real (SSE/WebSocket). Devolve a funcao de cancelamento. */
  subscribe(listener: (entry: OcppMessageLog) => void): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  private nextId(): string {
    this.sequence += 1;
    return `${this.idPrefix}-${this.sequence}`;
  }

  /**
   * Ponto unico de captura. Registra um frame (valido ou invalido) e devolve o
   * DTO imutavel do registro criado.
   */
  logFrame(input: LogFrameInput): OcppMessageLog {
    const now = input.nowMs ?? Date.now();
    const frame = input.frame ?? null;
    const messageTypeId = frame ? Number(frame.messageTypeId) : 0;
    const uniqueId = frame?.uniqueId ?? null;
    const direction = input.direction;

    const record: InternalRecord = {
      id: this.nextId(),
      chargePointId: input.chargePointId,
      connectionId: input.connectionId ?? null,
      direction,
      messageTypeId,
      messageType: toMessageType(messageTypeId),
      action: frame?.action ?? null,
      uniqueId,
      payload: frame ? sanitizePayload(frame.payload) : boundedRawPayload(input.raw),
      observedAtMs: now,
      processedAtMs: null,
      respondedAtMs: null,
      latencyMs: null,
      status: input.status ?? (direction === 'received' ? 'received' : 'succeeded'),
      errorCode: input.errorCode ?? frame?.error?.code ?? null,
      errorMessage: input.errorMessage ?? frame?.error?.description ?? null,
      messageBytes: Buffer.byteLength(input.raw),
      parseError: input.parseError ?? null,
      published: false,
    };

    this.correlate(record, now);
    this.publish(record);
    return toDto(record);
  }

  /**
   * Correlaciona request/response por `uniqueId`, independentemente da direcao:
   * um CALL (type 2) abre a pendencia; o CALLRESULT/CALLERROR (type 3/4) com o
   * mesmo `uniqueId` a fecha e preenche `respondedAt`/`latencyMs`.
   */
  private correlate(record: InternalRecord, now: number): void {
    if (!record.uniqueId) return;
    const key = `${record.chargePointId}|${record.uniqueId}`;

    if (record.messageTypeId === CALL) {
      this.pending.set(key, record);
      return;
    }

    if (record.messageTypeId === CALL_RESULT || record.messageTypeId === CALL_ERROR) {
      const pending = this.pending.get(key);
      if (!pending) return;
      this.pending.delete(key);
      pending.respondedAtMs = now;
      pending.latencyMs = Math.max(0, now - pending.observedAtMs);
      pending.status = record.messageTypeId === CALL_RESULT ? 'succeeded' : 'failed';
      if (record.messageTypeId === CALL_ERROR) {
        pending.errorCode = record.errorCode;
        pending.errorMessage = record.errorMessage;
      }
      // Reemite o request ja com a latencia preenchida (upsert no sink).
      this.notify(pending);
    }
  }

  /** Marca o termino do processamento de um registro (ex.: fim do handler). */
  markProcessed(recordId: string, nowMs?: number): void {
    const rec = this.byId.get(recordId);
    if (!rec) return;
    rec.processedAtMs = nowMs ?? Date.now();
    this.notify(rec);
  }

  private publish(record: InternalRecord): void {
    if (!record.published) {
      record.published = true;
      this.buffer.push(record);
      this.byId.set(record.id, record);
      this.evictIfNeeded();
      if (record.direction === 'received') this.totalReceived += 1;
      else this.totalSent += 1;
      if (record.status === 'failed') this.totalErrors += 1;
    }
    this.notify(record);
  }

  private notify(record: InternalRecord): void {
    const dto = toDto(record);
    for (const sink of this.sinks) {
      try {
        sink.write(dto);
      } catch {
        /* persistencia nunca propaga erro */
      }
    }
    for (const listener of this.listeners) {
      try {
        listener(dto);
      } catch {
        /* assinante nunca propaga erro */
      }
    }
  }

  private evictIfNeeded(): void {
    while (this.buffer.length > this.bufferSize) {
      const removed = this.buffer.shift();
      if (!removed) break;
      this.byId.delete(removed.id);
    }
  }

  /** Historico em memoria (mais recente primeiro), com filtros. */
  recent(query: MessageLogQuery = {}): OcppMessageLog[] {
    const limit = Math.min(Math.max(1, query.limit ?? 200), this.bufferSize);
    const fromMs = query.from ? Date.parse(query.from) : null;
    const toMs = query.to ? Date.parse(query.to) : null;
    const search = query.search ? query.search.toLowerCase() : null;

    const out: OcppMessageLog[] = [];
    for (let i = this.buffer.length - 1; i >= 0 && out.length < limit; i--) {
      const e = this.buffer[i];
      if (!e) continue;
      if (query.chargePointId && e.chargePointId !== query.chargePointId) continue;
      if (query.connectionId && e.connectionId !== query.connectionId) continue;
      if (query.direction && e.direction !== query.direction) continue;
      if (query.action && e.action !== query.action) continue;
      if (query.messageType && e.messageType !== query.messageType) continue;
      if (query.status && e.status !== query.status) continue;
      if (query.onlyErrors && e.status !== 'failed') continue;
      if (fromMs !== null && e.observedAtMs < fromMs) continue;
      if (toMs !== null && e.observedAtMs > toMs) continue;
      if (search) {
        const haystack =
          `${e.chargePointId} ${e.uniqueId ?? ''} ${e.action ?? ''} ${JSON.stringify(e.payload)}`.toLowerCase();
        if (!haystack.includes(search)) continue;
      }
      out.push(toDto(e));
    }
    return out;
  }

  stats(): {
    received: number;
    sent: number;
    errors: number;
    buffered: number;
    pendingCorrelations: number;
    listeners: number;
    sinks: number;
  } {
    return {
      received: this.totalReceived,
      sent: this.totalSent,
      errors: this.totalErrors,
      buffered: this.buffer.length,
      pendingCorrelations: this.pending.size,
      listeners: this.listeners.size,
      sinks: this.sinks.length,
    };
  }
}
