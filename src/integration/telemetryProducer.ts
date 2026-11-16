/**
 * Produtor de telemetria (F3): traduz os sinais OCPP ja expostos pelo NOVO em
 * eventos de integracao e os enfileira no outbox.
 *
 * O modulo NAO altera o OCPP: apenas CONSOME superficies publicas existentes,
 * exatamente como o `SessionCorrelator` e o SSE fazem (secao 4.5):
 *   - `chargers.subscribe(...)` + `chargers.get(...).activeTransactions`;
 *   - `messageLog.subscribe(...)`.
 *
 * Por que os DOIS sinais: `ChargerStateService.startTransaction`/
 * `stopTransaction` NAO chamam `notify()`. O sinal confiavel de abertura/fecho
 * e o frame OCPP recebido, reemitido pelo `messageLog` apos o handler rodar
 * (`markProcessed`). Ja `chargers.subscribe` cobre transicoes de qualquer outra
 * origem (ex.: queda de conexao).
 *
 * Correlacao: o `integrationSessionId` e resolvido UMA vez, na abertura da
 * transacao, por `sessions.getOpenSessionByConnector(...)`. Sem sessao de
 * negocio (ex.: swipe local no proprio carregador), nenhum evento e emitido.
 *
 * Eventos emitidos (contrato da secao 4.2):
 *   - `session.started`   { ocppTransactionId, meterStart, idTag }
 *   - `meter.sampled`     { samples[] }  (uma vez por MeterValues recebido)
 *   - `connector.status`  { status, errorCode, info }
 *   - `session.stopped`   { meterStop, reason }
 *
 * Observabilidade nunca interrompe o OCPP: erros sao capturados e logados.
 */
import { randomUUID } from 'node:crypto';
import type { IntegrationEvent, IntegrationEventType, IntegrationMeterSample } from './types';

/** Superficie minima do estado dos carregadores consumida pelo produtor. */
export interface TelemetryChargers {
  subscribe(listener: (chargePointId: string) => void): () => void;
  get(chargePointId: string): { activeTransactions: Map<number, number> } | undefined;
}

/** Recorte do `OcppMessageLog` relevante para a telemetria. */
export interface TelemetryMessage {
  chargePointId: string;
  direction: 'received' | 'sent';
  action: string | null;
  payload: unknown;
  observedAt: string;
  processedAt: string | null;
}

/** Superficie equivalente a `OcppMessageLogEngine.subscribe(...)`. */
export interface TelemetryMessages {
  subscribe(listener: (entry: TelemetryMessage) => void): () => void;
}

/** Resolucao da sessao de negocio correlacionada ao conector. */
export interface TelemetrySessionLookup {
  getOpenSessionByConnector(chargePointId: string, connectorId: number): Promise<string | null>;
}

/** Destino dos eventos (outbox). Nunca lanca. */
export interface TelemetryOutbox {
  enqueue(event: IntegrationEvent): Promise<void>;
}

export interface TelemetryProducerDeps {
  chargers: TelemetryChargers;
  messages: TelemetryMessages;
  sessions: TelemetrySessionLookup;
  outbox: TelemetryOutbox;
  log?: (msg: string, meta?: unknown) => void;
  /** Relogio injetavel para testes deterministicos. */
  now?: () => number;
}

interface StartCache {
  meterStart: number | null;
  timestamp: string | null;
  idTag: string | null;
}

interface StopCache {
  meterStop: number | null;
  timestamp: string | null;
  reason: string | null;
}

/** Transacao ativa observada, com a correlacao ja resolvida. */
interface ActiveTransaction {
  ocppTransactionId: number;
  integrationSessionId: string | null;
}

const DEFAULT_MEASURAND = 'Energy.Active.Import.Register';

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

function asString(value: unknown): string | null {
  return typeof value === 'string' && value.length > 0 ? value : null;
}

function asNumber(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string' && value.trim().length > 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function asInteger(value: unknown): number | null {
  const parsed = asNumber(value);
  return parsed !== null && Number.isInteger(parsed) ? parsed : null;
}

/** Normaliza `meterValue[]` exatamente como o handler de `MeterValues` faz. */
function normalizeMeterValue(meterValue: unknown): IntegrationMeterSample[] {
  if (!Array.isArray(meterValue)) return [];
  const samples: IntegrationMeterSample[] = [];
  for (const entry of meterValue) {
    if (!entry || typeof entry !== 'object' || Array.isArray(entry)) continue;
    const value = entry as Record<string, unknown>;
    const timestamp =
      typeof value.timestamp === 'string' && Number.isFinite(Date.parse(value.timestamp))
        ? value.timestamp
        : new Date().toISOString();
    if (!Array.isArray(value.sampledValue)) continue;
    for (const sampled of value.sampledValue) {
      if (!sampled || typeof sampled !== 'object' || Array.isArray(sampled)) continue;
      const sample = sampled as Record<string, unknown>;
      if (typeof sample.value !== 'string' && typeof sample.value !== 'number') continue;
      samples.push({
        measurand:
          typeof sample.measurand === 'string' && sample.measurand.length > 0
            ? sample.measurand
            : DEFAULT_MEASURAND,
        value: String(sample.value).slice(0, 128),
        unit: typeof sample.unit === 'string' ? sample.unit.slice(0, 32) : null,
        phase: typeof sample.phase === 'string' ? sample.phase.slice(0, 32) : null,
        location: typeof sample.location === 'string' ? sample.location.slice(0, 32) : null,
        context: typeof sample.context === 'string' ? sample.context.slice(0, 32) : null,
        timestamp,
      });
    }
  }
  return samples;
}

export class TelemetryProducer {
  private readonly active = new Map<string, ActiveTransaction>();
  private readonly pendingStart = new Map<string, StartCache>();
  private readonly pendingStop = new Map<string, StopCache>();
  private unsubscribers: Array<() => void> = [];

  constructor(private readonly deps: TelemetryProducerDeps) {}

  /** Assina estado dos carregadores e log de mensagens. Idempotente. */
  start(): void {
    if (this.unsubscribers.length > 0) return;
    this.unsubscribers.push(this.deps.chargers.subscribe((chargePointId) => this.onChargerChanged(chargePointId)));
    this.unsubscribers.push(this.deps.messages.subscribe((entry) => this.onMessage(entry)));
  }

  /** Cancela as assinaturas. Idempotente. */
  stop(): void {
    for (const unsubscribe of this.unsubscribers.splice(0)) {
      try {
        unsubscribe();
      } catch {
        /* observador nunca deve interromper o encerramento */
      }
    }
  }

  private key(chargePointId: string, connectorId: number): string {
    return `${chargePointId}|${connectorId}`;
  }

  private nowIso(): string {
    return new Date(this.deps.now ? this.deps.now() : Date.now()).toISOString();
  }

  /**
   * Trata um frame observado. O `processedAt` distingue a notificacao pre-handler
   * (nula) da pos-handler (preenchida): para `MeterValues`/`StatusNotification`
   * emitimos apenas na primeira, evitando eventos duplicados.
   */
  private onMessage(entry: TelemetryMessage): void {
    try {
      if (entry.direction !== 'received' || !entry.action) return;
      const payload = asRecord(entry.payload);
      switch (entry.action) {
        case 'StartTransaction': {
          const connectorId = asInteger(payload.connectorId);
          if (connectorId !== null) {
            this.pendingStart.set(this.key(entry.chargePointId, connectorId), {
              meterStart: asNumber(payload.meterStart),
              timestamp: asString(payload.timestamp),
              idTag: asString(payload.idTag),
            });
          }
          this.onChargerChanged(entry.chargePointId);
          return;
        }
        case 'StopTransaction': {
          const transactionId = asInteger(payload.transactionId);
          if (transactionId !== null) {
            this.pendingStop.set(`${entry.chargePointId}|${transactionId}`, {
              meterStop: asNumber(payload.meterStop),
              timestamp: asString(payload.timestamp),
              reason: asString(payload.reason),
            });
          }
          this.onChargerChanged(entry.chargePointId);
          return;
        }
        case 'MeterValues': {
          if (entry.processedAt !== null) return;
          this.fireMeterSampled(entry, payload);
          return;
        }
        case 'StatusNotification': {
          if (entry.processedAt !== null) return;
          this.fireConnectorStatus(entry, payload);
          return;
        }
        default:
          return;
      }
    } catch (err) {
      this.deps.log?.(`[integration] erro ao processar mensagem de ${entry.chargePointId}`, err);
    }
  }

  /** Detecta transacoes abertas/encerradas e emite started/stopped. */
  private onChargerChanged(chargePointId: string): void {
    try {
      const current = this.deps.chargers.get(chargePointId)?.activeTransactions ?? new Map<number, number>();

      for (const [connectorId, ocppTransactionId] of current.entries()) {
        const key = this.key(chargePointId, connectorId);
        if (this.active.get(key)?.ocppTransactionId === ocppTransactionId) continue;
        const start = this.pendingStart.get(key) ?? null;
        this.pendingStart.delete(key);
        this.active.set(key, { ocppTransactionId, integrationSessionId: null });
        this.fireStarted(chargePointId, connectorId, ocppTransactionId, start);
      }

      const prefix = `${chargePointId}|`;
      for (const key of Array.from(this.active.keys())) {
        if (!key.startsWith(prefix)) continue;
        const connectorId = Number(key.slice(prefix.length));
        if (current.has(connectorId)) continue;
        const previous = this.active.get(key);
        this.active.delete(key);
        if (!previous) continue;
        const stop = this.pendingStop.get(`${chargePointId}|${previous.ocppTransactionId}`) ?? null;
        this.pendingStop.delete(`${chargePointId}|${previous.ocppTransactionId}`);
        this.fireStopped(chargePointId, connectorId, previous, stop);
      }
    } catch (err) {
      this.deps.log?.(`[integration] erro ao avaliar estado de ${chargePointId}`, err);
    }
  }

  private fireStarted(
    chargePointId: string,
    connectorId: number,
    ocppTransactionId: number,
    start: StartCache | null,
  ): void {
    void (async () => {
      try {
        const integrationSessionId = await this.deps.sessions.getOpenSessionByConnector(chargePointId, connectorId);
        if (!integrationSessionId) return; // sem sessao de negocio: nao emite
        this.active.set(this.key(chargePointId, connectorId), { ocppTransactionId, integrationSessionId });
        await this.enqueue(
          this.buildEvent('session.started', {
            integrationSessionId,
            ocppTransactionId,
            chargePointId,
            connectorId,
            occurredAt: start?.timestamp ?? this.nowIso(),
            data: { meterStart: start?.meterStart ?? null, idTag: start?.idTag ?? null },
          }),
        );
      } catch (err) {
        this.deps.log?.(`[integration] falha ao emitir session.started de ${chargePointId}`, err);
      }
    })();
  }

  private fireStopped(
    chargePointId: string,
    connectorId: number,
    previous: ActiveTransaction,
    stop: StopCache | null,
  ): void {
    if (!previous.integrationSessionId) return; // transacao sem sessao de negocio correlacionada
    void this.enqueue(
      this.buildEvent('session.stopped', {
        integrationSessionId: previous.integrationSessionId,
        ocppTransactionId: previous.ocppTransactionId,
        chargePointId,
        connectorId,
        occurredAt: stop?.timestamp ?? this.nowIso(),
        data: { meterStop: stop?.meterStop ?? null, reason: stop?.reason ?? null },
      }),
    );
  }

  private fireMeterSampled(entry: TelemetryMessage, payload: Record<string, unknown>): void {
    const connectorId = asInteger(payload.connectorId);
    if (connectorId === null) return;
    const active = this.active.get(this.key(entry.chargePointId, connectorId));
    if (!active?.integrationSessionId) return;
    const samples = normalizeMeterValue(payload.meterValue);
    if (samples.length === 0) return;
    void this.enqueue(
      this.buildEvent('meter.sampled', {
        integrationSessionId: active.integrationSessionId,
        ocppTransactionId: active.ocppTransactionId,
        chargePointId: entry.chargePointId,
        connectorId,
        occurredAt: samples[0]?.timestamp ?? entry.observedAt,
        data: { samples },
      }),
    );
  }

  private fireConnectorStatus(entry: TelemetryMessage, payload: Record<string, unknown>): void {
    const connectorId = asInteger(payload.connectorId);
    const status = asString(payload.status);
    if (connectorId === null || !status) return;
    const active = this.active.get(this.key(entry.chargePointId, connectorId));
    void this.enqueue(
      this.buildEvent('connector.status', {
        integrationSessionId: active?.integrationSessionId ?? null,
        ocppTransactionId: active?.ocppTransactionId ?? null,
        chargePointId: entry.chargePointId,
        connectorId,
        occurredAt: entry.observedAt,
        data: {
          status,
          errorCode: asString(payload.errorCode),
          info: asString(payload.info),
        },
      }),
    );
  }

  private buildEvent(
    eventType: IntegrationEventType,
    parts: {
      integrationSessionId: string | null;
      ocppTransactionId: number | null;
      chargePointId: string;
      connectorId: number | null;
      occurredAt: string;
      data: Record<string, unknown>;
    },
  ): IntegrationEvent {
    return {
      eventId: randomUUID(),
      eventType,
      integrationSessionId: parts.integrationSessionId,
      ocppTransactionId: parts.ocppTransactionId,
      chargePointId: parts.chargePointId,
      connectorId: parts.connectorId,
      occurredAt: parts.occurredAt,
      data: parts.data,
    };
  }

  private async enqueue(event: IntegrationEvent): Promise<void> {
    try {
      await this.deps.outbox.enqueue(event);
    } catch (err) {
      this.deps.log?.(`[integration] falha ao enfileirar ${event.eventType}`, err);
    }
  }
}
