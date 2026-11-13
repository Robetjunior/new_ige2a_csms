import type { AppConfig } from '../config/env';
import type { OcppMessageLog } from '../domain/types';
import type { OcppMessageLogEngine } from '../logging/messageLogEngine';
import type { ConnectionManager } from '../transport/connectionManager';
import type { OcppWebSocketServer } from '../transport/ocppWebSocketServer';

export const PROBE_MESSAGES = ['Heartbeat', 'StatusNotification', 'MeterValues'] as const;
export type ProbeMessage = (typeof PROBE_MESSAGES)[number];
export type ProbeOutcome =
  | 'idle'
  | 'pending'
  | 'received'
  | 'unsupported'
  | 'rejected'
  | 'trigger_timeout'
  | 'report_timeout'
  | 'disconnected'
  | 'error';

export interface ProbeActionState {
  requestedMessage: ProbeMessage;
  outcome: ProbeOutcome;
  lastAttemptAt: string | null;
  lastReportAt: string | null;
  durationMs: number | null;
  detail: string | null;
}

export interface AvailabilityProbeSnapshot {
  chargePointId: string;
  enabled: boolean;
  intervalMs: number;
  timeoutMs: number;
  connectionId: string | null;
  lastCycleAt: string | null;
  nextProbeAt: string | null;
  running: boolean;
  actions: Record<ProbeMessage, ProbeActionState>;
}

export interface AvailabilityProbeEvent {
  id: string;
  chargePointId: string;
  connectionId: string | null;
  requestedMessage: ProbeMessage | null;
  event:
    | 'cycle_started'
    | 'cycle_completed'
    | 'trigger_sent'
    | 'trigger_accepted'
    | 'trigger_rejected'
    | 'unsupported'
    | 'report_received'
    | 'trigger_timeout'
    | 'report_timeout'
    | 'disconnected'
    | 'not_registered'
    | 'error';
  at: string;
  durationMs: number | null;
  detail: string | null;
  messageId: string | null;
  uniqueId: string | null;
}

interface AvailabilityProbeDeps {
  config: AppConfig;
  connections: ConnectionManager;
  messageLog: OcppMessageLogEngine;
  wsServer: OcppWebSocketServer;
}

interface ReportWaiter {
  chargePointId: string;
  connectionId: string;
  requestedMessage: ProbeMessage;
  startedAtMs: number;
  resolve: (entry: OcppMessageLog | null) => void;
  timer: ReturnType<typeof setTimeout>;
}

type ProbeListener = (event: AvailabilityProbeEvent, snapshot: AvailabilityProbeSnapshot) => void;
type ProbeLogger = (event: AvailabilityProbeEvent) => void;

function emptyAction(requestedMessage: ProbeMessage): ProbeActionState {
  return {
    requestedMessage,
    outcome: 'idle',
    lastAttemptAt: null,
    lastReportAt: null,
    durationMs: null,
    detail: null,
  };
}

function isProbeMessage(value: string | null): value is ProbeMessage {
  return value !== null && (PROBE_MESSAGES as readonly string[]).includes(value);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

export class AvailabilityProbeService {
  private readonly deps: AvailabilityProbeDeps;
  private readonly snapshots = new Map<string, AvailabilityProbeSnapshot>();
  private readonly unsupported = new Set<string>();
  private readonly busy = new Set<string>();
  private readonly waiters = new Map<string, ReportWaiter>();
  private readonly events: AvailabilityProbeEvent[] = [];
  private readonly listeners = new Set<ProbeListener>();
  private readonly maxEvents = 5000;
  private timer: ReturnType<typeof setInterval> | null = null;
  private unsubscribeConnections: (() => void) | null = null;
  private unsubscribeMessages: (() => void) | null = null;
  private logger: ProbeLogger | null = null;

  constructor(deps: AvailabilityProbeDeps) {
    this.deps = deps;
  }

  setLogger(logger: ProbeLogger): void {
    this.logger = logger;
  }

  subscribe(listener: ProbeListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  start(): void {
    if (!this.deps.config.ocpp.statusProbeEnabled || this.timer) return;
    this.unsubscribeMessages = this.deps.messageLog.subscribe((entry) => this.onMessage(entry));
    this.unsubscribeConnections = this.deps.connections.subscribe((event) => {
      if (event.eventType === 'accepted' && this.deps.connections.get(event.chargePointId)?.registration === 'ACCEPTED') {
        queueMicrotask(() => void this.probeNow(event.chargePointId));
      }
      if (event.eventType === 'closed' || event.eventType === 'rejected') {
        this.resolveWaiter(event.chargePointId, event.connectionId, null);
      }
    });
    this.timer = setInterval(() => {
      for (const chargePointId of this.deps.wsServer.connectedChargePointIds()) {
        void this.probeNow(chargePointId);
      }
    }, this.deps.config.ocpp.statusProbeIntervalMs);
    this.timer.unref?.();
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    this.unsubscribeConnections?.();
    this.unsubscribeConnections = null;
    this.unsubscribeMessages?.();
    this.unsubscribeMessages = null;
    for (const waiter of this.waiters.values()) {
      clearTimeout(waiter.timer);
      waiter.resolve(null);
    }
    this.waiters.clear();
  }

  get(chargePointId: string): AvailabilityProbeSnapshot {
    const existing = this.snapshots.get(chargePointId);
    if (existing) {
      return {
        ...existing,
        actions: { ...existing.actions },
        running: this.busy.has(chargePointId),
      };
    }
    return {
      chargePointId,
      enabled: this.deps.config.ocpp.statusProbeEnabled,
      intervalMs: this.deps.config.ocpp.statusProbeIntervalMs,
      timeoutMs: this.deps.config.ocpp.statusProbeTimeoutMs,
      connectionId: this.deps.connections.get(chargePointId)?.connectionId ?? null,
      lastCycleAt: null,
      nextProbeAt: null,
      running: this.busy.has(chargePointId),
      actions: {
        Heartbeat: emptyAction('Heartbeat'),
        StatusNotification: emptyAction('StatusNotification'),
        MeterValues: emptyAction('MeterValues'),
      },
    };
  }

  recent(chargePointId?: string, limit = 100): AvailabilityProbeEvent[] {
    return this.events
      .filter((event) => !chargePointId || event.chargePointId === chargePointId)
      .slice(-Math.max(1, Math.min(limit, 1000)))
      .reverse();
  }

  async probeNow(chargePointId: string): Promise<void> {
    if (!this.deps.config.ocpp.statusProbeEnabled || this.busy.has(chargePointId)) return;
    if (this.deps.connections.get(chargePointId)?.registration !== 'ACCEPTED') {
      this.recordEvent(
        chargePointId,
        null,
        null,
        'not_registered',
        null,
        'BootNotification ainda não foi aceito.',
        null,
        null,
      );
      return;
    }
    const session = this.deps.wsServer.getSession(chargePointId);
    if (!session) {
      this.recordEvent(chargePointId, null, null, 'disconnected', null, null, null, null);
      return;
    }

    const connectionId = session.connectionId;
    this.busy.add(chargePointId);
    const initial = this.get(chargePointId);
    this.snapshots.set(chargePointId, {
      ...initial,
      connectionId,
      lastCycleAt: new Date().toISOString(),
      nextProbeAt: new Date(Date.now() + this.deps.config.ocpp.statusProbeIntervalMs).toISOString(),
      running: true,
    });
    this.recordEvent(chargePointId, connectionId, null, 'cycle_started', null, null, null, null);

    try {
      for (const requestedMessage of PROBE_MESSAGES) {
        if (this.deps.wsServer.getSession(chargePointId)?.connectionId !== connectionId) {
          this.updateAction(chargePointId, requestedMessage, {
            outcome: 'disconnected',
            detail: 'A sessão WebSocket mudou ou foi encerrada durante a sondagem.',
          });
          this.recordEvent(chargePointId, connectionId, requestedMessage, 'disconnected', null, null, null, null);
          break;
        }
        if (this.unsupported.has(this.unsupportedKey(connectionId, requestedMessage))) continue;
        await this.probeMessage(chargePointId, connectionId, requestedMessage);
      }
    } finally {
      this.busy.delete(chargePointId);
      const current = this.snapshots.get(chargePointId);
      if (current) {
        this.snapshots.set(chargePointId, { ...current, running: false });
        this.notifyLast(chargePointId);
      }
      this.recordEvent(chargePointId, connectionId, null, 'cycle_completed', null, null, null, null);
    }
  }

  /**
   * Sondagem OCPP DIRECIONADA a uma unica mensagem (plano de reconexao:
   * "status do carregador"). Reutiliza a mesma maquina de waiters/outcome do
   * ciclo completo, mas nao executa Heartbeat/MeterValues. Retorna o estado da
   * acao apos a tentativa (null se desabilitada, ocupada ou sem sessao aceita).
   */
  async probeSingleNow(chargePointId: string, requestedMessage: ProbeMessage): Promise<ProbeActionState | null> {
    if (!this.deps.config.ocpp.statusProbeEnabled || this.busy.has(chargePointId)) return null;
    if (this.deps.connections.get(chargePointId)?.registration !== 'ACCEPTED') {
      this.recordEvent(
        chargePointId,
        null,
        requestedMessage,
        'not_registered',
        null,
        'BootNotification ainda não foi aceito.',
        null,
        null,
      );
      return null;
    }
    const session = this.deps.wsServer.getSession(chargePointId);
    if (!session) {
      this.recordEvent(chargePointId, null, requestedMessage, 'disconnected', null, null, null, null);
      return null;
    }

    const connectionId = session.connectionId;
    this.busy.add(chargePointId);
    try {
      if (!this.unsupported.has(this.unsupportedKey(connectionId, requestedMessage))) {
        await this.probeMessage(chargePointId, connectionId, requestedMessage);
      }
    } finally {
      this.busy.delete(chargePointId);
      const current = this.snapshots.get(chargePointId);
      if (current) {
        this.snapshots.set(chargePointId, { ...current, connectionId, running: false });
        this.notifyLast(chargePointId);
      }
    }
    return this.get(chargePointId).actions[requestedMessage];
  }

  private async probeMessage(
    chargePointId: string,
    connectionId: string,
    requestedMessage: ProbeMessage,
  ): Promise<void> {
    const startedAtMs = Date.now();
    const timeoutMs = this.deps.config.ocpp.statusProbeTimeoutMs;
    this.updateAction(chargePointId, requestedMessage, {
      outcome: 'pending',
      lastAttemptAt: new Date(startedAtMs).toISOString(),
      durationMs: null,
      detail: null,
    });
    const waiter = this.createReportWaiter(chargePointId, connectionId, requestedMessage, startedAtMs);
    const payload: Record<string, unknown> = { requestedMessage };
    const connectorId = this.deps.config.ocpp.statusProbeConnectorId;
    if (connectorId !== null && requestedMessage !== 'Heartbeat') payload.connectorId = connectorId;

    this.recordEvent(chargePointId, connectionId, requestedMessage, 'trigger_sent', null, null, null, null);
    let response: unknown;
    try {
      response = await this.deps.wsServer.sendCall(chargePointId, 'TriggerMessage', payload, timeoutMs);
    } catch (error) {
      waiter.cancel();
      const detail = String((error as Error)?.message ?? error);
      const triggerTimedOut = detail.toLowerCase().includes('timeout');
      const outcome = triggerTimedOut ? 'trigger_timeout' : 'error';
      this.updateAction(chargePointId, requestedMessage, {
        outcome,
        durationMs: Date.now() - startedAtMs,
        detail,
      });
      this.recordEvent(
        chargePointId,
        connectionId,
        requestedMessage,
        triggerTimedOut ? 'trigger_timeout' : 'error',
        Date.now() - startedAtMs,
        detail,
        null,
        null,
      );
      return;
    }

    const status = isRecord(response) && typeof response.status === 'string' ? response.status : '';
    if (status !== 'Accepted') {
      waiter.cancel();
      const notImplemented = status === 'NotImplemented';
      if (notImplemented) this.unsupported.add(this.unsupportedKey(connectionId, requestedMessage));
      const outcome = notImplemented ? 'unsupported' : 'rejected';
      const detail = status || 'TriggerMessage.conf sem status válido';
      this.updateAction(chargePointId, requestedMessage, {
        outcome,
        durationMs: Date.now() - startedAtMs,
        detail,
      });
      this.recordEvent(
        chargePointId,
        connectionId,
        requestedMessage,
        notImplemented ? 'unsupported' : 'trigger_rejected',
        Date.now() - startedAtMs,
        detail,
        null,
        null,
      );
      return;
    }

    this.recordEvent(chargePointId, connectionId, requestedMessage, 'trigger_accepted', Date.now() - startedAtMs, null, null, null);
    const report = await waiter.promise;
    if (report) {
      const durationMs = Date.now() - startedAtMs;
      this.updateAction(chargePointId, requestedMessage, {
        outcome: 'received',
        lastReportAt: report.observedAt,
        durationMs,
        detail: null,
      });
      this.recordEvent(
        chargePointId,
        connectionId,
        requestedMessage,
        'report_received',
        durationMs,
        null,
        report.id,
        report.uniqueId,
      );
      return;
    }

    const stillSameSession = this.deps.wsServer.getSession(chargePointId)?.connectionId === connectionId;
    const outcome = stillSameSession ? 'report_timeout' : 'disconnected';
    const detail = stillSameSession
      ? `TriggerMessage aceito, mas ${requestedMessage} não chegou em ${timeoutMs}ms.`
      : 'A sessão WebSocket foi encerrada antes da chegada da mensagem solicitada.';
    this.updateAction(chargePointId, requestedMessage, {
      outcome,
      durationMs: Date.now() - startedAtMs,
      detail,
    });
    this.recordEvent(
      chargePointId,
      connectionId,
      requestedMessage,
      stillSameSession ? 'report_timeout' : 'disconnected',
      Date.now() - startedAtMs,
      detail,
      null,
      null,
    );
  }

  private createReportWaiter(
    chargePointId: string,
    connectionId: string,
    requestedMessage: ProbeMessage,
    startedAtMs: number,
  ): { promise: Promise<OcppMessageLog | null>; cancel: () => void } {
    const key = this.waiterKey(chargePointId, connectionId, requestedMessage);
    const current = this.waiters.get(key);
    if (current) {
      clearTimeout(current.timer);
      current.resolve(null);
    }
    let resolveWaiter!: (entry: OcppMessageLog | null) => void;
    const promise = new Promise<OcppMessageLog | null>((resolve) => {
      resolveWaiter = resolve;
    });
    const waiter: ReportWaiter = {
      chargePointId,
      connectionId,
      requestedMessage,
      startedAtMs,
      resolve: resolveWaiter,
      timer: setTimeout(() => {
        if (this.waiters.get(key) === waiter) this.waiters.delete(key);
        waiter.resolve(null);
      }, this.deps.config.ocpp.statusProbeTimeoutMs),
    };
    waiter.timer.unref?.();
    this.waiters.set(key, waiter);
    return {
      promise,
      cancel: () => {
        clearTimeout(waiter.timer);
        if (this.waiters.get(key) === waiter) this.waiters.delete(key);
        waiter.resolve(null);
      },
    };
  }

  private onMessage(entry: OcppMessageLog): void {
    if (
      entry.direction !== 'received' ||
      entry.messageType !== 'CALL' ||
      entry.processedAt === null ||
      !isProbeMessage(entry.action)
    ) {
      return;
    }
    const candidates = Array.from(this.waiters.entries());
    for (const [key, waiter] of candidates) {
      if (
        waiter.chargePointId !== entry.chargePointId ||
        waiter.connectionId !== entry.connectionId ||
        waiter.requestedMessage !== entry.action ||
        Date.parse(entry.observedAt) < waiter.startedAtMs
      ) {
        continue;
      }
      clearTimeout(waiter.timer);
      this.waiters.delete(key);
      waiter.resolve(entry);
      return;
    }
  }

  private resolveWaiter(chargePointId: string, connectionId: string | null, entry: OcppMessageLog | null): void {
    for (const [key, waiter] of this.waiters.entries()) {
      if (waiter.chargePointId !== chargePointId) continue;
      if (connectionId && waiter.connectionId !== connectionId) continue;
      clearTimeout(waiter.timer);
      this.waiters.delete(key);
      waiter.resolve(entry);
    }
  }

  private unsupportedKey(connectionId: string, action: ProbeMessage): string {
    return `${connectionId}:${action}`;
  }

  private waiterKey(chargePointId: string, connectionId: string, action: ProbeMessage): string {
    return `${chargePointId}:${connectionId}:${action}`;
  }

  private updateAction(
    chargePointId: string,
    requestedMessage: ProbeMessage,
    patch: Partial<ProbeActionState>,
  ): void {
    const snapshot = this.get(chargePointId);
    this.snapshots.set(chargePointId, {
      ...snapshot,
      connectionId: this.deps.connections.get(chargePointId)?.connectionId ?? snapshot.connectionId,
      actions: {
        ...snapshot.actions,
        [requestedMessage]: { ...snapshot.actions[requestedMessage], ...patch },
      },
      running: this.busy.has(chargePointId),
    });
    this.notifyLast(chargePointId);
  }

  private recordEvent(
    chargePointId: string,
    connectionId: string | null,
    requestedMessage: ProbeMessage | null,
    event: AvailabilityProbeEvent['event'],
    durationMs: number | null,
    detail: string | null,
    messageId: string | null,
    uniqueId: string | null,
  ): void {
    const probeEvent: AvailabilityProbeEvent = {
      id: `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`,
      chargePointId,
      connectionId,
      requestedMessage,
      event,
      at: new Date().toISOString(),
      durationMs,
      detail,
      messageId,
      uniqueId,
    };
    this.events.push(probeEvent);
    if (this.events.length > this.maxEvents) this.events.splice(0, this.events.length - this.maxEvents);
    try {
      this.logger?.(probeEvent);
    } catch {
      /* falha de log nao deve afetar o transporte OCPP */
    }
    const snapshot = this.get(chargePointId);
    for (const listener of this.listeners) {
      try {
        listener(probeEvent, snapshot);
      } catch {
        /* clientes de monitoramento sao independentes do fluxo OCPP */
      }
    }
  }

  private notifyLast(chargePointId: string): void {
    const last = [...this.events].reverse().find((event) => event.chargePointId === chargePointId);
    if (last) {
      const snapshot = this.get(chargePointId);
      for (const listener of this.listeners) {
        try {
          listener(last, snapshot);
        } catch {
          /* noop */
        }
      }
    }
  }
}
