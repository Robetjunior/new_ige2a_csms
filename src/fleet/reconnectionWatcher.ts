/**
 * ReconnectionWatcher (plano de reconexao, passo 3).
 *
 * Compara a frota conhecida (FleetRegistry) com os sockets ativos
 * (`wsServer.isConnected`) e responde a pergunta operacional que hoje nao tem
 * resposta: "quem e conhecido e NAO voltou?".
 *
 * Regras:
 *  - o CSMS NUNCA disca para o carregador; aqui apenas observamos e classificamos;
 *  - nao invade a regra do HeartbeatMonitor: OFFLINE continua sendo decisao do
 *    socket. O watcher so publica CONNECTED / AWAITING_RECONNECTION / UNKNOWN;
 *  - a janela comeca em `max(ultima atividade observada, inicio do watcher)`:
 *    apos um restart, todos os carregadores ganham a janela completa antes de
 *    serem marcados como "aguardando reconexao";
 *  - eventos sao emitidos apenas em TRANSAO (nao repetem a cada tick).
 */
import type { FleetRegistry } from './fleetRegistry';
import type { ConnectionManager } from '../transport/connectionManager';
import type { HeartbeatMonitor } from '../heartbeat/heartbeatMonitor';
import type { OcppWebSocketServer } from '../transport/ocppWebSocketServer';

export type FleetConnectionStatus = 'CONNECTED' | 'AWAITING_RECONNECTION' | 'UNKNOWN';

export type FleetStatusEventType = 'known_charger_awaiting_reconnection' | 'known_charger_reconnected';

export interface FleetMemberStatus {
  chargePointId: string;
  status: FleetConnectionStatus;
  expectedIntervalSeconds: number;
  /** Janela total (ms) de tolerancia antes de considerar "aguardando reconexao". */
  graceWindowMs: number;
  awaitingSince: string | null;
  awaitingMs: number | null;
  lastSeenAt: string | null;
  reason: string;
}

export interface FleetStatusEvent {
  chargePointId: string;
  eventType: FleetStatusEventType;
  status: FleetConnectionStatus;
  awaitingSince: string | null;
  at: string;
  expectedIntervalSeconds: number;
  metadata: Record<string, unknown>;
}

export interface ReconnectionWatcherOptions {
  enabled: boolean;
  /** Periodo de reavaliacao. */
  intervalMs: number;
  /** N multiplicador do intervalo negociado de heartbeat. */
  graceMultiplier: number;
  /** Tolerancia adicional em segundos. */
  toleranceSeconds: number;
  now?: () => number;
}

export interface ReconnectionWatcherDeps {
  fleet: FleetRegistry;
  connections: ConnectionManager;
  wsServer: Pick<OcppWebSocketServer, 'isConnected'>;
  heartbeat: Pick<HeartbeatMonitor, 'getNegotiatedInterval'>;
}

interface MemberState {
  status: FleetConnectionStatus;
  awaitingSinceMs: number | null;
  lastSeenAtMs: number | null;
}

function parseMs(iso: string | null | undefined): number | null {
  if (!iso) return null;
  const parsed = Date.parse(iso);
  return Number.isFinite(parsed) ? parsed : null;
}

export class ReconnectionWatcher {
  readonly enabled: boolean;
  private readonly opts: Required<ReconnectionWatcherOptions>;
  private readonly deps: ReconnectionWatcherDeps;
  private readonly states = new Map<string, MemberState>();
  private readonly listeners = new Set<(event: FleetStatusEvent) => void>();
  private timer: ReturnType<typeof setInterval> | null = null;
  private unsubscribe: (() => void) | null = null;
  private startedAtMs = 0;

  constructor(deps: ReconnectionWatcherDeps, options: ReconnectionWatcherOptions) {
    this.deps = deps;
    this.enabled = options.enabled;
    this.opts = {
      enabled: options.enabled,
      intervalMs: Math.max(1000, options.intervalMs),
      graceMultiplier: Math.max(1, options.graceMultiplier),
      toleranceSeconds: Math.max(0, options.toleranceSeconds),
      now: options.now ?? (() => Date.now()),
    };
  }

  subscribe(listener: (event: FleetStatusEvent) => void): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  private emit(event: FleetStatusEvent): void {
    for (const listener of this.listeners) {
      try {
        listener(event);
      } catch {
        /* observabilidade nunca derruba o transporte OCPP */
      }
    }
  }

  start(): void {
    if (!this.opts.enabled || this.timer) return;
    this.startedAtMs = this.opts.now();
    // Toda transicao de conexao reavalia na hora: a reconexao e percebida no
    // mesmo instante do `accepted`, sem esperar o proximo tick.
    this.unsubscribe = this.deps.connections.subscribe(() => this.evaluate());
    this.timer = setInterval(() => this.evaluate(), this.opts.intervalMs);
    this.timer.unref?.();
    this.evaluate();
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    this.unsubscribe?.();
    this.unsubscribe = null;
  }

  private expectedIntervalSeconds(chargePointId: string): number {
    const negotiated = this.deps.heartbeat.getNegotiatedInterval(chargePointId);
    if (Number.isFinite(negotiated) && negotiated > 0) return negotiated;
    return 60;
  }

  private lastSeenMs(chargePointId: string): number | null {
    const record = this.deps.connections.get(chargePointId);
    const fromConnection =
      parseMs(record?.lastMessageAt ?? null) ??
      parseMs(record?.lastHeartbeatAt ?? null) ??
      parseMs(record?.connectedAt ?? null);
    if (fromConnection !== null) return fromConnection;
    const fleet = this.deps.fleet.get(chargePointId);
    return (
      parseMs(fleet?.lastMessageAt ?? null) ??
      parseMs(fleet?.lastHeartbeatAt ?? null) ??
      parseMs(fleet?.lastConnectedAt ?? null)
    );
  }

  /** Reavalia a frota e emite eventos apenas em transicao de status. */
  evaluate(): void {
    if (!this.opts.enabled || this.startedAtMs === 0) return;
    const now = this.opts.now();
    for (const chargePointId of this.deps.fleet.ids()) {
      const expectedIntervalSeconds = this.expectedIntervalSeconds(chargePointId);
      const graceWindowMs = expectedIntervalSeconds * 1000 * this.opts.graceMultiplier + this.opts.toleranceSeconds * 1000;
      const lastSeenMs = this.lastSeenMs(chargePointId);
      const referenceMs = Math.max(lastSeenMs ?? 0, this.startedAtMs);
      const connected = Boolean(this.deps.wsServer.isConnected(chargePointId));
      const previous = this.states.get(chargePointId) ?? { status: 'UNKNOWN' as FleetConnectionStatus, awaitingSinceMs: null, lastSeenAtMs: lastSeenMs };

      let status: FleetConnectionStatus;
      let awaitingSinceMs: number | null = previous.awaitingSinceMs;

      if (connected) {
        status = 'CONNECTED';
        awaitingSinceMs = null;
      } else if (now - referenceMs >= graceWindowMs) {
        status = 'AWAITING_RECONNECTION';
        awaitingSinceMs = previous.awaitingSinceMs ?? now;
      } else {
        status = 'UNKNOWN';
        awaitingSinceMs = null;
      }

      const state: MemberState = { status, awaitingSinceMs, lastSeenAtMs: lastSeenMs };
      this.states.set(chargePointId, state);

      if (status === previous.status && awaitingSinceMs === previous.awaitingSinceMs) continue;

      const at = new Date(now).toISOString();
      if (status === 'AWAITING_RECONNECTION') {
        this.emit({
          chargePointId,
          eventType: 'known_charger_awaiting_reconnection',
          status,
          awaitingSince: awaitingSinceMs === null ? null : new Date(awaitingSinceMs).toISOString(),
          at,
          expectedIntervalSeconds,
          metadata: {
            graceWindowMs,
            lastSeenAt: lastSeenMs === null ? null : new Date(lastSeenMs).toISOString(),
            reason: 'Carregador conhecido sem socket apos a janela de tolerancia',
          },
        });
      } else if (previous.status === 'AWAITING_RECONNECTION' && status === 'CONNECTED') {
        this.emit({
          chargePointId,
          eventType: 'known_charger_reconnected',
          status,
          awaitingSince: null,
          at,
          expectedIntervalSeconds,
          metadata: { reason: 'Socket reaberto pelo carregador' },
        });
      }
    }
  }

  getStatus(chargePointId: string): FleetMemberStatus {
    const expectedIntervalSeconds = this.expectedIntervalSeconds(chargePointId);
    const graceWindowMs = expectedIntervalSeconds * 1000 * this.opts.graceMultiplier + this.opts.toleranceSeconds * 1000;
    const state = this.states.get(chargePointId);
    const lastSeenMs = this.lastSeenMs(chargePointId);
    const inFleet = this.deps.fleet.has(chargePointId);
    const now = this.opts.now();
    return {
      chargePointId,
      status: state?.status ?? (this.deps.wsServer.isConnected(chargePointId) ? 'CONNECTED' : 'UNKNOWN'),
      expectedIntervalSeconds,
      graceWindowMs,
      awaitingSince: state?.awaitingSinceMs == null ? null : new Date(state.awaitingSinceMs).toISOString(),
      awaitingMs: state?.awaitingSinceMs == null ? null : Math.max(0, now - state.awaitingSinceMs),
      lastSeenAt: lastSeenMs === null ? null : new Date(lastSeenMs).toISOString(),
      reason: inFleet ? 'carregador da frota conhecida' : 'carregador nao pertence a frota conhecida',
    };
  }

  snapshot(): FleetMemberStatus[] {
    return this.deps.fleet.ids().map((id) => this.getStatus(id));
  }
}
