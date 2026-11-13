/**
 * Gerenciador de conexoes OCPP por carregador (spec §5).
 *
 * Modela TRES dimensoes independentes de estado:
 *   1. transporte  -> CONNECTING | CONNECTED | DISCONNECTING | DISCONNECTED
 *   2. registro    -> NOT_REGISTERED | PENDING | ACCEPTED | REJECTED
 *   3. operacional -> ONLINE | DEGRADED | OFFLINE
 *
 * Regras atendidas:
 *   - reconexao NAO cria registro duplicado (a chave e o chargePointId);
 *   - socket obsoleto NAO derruba a conexao nova (connectionId por socket);
 *   - ONLINE e sustentado por criterio tecnico verificavel, nao pela mera
 *     existencia do socket.
 */
import { randomUUID } from 'node:crypto';
import type { ConnectionEvent, TransportState } from '../domain/types';

export type RegistrationState = 'NOT_REGISTERED' | 'PENDING' | 'ACCEPTED' | 'REJECTED';
export type OperationalState = 'ONLINE' | 'DEGRADED' | 'OFFLINE';

export interface ConnectionMeta {
  protocol?: string | null;
  remoteIp?: string | null;
  usedFallback?: boolean;
  userAgent?: string | null;
  /** Fato informado pelo transporte: havia socket ATIVO sendo substituido. */
  replaced?: boolean;
}

export interface ChargerConnectionRecord {
  chargePointId: string;
  connectionId: string | null;
  vendor: string | null;
  model: string | null;
  firmwareVersion: string | null;
  negotiatedProtocol: string | null;
  remoteIp: string | null;
  usedFallback: boolean;
  connectedAt: string | null;
  lastBootAt: string | null;
  lastHeartbeatAt: string | null;
  lastMessageAt: string | null;
  lastPongAt: string | null;
  disconnectReason: string | null;
  reconnectCount: number;
  transport: TransportState;
  registration: RegistrationState;
  operational: OperationalState;
  updatedAt: string;
}

/** Janela em que uma conexao sem atividade deixa de ser considerada ONLINE. */
const OPERATIONAL_STALE_MS = 300_000;

export interface ConnectionManagerOptions {
  maxRecords?: number;
  staleMs?: number;
  now?: () => number;
}

export class ConnectionManager {
  private readonly records = new Map<string, ChargerConnectionRecord>();
  private readonly listeners = new Set<(event: ConnectionEvent) => void>();
  private readonly maxRecords: number;
  private readonly staleMs: number;
  private readonly now: () => number;

  constructor(opts: ConnectionManagerOptions = {}) {
    this.maxRecords = Math.max(1, opts.maxRecords ?? 5000);
    this.staleMs = Math.max(1000, opts.staleMs ?? OPERATIONAL_STALE_MS);
    this.now = opts.now ?? (() => Date.now());
  }

  subscribe(listener: (event: ConnectionEvent) => void): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  private emit(event: ConnectionEvent): void {
    for (const listener of this.listeners) {
      try {
        listener(event);
      } catch {
        /* observabilidade nunca derruba a conexao */
      }
    }
  }

  private ensure(chargePointId: string): ChargerConnectionRecord {
    let rec = this.records.get(chargePointId);
    if (!rec) {
      rec = {
        chargePointId,
        connectionId: null,
        vendor: null,
        model: null,
        firmwareVersion: null,
        negotiatedProtocol: null,
        remoteIp: null,
        usedFallback: false,
        connectedAt: null,
        lastBootAt: null,
        lastHeartbeatAt: null,
        lastMessageAt: null,
        lastPongAt: null,
        disconnectReason: null,
        reconnectCount: 0,
        transport: 'DISCONNECTED',
        registration: 'NOT_REGISTERED',
        operational: 'OFFLINE',
        updatedAt: new Date(this.now()).toISOString(),
      };
      this.records.set(chargePointId, rec);
      this.evictIfNeeded();
    }
    return rec;
  }

  private evictIfNeeded(): void {
    while (this.records.size > this.maxRecords) {
      const oldest = this.records.keys().next().value as string | undefined;
      if (oldest === undefined) break;
      this.records.delete(oldest);
    }
  }

  private recomputeOperational(rec: ChargerConnectionRecord): void {
    if (rec.transport !== 'CONNECTED') {
      rec.operational = 'OFFLINE';
      return;
    }
    if (rec.registration !== 'ACCEPTED') {
      rec.operational = 'DEGRADED';
      return;
    }
    const last = rec.lastHeartbeatAt ?? rec.lastMessageAt ?? rec.connectedAt;
    const fresh = last ? this.now() - Date.parse(last) <= this.staleMs : false;
    rec.operational = fresh ? 'ONLINE' : 'DEGRADED';
  }

  private touch(rec: ChargerConnectionRecord): void {
    rec.updatedAt = new Date(this.now()).toISOString();
    this.recomputeOperational(rec);
  }

  onAttempt(chargePointId: string, meta: ConnectionMeta = {}): void {
    const rec = this.ensure(chargePointId);
    rec.transport = 'CONNECTING';
    if (meta.protocol) rec.negotiatedProtocol = meta.protocol;
    if (meta.remoteIp) rec.remoteIp = meta.remoteIp;
    rec.usedFallback = Boolean(meta.usedFallback);
    this.touch(rec);
    this.emit({
      chargePointId,
      connectionId: rec.connectionId,
      eventType: 'attempt',
      eventTimestamp: rec.updatedAt,
      disconnectReason: null,
      metadata: { protocol: meta.protocol ?? null, remoteIp: meta.remoteIp ?? null, usedFallback: rec.usedFallback },
    });
  }

  onRejected(chargePointId: string, reason: string, meta: ConnectionMeta = {}): void {
    const rec = this.ensure(chargePointId);
    rec.transport = 'DISCONNECTED';
    rec.disconnectReason = reason;
    rec.remoteIp = meta.remoteIp ?? rec.remoteIp;
    this.touch(rec);
    this.emit({
      chargePointId,
      connectionId: rec.connectionId,
      eventType: 'rejected',
      eventTimestamp: rec.updatedAt,
      disconnectReason: reason,
      metadata: { protocol: meta.protocol ?? null, remoteIp: meta.remoteIp ?? null },
    });
  }

  /** Registra o socket aceito e devolve o connectionId atribuido. */
  onConnected(chargePointId: string, meta: ConnectionMeta = {}): { connectionId: string; replaced: boolean } {
    const rec = this.ensure(chargePointId);
    const replaced = meta.replaced ?? rec.transport === 'CONNECTED';
    if (rec.connectedAt !== null) rec.reconnectCount += 1;
    const connectionId = randomUUID();
    rec.connectionId = connectionId;
    rec.transport = 'CONNECTED';
    rec.registration = 'PENDING';
    rec.negotiatedProtocol = meta.protocol ?? rec.negotiatedProtocol;
    rec.remoteIp = meta.remoteIp ?? rec.remoteIp;
    rec.usedFallback = Boolean(meta.usedFallback);
    rec.connectedAt = new Date(this.now()).toISOString();
    rec.disconnectReason = null;
    this.touch(rec);
    const at = rec.updatedAt;
    this.emit({
      chargePointId,
      connectionId,
      eventType: 'accepted',
      eventTimestamp: at,
      disconnectReason: null,
      metadata: {
        protocol: rec.negotiatedProtocol,
        remoteIp: rec.remoteIp,
        usedFallback: rec.usedFallback,
        reconnectCount: rec.reconnectCount,
      },
    });
    if (replaced) {
      this.emit({
        chargePointId,
        connectionId,
        eventType: 'replaced',
        eventTimestamp: at,
        disconnectReason: null,
        metadata: { reconnectCount: rec.reconnectCount },
      });
    }
    return { connectionId, replaced };
  }

  onBootAccepted(
    chargePointId: string,
    info: { vendor?: string | null; model?: string | null; firmwareVersion?: string | null } = {},
  ): void {
    const rec = this.ensure(chargePointId);
    rec.registration = 'ACCEPTED';
    rec.lastBootAt = new Date(this.now()).toISOString();
    if (info.vendor) rec.vendor = info.vendor;
    if (info.model) rec.model = info.model;
    if (info.firmwareVersion) rec.firmwareVersion = info.firmwareVersion;
    this.touch(rec);
    this.emit({
      chargePointId,
      connectionId: rec.connectionId,
      eventType: 'accepted',
      eventTimestamp: rec.updatedAt,
      disconnectReason: null,
      metadata: { kind: 'boot', vendor: rec.vendor, model: rec.model },
    });
  }

  onHeartbeat(chargePointId: string): void {
    const rec = this.ensure(chargePointId);
    rec.lastHeartbeatAt = new Date(this.now()).toISOString();
    this.touch(rec);
  }

  onMessage(chargePointId: string): void {
    const rec = this.ensure(chargePointId);
    rec.lastMessageAt = new Date(this.now()).toISOString();
    this.touch(rec);
  }

  onPong(chargePointId: string): void {
    const rec = this.ensure(chargePointId);
    rec.lastPongAt = new Date(this.now()).toISOString();
    this.touch(rec);
  }

  /** Registra um evento de transporte relevante (watchdog/pong timeout/erro). */
  onTransportEvent(chargePointId: string, eventType: ConnectionEvent['eventType'], reason: string | null = null): void {
    const rec = this.ensure(chargePointId);
    this.touch(rec);
    this.emit({
      chargePointId,
      connectionId: rec.connectionId,
      eventType,
      eventTimestamp: rec.updatedAt,
      disconnectReason: reason,
      metadata: {},
    });
  }

  /** Fecha o registro; ignora se o connectionId nao for mais o corrente. */
  onClosed(chargePointId: string, reason: string, connectionId?: string | null): void {
    const rec = this.records.get(chargePointId);
    if (!rec) return;
    if (connectionId && rec.connectionId && connectionId !== rec.connectionId) return;
    rec.transport = 'DISCONNECTED';
    rec.disconnectReason = reason;
    this.touch(rec);
    this.emit({
      chargePointId,
      connectionId: rec.connectionId,
      eventType: 'closed',
      eventTimestamp: rec.updatedAt,
      disconnectReason: reason,
      metadata: {},
    });
  }

  /**
   * Semeia a IDENTIDADE de um carregador conhecido (plano de reconexao, passo 2)
   * a partir de evidencias anteriores/persistidas, SEM marca-lo como conectado.
   *
   * Garantias:
   *  - `transport=DISCONNECTED`, `registration=NOT_REGISTERED`,
   *    `operational=OFFLINE`: a frota nunca nasce ONLINE (evita estado fantasma);
   *  - `connectionId` corrente nunca e reidratado (e fisica do runtime);
   *  - preserva historico: vendor/model/firmware e o ultimo
   *    boot/heartbeat/pong/mensagem/desconexao;
   *  - `connectedAt` e semeado como "ultima conexao", para que `reconnectCount`
   *    continue acumulando a partir do valor persistido;
   *  - nunca sobrescreve um socket ja CONNECTED (a sessao viva tem prioridade).
   */
  seedIdentity(entry: {
    chargePointId: string;
    vendor?: string | null;
    model?: string | null;
    firmwareVersion?: string | null;
    negotiatedProtocol?: string | null;
    connectedAt?: string | null;
    lastBootAt?: string | null;
    lastHeartbeatAt?: string | null;
    lastPongAt?: string | null;
    lastMessageAt?: string | null;
    disconnectReason?: string | null;
    reconnectCount?: number;
  }): void {
    const rec = this.ensure(entry.chargePointId);
    if (rec.transport === 'CONNECTED') return;
    if (entry.vendor != null) rec.vendor = entry.vendor;
    if (entry.model != null) rec.model = entry.model;
    if (entry.firmwareVersion != null) rec.firmwareVersion = entry.firmwareVersion;
    if (entry.negotiatedProtocol != null) rec.negotiatedProtocol = entry.negotiatedProtocol;
    if (entry.connectedAt != null) rec.connectedAt = entry.connectedAt;
    if (entry.lastBootAt != null) rec.lastBootAt = entry.lastBootAt;
    if (entry.lastHeartbeatAt != null) rec.lastHeartbeatAt = entry.lastHeartbeatAt;
    if (entry.lastPongAt != null) rec.lastPongAt = entry.lastPongAt;
    if (entry.lastMessageAt != null) rec.lastMessageAt = entry.lastMessageAt;
    if (entry.disconnectReason != null) rec.disconnectReason = entry.disconnectReason;
    if (typeof entry.reconnectCount === 'number' && Number.isFinite(entry.reconnectCount)) {
      rec.reconnectCount = entry.reconnectCount;
    }
    rec.connectionId = null;
    rec.transport = 'DISCONNECTED';
    rec.registration = 'NOT_REGISTERED';
    this.touch(rec);
  }

  get(chargePointId: string): ChargerConnectionRecord | undefined {
    return this.records.get(chargePointId);
  }

  snapshot(): ChargerConnectionRecord[] {
    return Array.from(this.records.values()).map((r) => ({ ...r }));
  }

  size(): number {
    return this.records.size;
  }
}
