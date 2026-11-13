/**
 * Monitor de Heartbeat OCPP por estacao.
 *
 * Regras de projeto (spec §4):
 *  - usa o intervalo EFETIVAMENTE negociado com cada carregador (nao um valor global);
 *  - NUNCA marca o carregador como OFFLINE apenas porque um Heartbeat nao chegou:
 *    OFFLINE depende do estado da conexao WebSocket, nao da ausencia de heartbeat;
 *  - outras mensagens OCPP atualizam "ultima comunicacao" (lastOcppMessageAt) sem
 *    falsificar o horario do ultimo Heartbeat.
 */
import type { HeartbeatClassification, HeartbeatStatus } from '../domain/types';

interface HeartbeatRecord {
  negotiatedIntervalSeconds: number;
  lastHeartbeatAt: number | null;
  lastOcppMessageAt: number | null;
  heartbeatCount: number;
  delayedCount: number;
  latencySumMs: number;
  latencySamples: number;
  maxLatencyMs: number | null;
  connectionOnline: boolean;
  connectionChangedAt: number;
  updatedAt: number;
}

export interface HeartbeatMonitorOptions {
  defaultIntervalSeconds: number;
  toleranceSeconds: number;
  criticalSeconds: number;
  now?: () => number;
}

export class HeartbeatMonitor {
  private readonly records = new Map<string, HeartbeatRecord>();
  private readonly defaultIntervalSeconds: number;
  private readonly toleranceMs: number;
  private readonly criticalMs: number;
  private readonly now: () => number;

  constructor(opts: HeartbeatMonitorOptions) {
    this.defaultIntervalSeconds = opts.defaultIntervalSeconds;
    this.toleranceMs = opts.toleranceSeconds * 1000;
    this.criticalMs = opts.criticalSeconds * 1000;
    this.now = opts.now ?? (() => Date.now());
  }

  private ensure(chargePointId: string): HeartbeatRecord {
    let rec = this.records.get(chargePointId);
    if (!rec) {
      const t = this.now();
      rec = {
        negotiatedIntervalSeconds: this.defaultIntervalSeconds,
        lastHeartbeatAt: null,
        lastOcppMessageAt: null,
        heartbeatCount: 0,
        delayedCount: 0,
        latencySumMs: 0,
        latencySamples: 0,
        maxLatencyMs: null,
        connectionOnline: false,
        connectionChangedAt: t,
        updatedAt: t,
      };
      this.records.set(chargePointId, rec);
    }
    return rec;
  }

  /** Define o intervalo negociado (ex.: valor informado no BootNotification.conf). */
  setNegotiatedInterval(chargePointId: string, seconds: number): void {
    const rec = this.ensure(chargePointId);
    if (Number.isFinite(seconds) && seconds > 0) {
      rec.negotiatedIntervalSeconds = Math.round(seconds);
      rec.updatedAt = this.now();
    }
  }

  /**
   * Semeia a identidade de um carregador conhecido (plano de reconexao, passo 2)
   * SEM simular atividade: `connectionOnline` permanece `false`, portanto a
   * classificacao e `OFFLINE` ate existir socket real.
   *
   * `lastHeartbeatAt` NAO e semeado de proposito: o horario do ultimo heartbeat
   * pertence a epoca do socket atual. Semee-lo produziria ATRASADO/CRITICO falso
   * entre a reconexao e o primeiro Heartbeat da nova sessao. Preserva-se apenas
   * o intervalo negociado e os contadores historicos.
   */
  seedIdentity(
    chargePointId: string,
    info: { negotiatedIntervalSeconds?: number | null; heartbeatCount?: number; delayedCount?: number } = {},
  ): void {
    const rec = this.ensure(chargePointId);
    if (rec.connectionOnline) return;
    if (typeof info.negotiatedIntervalSeconds === 'number' && Number.isFinite(info.negotiatedIntervalSeconds)) {
      rec.negotiatedIntervalSeconds = Math.max(1, Math.round(info.negotiatedIntervalSeconds));
    }
    if (typeof info.heartbeatCount === 'number' && Number.isFinite(info.heartbeatCount)) {
      rec.heartbeatCount = Math.max(0, Math.floor(info.heartbeatCount));
    }
    if (typeof info.delayedCount === 'number' && Number.isFinite(info.delayedCount)) {
      rec.delayedCount = Math.max(0, Math.floor(info.delayedCount));
    }
    rec.updatedAt = this.now();
  }

  getNegotiatedInterval(chargePointId: string): number {
    return this.ensure(chargePointId).negotiatedIntervalSeconds;
  }

  /** Registra qualquer mensagem OCPP recebida (atualiza ultima comunicacao). */
  registerMessage(chargePointId: string): void {
    const rec = this.ensure(chargePointId);
    rec.lastOcppMessageAt = this.now();
    rec.updatedAt = rec.lastOcppMessageAt;
  }

  /**
   * Registra um Heartbeat. `latencyMs` opcional = tempo de processamento/resposta.
   * Contabiliza Heartbeats perdidos/atrasados com base no intervalo negociado.
   */
  registerHeartbeat(chargePointId: string, latencyMs?: number): void {
    const rec = this.ensure(chargePointId);
    const t = this.now();
    if (rec.lastHeartbeatAt !== null) {
      const elapsed = t - rec.lastHeartbeatAt;
      const deadline = rec.negotiatedIntervalSeconds * 1000 + this.toleranceMs;
      if (elapsed > deadline) {
        const missed = Math.max(1, Math.floor(elapsed / (rec.negotiatedIntervalSeconds * 1000)) - 1);
        rec.delayedCount += missed;
      }
    }
    rec.lastHeartbeatAt = t;
    rec.heartbeatCount += 1;
    if (typeof latencyMs === 'number' && Number.isFinite(latencyMs)) {
      const clamped = Math.max(0, latencyMs);
      rec.latencySumMs += clamped;
      rec.latencySamples += 1;
      rec.maxLatencyMs = rec.maxLatencyMs === null ? clamped : Math.max(rec.maxLatencyMs, clamped);
    }
    rec.updatedAt = t;
  }

  /** Atualiza o estado real da conexao WebSocket (unica fonte de OFFLINE). */
  setConnectionStatus(chargePointId: string, online: boolean): void {
    const rec = this.ensure(chargePointId);
    if (rec.connectionOnline !== online) {
      rec.connectionOnline = online;
      rec.connectionChangedAt = this.now();
    }
    rec.updatedAt = this.now();
  }

  private classify(rec: HeartbeatRecord): HeartbeatClassification {
    if (!rec.connectionOnline) return 'OFFLINE';
    if (rec.lastHeartbeatAt === null) return 'SEM_DADOS';
    const elapsed = this.now() - rec.lastHeartbeatAt;
    const expected = rec.negotiatedIntervalSeconds * 1000;
    if (elapsed <= expected + this.toleranceMs) return 'NORMAL';
    if (elapsed <= expected + this.toleranceMs + this.criticalMs) return 'ATRASADO';
    return 'CRITICO';
  }

  getStatus(chargePointId: string): HeartbeatStatus {
    const rec = this.ensure(chargePointId);
    const expectedIntervalMs = rec.negotiatedIntervalSeconds * 1000;
    const t = this.now();
    return {
      chargePointId,
      negotiatedIntervalSeconds: rec.negotiatedIntervalSeconds,
      lastHeartbeatAt: rec.lastHeartbeatAt === null ? null : new Date(rec.lastHeartbeatAt).toISOString(),
      nextExpectedAt:
        rec.lastHeartbeatAt === null ? null : new Date(rec.lastHeartbeatAt + expectedIntervalMs).toISOString(),
      lastOcppMessageAt: rec.lastOcppMessageAt === null ? null : new Date(rec.lastOcppMessageAt).toISOString(),
      heartbeatCount: rec.heartbeatCount,
      delayedCount: rec.delayedCount,
      averageLatencyMs: rec.latencySamples === 0 ? null : Math.round(rec.latencySumMs / rec.latencySamples),
      maxLatencyMs: rec.maxLatencyMs,
      connectionStatus: rec.connectionOnline ? 'ONLINE' : 'OFFLINE',
      classification: this.classify(rec),
      secondsSinceLastHeartbeat:
        rec.lastHeartbeatAt === null ? null : Math.max(0, Math.round((t - rec.lastHeartbeatAt) / 1000)),
      updatedAt: new Date(rec.updatedAt).toISOString(),
    };
  }

  snapshot(): HeartbeatStatus[] {
    return Array.from(this.records.keys()).map((id) => this.getStatus(id));
  }

  size(): number {
    return this.records.size;
  }
}
