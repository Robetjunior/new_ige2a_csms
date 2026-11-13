/**
 * Sessao de um carregador: dona do ciclo de vida de UM socket WebSocket.
 *
 * Responsabilidades:
 *  - capturar 100% das mensagens (entrada e saida) no motor de log;
 *  - parsear e despachar CALLs para o roteador OCPP, respondendo CALLRESULT/
 *    CALLERROR correlacionado por `uniqueId` (nunca encerra o socket por acao
 *    desconhecida);
 *  - manter o keepalive WebSocket (ping/pong) e o watchdog de inatividade;
 *  - permitir comandos originados pelo CSMS (CALL) com timeout.
 */
import { WebSocket } from 'ws';
import type { AppConfig } from '../config/env';
import type { OcppFrame, OcppMessageLog, MessageStatus } from '../domain/types';
import type { ChargerStateService } from '../chargers/chargerState';
import type { HeartbeatMonitor } from '../heartbeat/heartbeatMonitor';
import type { OcppMessageLogEngine } from '../logging/messageLogEngine';
import { OcppError, OcppErrorCode } from '../ocpp/errors';
import { parseOcppFrame, serializeCall, serializeCallError, serializeCallResult } from '../ocpp/protocol';
import { resolveOcpp16Handler } from '../ocpp/router';
import type { OcppCallContext } from '../ocpp/handlers';
import type { ConnectionManager } from './connectionManager';

export interface ChargerSessionDeps {
  config: AppConfig;
  chargers: ChargerStateService;
  heartbeat: HeartbeatMonitor;
  messageLog: OcppMessageLogEngine;
  connections: ConnectionManager;
}

export interface ChargerSessionOptions {
  chargePointId: string;
  connectionId: string;
  protocol: string;
  remoteIp: string | null;
  usedFallback: boolean;
  onClosed?: (chargePointId: string, connectionId: string) => void;
}

interface PendingCommand {
  action: string;
  resolve: (payload: unknown) => void;
  reject: (error: Error) => void;
  timer: ReturnType<typeof setTimeout>;
}

const CALL = 2;
const CALL_RESULT = 3;
const CALL_ERROR = 4;

function asRecord(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function decodeRaw(data: unknown): string {
  if (typeof data === 'string') return data;
  if (Buffer.isBuffer(data)) return data.toString('utf8');
  if (data instanceof ArrayBuffer) return Buffer.from(data).toString('utf8');
  if (Array.isArray(data)) return Buffer.concat(data as Buffer[]).toString('utf8');
  return String(data);
}

export class ChargerSession {
  readonly chargePointId: string;
  readonly connectionId: string;
  readonly protocol: string;
  readonly remoteIp: string | null;
  readonly usedFallback: boolean;

  private readonly deps: ChargerSessionDeps;
  private readonly ws: WebSocket;
  private readonly opts: ChargerSessionOptions;
  private readonly pendingCommands = new Map<string, PendingCommand>();
  private lastActivityAt: number;
  private lastPingAt: number;
  private awaitingPong = false;
  private watchdog: ReturnType<typeof setInterval> | null = null;
  private keepalive: ReturnType<typeof setInterval> | null = null;
  private superseded = false;
  private closed = false;
  private commandSequence = 0;

  constructor(deps: ChargerSessionDeps, ws: WebSocket, opts: ChargerSessionOptions) {
    this.deps = deps;
    this.ws = ws;
    this.opts = opts;
    this.chargePointId = opts.chargePointId;
    this.connectionId = opts.connectionId;
    this.protocol = opts.protocol;
    this.remoteIp = opts.remoteIp;
    this.usedFallback = opts.usedFallback;
    this.lastActivityAt = Date.now();
    this.lastPingAt = Date.now();
    this.attach();
  }

  private attach(): void {
    this.ws.on('message', (data) => {
      this.onMessage(data);
    });

    // Instancia WebSocket sem listener de 'error' derruba o processo inteiro.
    this.ws.on('error', (err) => {
      this.deps.connections.onTransportEvent(this.chargePointId, 'error', String((err as Error)?.message ?? err));
      try {
        this.ws.terminate();
      } catch {
        /* noop */
      }
    });

    this.ws.on('pong', () => {
      this.awaitingPong = false;
      this.lastActivityAt = Date.now();
      this.deps.connections.onPong(this.chargePointId);
    });

    this.ws.on('close', (code: number, reason: Buffer) => {
      this.onClose(code, reason);
    });

    this.startWatchdog();
    this.startKeepalive();
  }

  private startWatchdog(): void {
    const timeoutMs = this.deps.config.ocpp.watchdogTimeoutMs;
    const tickMs = Math.max(1000, Math.min(30000, timeoutMs));
    this.watchdog = setInterval(() => {
      if (Date.now() - this.lastActivityAt <= timeoutMs) return;
      this.deps.connections.onTransportEvent(this.chargePointId, 'watchdog_timeout', 'inatividade');
      try {
        this.ws.terminate();
      } catch {
        /* noop */
      }
    }, tickMs);
    this.watchdog.unref?.();
  }

  private startKeepalive(): void {
    const pingIntervalMs = this.deps.config.ocpp.pingIntervalMs;
    const pongTimeoutMs = this.deps.config.ocpp.pongTimeoutMs;
    const tickMs = Math.max(1000, Math.min(pingIntervalMs, pongTimeoutMs));
    this.keepalive = setInterval(() => {
      const now = Date.now();
      if (this.awaitingPong) {
        if (now - this.lastPingAt > pongTimeoutMs) {
          this.deps.connections.onTransportEvent(this.chargePointId, 'pong_timeout', `sem pong por ${now - this.lastPingAt}ms`);
          try {
            this.ws.terminate();
          } catch {
            /* noop */
          }
        }
        return;
      }
      if (now - this.lastPingAt >= pingIntervalMs) {
        this.lastPingAt = now;
        this.awaitingPong = true;
        try {
          this.ws.ping();
        } catch {
          /* noop */
        }
      }
    }, tickMs);
    this.keepalive.unref?.();
  }

  private onMessage(data: unknown): void {
    const observedAtMs = Date.now();
    this.lastActivityAt = observedAtMs;
    const raw = decodeRaw(data);
    this.deps.connections.onMessage(this.chargePointId);

    const parsed = parseOcppFrame(raw);
    if (!parsed.ok) {
      this.deps.messageLog.logFrame({
        chargePointId: this.chargePointId,
        connectionId: this.connectionId,
        direction: 'received',
        frame: null,
        raw,
        parseError: parsed.reason,
        status: 'failed',
      });
      // So e possivel responder quando o uniqueId foi recuperado do frame.
      if (parsed.uniqueId) {
        this.send(
          serializeCallError(parsed.uniqueId, OcppErrorCode.ProtocolError, `Frame OCPP invalido (${parsed.reason})`),
          'failed',
        );
      }
      return;
    }

    const frame = parsed.frame;
    const dto = this.deps.messageLog.logFrame({
      chargePointId: this.chargePointId,
      connectionId: this.connectionId,
      direction: 'received',
      frame,
      raw,
      status: frame.messageTypeId === CALL_ERROR ? 'failed' : frame.messageTypeId === CALL_RESULT ? 'succeeded' : 'received',
    });

    if (frame.messageTypeId === CALL) {
      void this.handleCall(frame, dto, observedAtMs);
    } else {
      this.handleResponse(frame);
    }
  }

  private async handleCall(frame: OcppFrame, dto: OcppMessageLog, observedAtMs: number): Promise<void> {
    this.deps.heartbeat.registerMessage(this.chargePointId);

    let resultPayload: unknown;
    let protocolError: OcppError | null = null;
    try {
      const handler = resolveOcpp16Handler(frame.action ?? '');
      const ctx: OcppCallContext = {
        chargePointId: this.chargePointId,
        connectionId: this.connectionId,
        config: this.deps.config,
        chargers: this.deps.chargers,
        heartbeat: this.deps.heartbeat,
      };
      resultPayload = await handler(ctx, asRecord(frame.payload));
    } catch (err) {
      protocolError =
        err instanceof OcppError
          ? err
          : new OcppError(OcppErrorCode.InternalError, String((err as Error)?.message ?? 'Erro interno'), {});
    }

    this.deps.messageLog.markProcessed(dto.id);

    if (frame.action === 'BootNotification' && !protocolError) {
      const req = asRecord(frame.payload);
      this.deps.connections.onBootAccepted(this.chargePointId, {
        vendor: typeof req.chargePointVendor === 'string' ? req.chargePointVendor : null,
        model: typeof req.chargePointModel === 'string' ? req.chargePointModel : null,
        firmwareVersion: typeof req.firmwareVersion === 'string' ? req.firmwareVersion : null,
      });
    }

    if (protocolError) {
      this.send(
        serializeCallError(frame.uniqueId, protocolError.code, protocolError.message, protocolError.details),
        'failed',
      );
    } else {
      this.send(serializeCallResult(frame.uniqueId, resultPayload), 'succeeded');
    }

    if (frame.action === 'Heartbeat') {
      const latencyMs = Date.now() - observedAtMs;
      this.deps.heartbeat.registerHeartbeat(this.chargePointId, latencyMs);
      this.deps.connections.onHeartbeat(this.chargePointId);
    }
  }

  private handleResponse(frame: OcppFrame): void {
    const pending = this.pendingCommands.get(frame.uniqueId);
    if (!pending) return;
    clearTimeout(pending.timer);
    this.pendingCommands.delete(frame.uniqueId);
    if (frame.messageTypeId === CALL_RESULT) {
      pending.resolve(frame.payload);
    } else {
      const err = frame.error;
      pending.reject(new Error(`${err?.code ?? 'CALLERROR'}: ${err?.description ?? ''}`));
    }
  }

  /** Envia um frame ja serializado e o registra no motor de log. */
  private send(text: string, status: MessageStatus): boolean {
    let delivered = true;
    if (this.ws.readyState !== WebSocket.OPEN) {
      delivered = false;
    } else {
      try {
        this.ws.send(text);
      } catch {
        delivered = false;
      }
    }
    const parsed = parseOcppFrame(text);
    this.deps.messageLog.logFrame({
      chargePointId: this.chargePointId,
      connectionId: this.connectionId,
      direction: 'sent',
      frame: parsed.ok ? parsed.frame : null,
      raw: text,
      parseError: parsed.ok ? null : `serialize:${parsed.reason}`,
      status: delivered ? status : 'failed',
      ...(delivered
        ? {}
        : { errorCode: null, errorMessage: 'Falha ao enviar frame: socket nao esta aberto' }),
    });
    return delivered;
  }

  /** Envia um comando do CSMS para o carregador (CALL) aguardando a resposta. */
  sendCall(action: string, payload: unknown, timeoutMs = 30_000): Promise<unknown> {
    this.commandSequence += 1;
    const uniqueId = `csms-${this.connectionId.slice(0, 8)}-${this.commandSequence}`;
    const text = serializeCall(uniqueId, action, payload);
    return new Promise<unknown>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pendingCommands.delete(uniqueId);
        reject(new Error(`Timeout aguardando resposta de ${action} (${timeoutMs}ms)`));
      }, timeoutMs);
      timer.unref?.();
      this.pendingCommands.set(uniqueId, { action, resolve, reject, timer });
      const delivered = this.send(text, 'succeeded');
      if (!delivered) {
        clearTimeout(timer);
        this.pendingCommands.delete(uniqueId);
        reject(new Error('Socket nao esta aberto para enviar o comando'));
      }
    });
  }

  isOpen(): boolean {
    return !this.closed && this.ws.readyState === WebSocket.OPEN;
  }

  get active(): boolean {
    return !this.closed && !this.superseded;
  }

  /** Encerra o socket por substituicao (nova conexao do mesmo chargePointId). */
  supersede(): void {
    this.superseded = true;
    try {
      this.ws.close(1000, 'superseded by new connection');
    } catch {
      try {
        this.ws.terminate();
      } catch {
        /* noop */
      }
    }
  }

  close(code = 1000, reason = 'closed by csms'): void {
    try {
      this.ws.close(code, reason);
    } catch {
      try {
        this.ws.terminate();
      } catch {
        /* noop */
      }
    }
  }

  private onClose(code: number, reason: Buffer): void {
    if (this.closed) return;
    this.closed = true;
    this.clearTimers();
    for (const pending of this.pendingCommands.values()) {
      clearTimeout(pending.timer);
      pending.reject(new Error('Conexao encerrada antes da resposta'));
    }
    this.pendingCommands.clear();
    const reasonText = `${code}${reason && reason.length ? `:${reason.toString().slice(0, 120)}` : ''}`;
    this.deps.connections.onClosed(this.chargePointId, reasonText, this.connectionId);
    this.opts.onClosed?.(this.chargePointId, this.connectionId);
  }

  private clearTimers(): void {
    if (this.watchdog) clearInterval(this.watchdog);
    if (this.keepalive) clearInterval(this.keepalive);
    this.watchdog = null;
    this.keepalive = null;
  }
}
