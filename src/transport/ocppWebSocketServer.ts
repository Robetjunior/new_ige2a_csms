/**
 * Servidor WebSocket OCPP 1.6J: negociacao do handshake (RFC 6455 §4.1),
 * resolucao da rota/identidade e registro da sessao ativa por carregador.
 *
 * Handshake (regras herdadas do adaptador validado em producao):
 *   - o subprotocolo ECOADO deve ser sempre um valor oferecido pelo cliente;
 *   - o firmware Teison envia a variante `socpp1.6` quando a URL usa wss/TLS;
 *   - quando o cliente nao oferece subprotocolo conhecido e o fallback leniente
 *     esta habilitado, a conexao e aceita SEM eco de cabecalho;
 *   - quando o fallback esta desabilitado, a conexao e rejeitada (400).
 */
import type { IncomingMessage } from 'node:http';
import type { Duplex } from 'node:stream';
import { WebSocket, WebSocketServer } from 'ws';
import type { AppConfig } from '../config/env';
import type { ChargerSession, ChargerSessionDeps } from './chargerSession';
import { ChargerSession as ChargerSessionImpl } from './chargerSession';
import type { ConnectionManager } from './connectionManager';

export interface OcppWebSocketServerDeps {
  config: AppConfig;
  connections: ConnectionManager;
  sessionDeps: ChargerSessionDeps;
}

interface AcceptedConnection {
  chargePointId: string;
  protocol: string;
  remoteIp: string | null;
  usedFallback: boolean;
}

/** Desfecho de uma tentativa de handshake observada pelo servidor. */
export type UpgradeDecision =
  | 'accepted'
  | 'accepted_without_protocol'
  | 'rejected_path'
  | 'rejected_no_id'
  | 'rejected_protocol';

/**
 * Evento de observabilidade de uma tentativa de handshake.
 *
 * Existe porque o handshake nao passa pelo logger HTTP do Fastify: sem isto,
 * uma tentativa que chega na URL errada (404) ou com subprotocolo invalido
 * ficava completamente invisivel, impossibilitando distinguir "o carregador
 * nao chegou" de "chegou e foi rejeitado".
 */
export interface UpgradeAttemptEvent {
  /** Path efetivamente solicitado (ex.: /ocpp/CentralSystemService/ABC). */
  path: string;
  /** ID resolvido (path ou query), quando identificado. */
  chargePointId: string | null;
  /** IP de origem (respeita X-Forwarded-For). */
  remoteIp: string | null;
  /** User-Agent declarado: principal pista do firmware/modelo. */
  userAgent: string;
  /** Subprotocolos oferecidos pelo cliente. */
  protocols: string[];
  /** Subprotocolo finalmente negociado, quando houve handshake. */
  negotiatedProtocol: string | null;
  /** Desfecho da tentativa. */
  decision: UpgradeDecision;
  /** Motivo legivel quando rejeitada (null se aceita). */
  reason: string | null;
}

const OCPP16_PATH = '/ocpp/CentralSystemService';

/** Variantes aceitas para OCPP 1.6 (case-insensitive), incluindo as "secure". */
const OCPP16_PROTOCOLS = new Set([
  'ocpp1.6j',
  'ocpp1.6',
  'ocpp.1.6',
  'ocpp.1.6j',
  'socpp1.6',
  'socpp1.6j',
  'socpp.1.6',
  'socpp.1.6j',
]);

function isAcceptableProtocol(value: string): boolean {
  return OCPP16_PROTOCOLS.has(value.trim().toLowerCase());
}

function resolveRemoteIp(request: IncomingMessage): string | null {
  const forwarded = request.headers['x-forwarded-for'];
  if (typeof forwarded === 'string' && forwarded.length > 0) {
    const first = forwarded.split(',')[0];
    if (first) return first.trim();
  }
  return request.socket?.remoteAddress ?? null;
}

export class OcppWebSocketServer {
  private readonly deps: OcppWebSocketServerDeps;
  private readonly wss: WebSocketServer;
  private readonly registry = new Map<string, ChargerSession>();
  private upgradeLogger: ((event: UpgradeAttemptEvent) => void) | null = null;

  constructor(deps: OcppWebSocketServerDeps) {
    this.deps = deps;
    this.wss = new WebSocketServer({
      noServer: true,
      // Ecoa o primeiro subprotocolo oferecido (o cabecalho ja foi normalizado
      // para um unico valor aceito antes do handleUpgrade).
      handleProtocols: (protocols) => protocols.values().next().value ?? false,
    });
  }

  /** Registra o destino dos eventos de tentativa de handshake. Idempotente. */
  setLogger(logger: (event: UpgradeAttemptEvent) => void): void {
    this.upgradeLogger = logger;
  }

  /** Emite o evento de observabilidade; nunca deixa o logger afetar o protocolo. */
  private emitUpgrade(event: UpgradeAttemptEvent): void {
    if (!this.upgradeLogger) return;
    try {
      this.upgradeLogger(event);
    } catch {
      /* observabilidade nunca interrompe o handshake */
    }
  }

  /** Ponto de entrada do evento HTTP 'upgrade' do servidor. */
  handleUpgrade(request: IncomingMessage, socket: Duplex, head: Buffer): void {
    // Apos 'upgrade' o Node transfere a posse do socket: sem listener, um erro
    // de rede durante o handshake viraria excecao nao capturada.
    try {
      socket.on('error', () => {
        try {
          socket.destroy();
        } catch {
          /* noop */
        }
      });
    } catch {
      /* noop */
    }

    const reject = (statusCode: number, reason: string): void => {
      try {
        socket.write(`HTTP/1.1 ${statusCode} ${reason}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`);
      } catch {
        /* noop */
      }
      try {
        socket.destroy();
      } catch {
        /* noop */
      }
    };

    const url = new URL(`http://x${request.url ?? '/'}`);
    const pathname = url.pathname;
    const protoHeader = String(request.headers['sec-websocket-protocol'] ?? '');
    const protocols = protoHeader
      .split(',')
      .map((p) => p.trim())
      .filter((p) => p.length > 0);
    const remoteIp = resolveRemoteIp(request);
    const userAgent = String(request.headers['user-agent'] ?? '').trim() || 'unknown';

    if (pathname !== OCPP16_PATH && !pathname.startsWith(`${OCPP16_PATH}/`)) {
      this.emitUpgrade({
        path: pathname,
        chargePointId: null,
        remoteIp,
        userAgent,
        protocols,
        negotiatedProtocol: null,
        decision: 'rejected_path',
        reason: 'path fora de /ocpp/CentralSystemService',
      });
      reject(404, 'Not Found');
      return;
    }

    let chargePointId = pathname === OCPP16_PATH ? '' : decodeURIComponent(pathname.split('/').pop() ?? '');
    if (!chargePointId) {
      chargePointId = (url.searchParams.get('charge_box_id') ?? url.searchParams.get('id') ?? '').trim();
    }
    if (!chargePointId) {
      this.deps.connections.onRejected('', 'missing_charge_box_id', { remoteIp, protocol: protoHeader, userAgent });
      this.emitUpgrade({
        path: pathname,
        chargePointId: null,
        remoteIp,
        userAgent,
        protocols,
        negotiatedProtocol: null,
        decision: 'rejected_no_id',
        reason: 'chargePointId ausente no path e na query',
      });
      reject(400, 'Bad Request');
      return;
    }

    const accepted = protocols.find(isAcceptableProtocol);
    let usedFallback = false;
    if (!accepted) {
      if (!this.deps.config.ocpp.lenientProtocolFallback) {
        this.deps.connections.onRejected(chargePointId, 'invalid_subprotocol_1.6', {
          remoteIp,
          protocol: protoHeader,
          userAgent,
        });
        this.emitUpgrade({
          path: pathname,
          chargePointId,
          remoteIp,
          userAgent,
          protocols,
          negotiatedProtocol: null,
          decision: 'rejected_protocol',
          reason: `subprotocolo nao aceito: "${protoHeader}"`,
        });
        reject(400, 'Bad Request');
        return;
      }
      usedFallback = true;
    }

    const negotiatedProtocol = accepted ?? 'ocpp1.6';
    this.deps.connections.onAttempt(chargePointId, { protocol: negotiatedProtocol, remoteIp, usedFallback, userAgent });
    this.emitUpgrade({
      path: pathname,
      chargePointId,
      remoteIp,
      userAgent,
      protocols,
      negotiatedProtocol,
      decision: accepted ? 'accepted' : 'accepted_without_protocol',
      reason: null,
    });

    if (accepted) {
      request.headers['sec-websocket-protocol'] = accepted;
    } else {
      delete request.headers['sec-websocket-protocol'];
    }

    this.wss.handleUpgrade(request, socket, head, (ws) => {
      this.onConnection(ws, { chargePointId, protocol: negotiatedProtocol, remoteIp, usedFallback });
    });
  }

  private onConnection(ws: WebSocket, meta: AcceptedConnection): void {
    const previous = this.registry.get(meta.chargePointId);
    const replaced = Boolean(previous && previous.active);
    if (previous && replaced) previous.supersede();

    const { connectionId } = this.deps.connections.onConnected(meta.chargePointId, {
      protocol: meta.protocol,
      remoteIp: meta.remoteIp,
      usedFallback: meta.usedFallback,
      replaced,
    });

    const session = new ChargerSessionImpl(this.deps.sessionDeps, ws, {
      chargePointId: meta.chargePointId,
      connectionId,
      protocol: meta.protocol,
      remoteIp: meta.remoteIp,
      usedFallback: meta.usedFallback,
      onClosed: (chargePointId, closedConnectionId) => {
        const current = this.registry.get(chargePointId);
        if (current && current.connectionId === closedConnectionId) {
          this.registry.delete(chargePointId);
        }
      },
    });

    this.registry.set(meta.chargePointId, session);
  }

  getSession(chargePointId: string): ChargerSession | undefined {
    const session = this.registry.get(chargePointId);
    return session && session.isOpen() ? session : undefined;
  }

  /** Envia um comando do CSMS ao carregador conectado. */
  sendCall(chargePointId: string, action: string, payload: unknown, timeoutMs = 30_000): Promise<unknown> {
    const session = this.getSession(chargePointId);
    if (!session) {
      return Promise.reject(new Error(`Carregador nao conectado: ${chargePointId}`));
    }
    return session.sendCall(action, payload, timeoutMs);
  }

  isConnected(chargePointId: string): boolean {
    return this.getSession(chargePointId) !== undefined;
  }

  connectedChargePointIds(): string[] {
    return Array.from(this.registry.keys());
  }

  closeAll(): void {
    for (const session of this.registry.values()) session.close(1001, 'server shutting down');
    this.registry.clear();
  }
}
