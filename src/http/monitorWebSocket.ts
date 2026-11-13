/**
 * Canal WebSocket de monitoramento (alternativa/complemento ao SSE) para a
 * Central de Monitoramento. Transmite mensagens OCPP, eventos de conexao e um
 * snapshot inicial no momento da conexao.
 */
import type { IncomingMessage } from 'node:http';
import type { Duplex } from 'node:stream';
import { WebSocket, WebSocketServer } from 'ws';
import type { CsmsComponents } from '../components';

export class MonitorWebSocketServer {
  private readonly components: CsmsComponents;
  private readonly wss: WebSocketServer;
  private readonly clients = new Set<WebSocket>();
  private readonly unsubscribe: Array<() => void>;

  constructor(components: CsmsComponents) {
    this.components = components;
    this.wss = new WebSocketServer({ noServer: true });
    this.wss.on('connection', (ws) => this.onConnection(ws));

    const unsubscribeLog = components.messageLog.subscribe((entry) =>
      this.broadcast({ type: 'ocpp-message', data: entry }),
    );
    const unsubscribeConn = components.connections.subscribe((event) =>
      this.broadcast({ type: 'connection', data: event }),
    );
    this.unsubscribe = [unsubscribeLog, unsubscribeConn];
  }

  handleUpgrade(request: IncomingMessage, socket: Duplex, head: Buffer): void {
    this.wss.handleUpgrade(request, socket, head, (ws) => {
      this.wss.emit('connection', ws, request);
    });
  }

  private onConnection(ws: WebSocket): void {
    this.clients.add(ws);
    this.sendTo(ws, {
      type: 'snapshot',
      data: {
        chargers: this.components.connections.snapshot(),
        heartbeat: this.components.heartbeat.snapshot(),
        messageStats: this.components.messageLog.stats(),
      },
    });

    ws.on('close', () => this.clients.delete(ws));
    ws.on('error', () => {
      this.clients.delete(ws);
      try {
        ws.terminate();
      } catch {
        /* noop */
      }
    });
    ws.on('message', (data) => {
      // Handshake simples: o cliente pode enviar {"type":"ping"} para manter vivo.
      try {
        const parsed = JSON.parse(data.toString()) as { type?: string };
        if (parsed?.type === 'ping') this.sendTo(ws, { type: 'pong', data: { at: new Date().toISOString() } });
      } catch {
        /* ignora payload invalido */
      }
    });
  }

  private sendTo(ws: WebSocket, message: unknown): void {
    if (ws.readyState !== WebSocket.OPEN) return;
    try {
      ws.send(JSON.stringify(message));
    } catch {
      /* noop */
    }
  }

  private broadcast(message: unknown): void {
    if (this.clients.size === 0) return;
    const payload = JSON.stringify(message);
    for (const ws of this.clients) {
      if (ws.readyState !== WebSocket.OPEN) continue;
      try {
        ws.send(payload);
      } catch {
        /* noop */
      }
    }
  }

  size(): number {
    return this.clients.size;
  }

  closeAll(): void {
    for (const unsubscribe of this.unsubscribe) unsubscribe();
    for (const ws of this.clients) {
      try {
        ws.close(1001, 'server shutting down');
      } catch {
        /* noop */
      }
    }
    this.clients.clear();
    this.wss.close();
  }
}
