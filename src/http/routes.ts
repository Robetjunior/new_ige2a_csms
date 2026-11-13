/**
 * API HTTP de monitoramento (spec §7/§8): saude, visao geral, carregadores,
 * heartbeat, conexoes, mensagens (consulta/filtros/export) e stream em tempo
 * real via SSE. Todas as rotas /api/* podem ser protegidas por CSMS_API_KEY.
 */
import type { FastifyInstance } from 'fastify';
import type { CsmsComponents } from '../components';
import { buildDiagnosis } from '../diagnostics/diagnosis';
import type { MessageLogQuery } from '../logging/messageLogEngine';
import type { HeartbeatStatus, OcppMessageLog } from '../domain/types';

const HEARTBEAT_PUSH_INTERVAL_MS = 5_000;
const SSE_KEEPALIVE_MS = 15_000;

function asString(value: unknown): string | undefined {
  return typeof value === 'string' && value.length > 0 ? value : undefined;
}

function asNumber(value: unknown): number | undefined {
  if (typeof value !== 'string' || value.length === 0) return undefined;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function parseMessageQuery(query: Record<string, unknown>): MessageLogQuery {
  const direction = asString(query.direction);
  return {
    chargePointId: asString(query.chargePointId),
    connectionId: asString(query.connectionId),
    direction: direction === 'received' || direction === 'sent' ? direction : undefined,
    action: asString(query.action),
    messageType: asString(query.messageType),
    status: asString(query.status),
    onlyErrors: query.onlyErrors === '1' || query.onlyErrors === 'true',
    search: asString(query.search),
    from: asString(query.from),
    to: asString(query.to),
    limit: asNumber(query.limit),
  };
}

function csvCell(value: unknown): string {
  const text = value === null || value === undefined ? '' : typeof value === 'string' ? value : JSON.stringify(value);
  return `"${text.replace(/"/g, '""')}"`;
}

function chargerStateView(components: CsmsComponents, chargePointId: string): Record<string, unknown> | null {
  const charger = components.chargers.get(chargePointId);
  if (!charger) return null;
  return {
    vendor: charger.vendor,
    model: charger.model,
    serialNumber: charger.serialNumber,
    firmwareVersion: charger.firmwareVersion,
    bootAt: charger.bootAt,
    lastStatusAt: charger.lastStatusAt,
    connectors: Array.from(charger.connectors.values()),
    meterReadings: Array.from(charger.meterReadings.values()),
    activeTransactions: Array.from(charger.activeTransactions.entries()).map(([connectorId, txId]) => ({
      connectorId,
      transactionId: txId,
    })),
  };
}

function toCsv(entries: OcppMessageLog[]): string {
  const header = [
    'id',
    'chargePointId',
    'connectionId',
    'direction',
    'messageType',
    'action',
    'uniqueId',
    'observedAt',
    'observedAtLocal',
    'processedAt',
    'respondedAt',
    'latencyMs',
    'status',
    'errorCode',
    'errorMessage',
    'messageBytes',
    'parseError',
    'payload',
  ];
  const lines = [header.join(',')];
  for (const e of entries) {
    lines.push(
      [
        e.id,
        e.chargePointId,
        e.connectionId,
        e.direction,
        e.messageType,
        e.action,
        e.uniqueId,
        e.observedAt,
        e.observedAtLocal,
        e.processedAt,
        e.respondedAt,
        e.latencyMs,
        e.status,
        e.errorCode,
        e.errorMessage,
        e.messageBytes,
        e.parseError,
        e.payload,
      ]
        .map(csvCell)
        .join(','),
    );
  }
  return lines.join('\n');
}

function mergeOverview(components: CsmsComponents): unknown[] {
  const connections = new Map(components.connections.snapshot().map((c) => [c.chargePointId, c]));
  const heartbeats = new Map(components.heartbeat.snapshot().map((h) => [h.chargePointId, h]));
  const fleet = new Map(components.reconnectionWatcher.snapshot().map((m) => [m.chargePointId, m]));
  const ids = new Set<string>([
    ...connections.keys(),
    ...heartbeats.keys(),
    ...components.chargers.snapshot().map((c) => c.chargePointId),
    ...components.fleet.ids(),
  ]);
  return Array.from(ids)
    .sort()
    .map((chargePointId) => ({
      chargePointId,
      connection: connections.get(chargePointId) ?? null,
      heartbeat: heartbeats.get(chargePointId) ?? null,
      charger: chargerStateView(components, chargePointId),
      availabilityProbe: components.availabilityProbe.get(chargePointId),
      fleet: fleet.get(chargePointId) ?? null,
      connected: components.wsServer.isConnected(chargePointId),
    }));
}

function fleetSummary(components: CsmsComponents): {
  enabled: boolean;
  total: number;
  awaitingReconnection: number;
  connected: number;
} {
  const members = components.reconnectionWatcher.snapshot();
  return {
    enabled: components.config.ocpp.fleetEnabled,
    total: members.length,
    awaitingReconnection: members.filter((m) => m.status === 'AWAITING_RECONNECTION').length,
    connected: members.filter((m) => m.status === 'CONNECTED').length,
  };
}

export async function registerRoutes(fastify: FastifyInstance, components: CsmsComponents): Promise<void> {
  const apiKey = components.config.apiKey;

  fastify.addHook('onRequest', async (request, reply) => {
    if (!apiKey) return;
    if (!(request.raw.url ?? '').startsWith('/api/')) return;
    const header = request.headers['x-api-key'];
    const fromQuery = (request.query as Record<string, unknown> | undefined)?.api_key;
    const presented = typeof header === 'string' ? header.trim() : typeof fromQuery === 'string' ? fromQuery.trim() : '';
    if (presented !== apiKey) {
      await reply.code(401).send({ error: 'unauthorized', message: 'CSMS_API_KEY invalida ou ausente' });
    }
  });

  fastify.get('/health', async () => ({
    status: 'ok',
    env: components.config.env,
    uptimeSeconds: Math.round(process.uptime()),
    connectedChargers: components.wsServer.connectedChargePointIds().length,
    timestamp: new Date().toISOString(),
  }));

  fastify.get('/api/overview', async () => ({
    generatedAt: new Date().toISOString(),
    chargers: mergeOverview(components),
    messageStats: components.messageLog.stats(),
    fleetSummary: fleetSummary(components),
  }));

  fastify.get('/api/chargers', async () => ({
    chargers: mergeOverview(components),
    fleetSummary: fleetSummary(components),
  }));

  // Frota conhecida e status de reconexao ("quem e conhecido e NAO voltou").
  fastify.get('/api/fleet', async () => ({
    enabled: components.config.ocpp.fleetEnabled,
    generatedAt: new Date().toISOString(),
    checkIntervalMs: components.config.ocpp.reconnectionCheckIntervalMs,
    graceMultiplier: components.config.ocpp.reconnectionGraceMultiplier,
    toleranceSeconds: components.config.ocpp.reconnectionToleranceSeconds,
    summary: fleetSummary(components),
    members: components.reconnectionWatcher.snapshot(),
  }));

  fastify.get('/api/chargers/:chargePointId', async (request, reply) => {
    const { chargePointId } = request.params as { chargePointId: string };
    const connection = components.connections.get(chargePointId);
    const heartbeat = components.heartbeat.getStatus(chargePointId);
    const charger = components.chargers.get(chargePointId);
    if (!connection && !charger && heartbeat.lastOcppMessageAt === null) {
      await reply.code(404).send({ error: 'not_found', chargePointId });
      return;
    }
    return {
      chargePointId,
      online: components.wsServer.isConnected(chargePointId),
      connection: connection ?? null,
      heartbeat,
      charger: chargerStateView(components, chargePointId),
      availabilityProbe: components.availabilityProbe.get(chargePointId),
      fleet: components.reconnectionWatcher.getStatus(chargePointId),
      recentMessages: components.messageLog.recent({ chargePointId, limit: 50 }),
    };
  });

  fastify.get('/api/availability-probes', async () => ({
    enabled: components.config.ocpp.statusProbeEnabled,
    probes: components.wsServer.connectedChargePointIds().map((id) => components.availabilityProbe.get(id)),
    events: components.availabilityProbe.recent(undefined, 100),
  }));

  fastify.post('/api/chargers/:chargePointId/probe', async (request, reply) => {
    const { chargePointId } = request.params as { chargePointId: string };
    if (!components.wsServer.isConnected(chargePointId)) {
      await reply.code(409).send({ error: 'not_connected', chargePointId });
      return;
    }
    void components.availabilityProbe.probeNow(chargePointId);
    await reply.code(202).send({ scheduled: true, chargePointId });
  });

  // Sondagem OCPP DIRECIONADA ao status: pede SO StatusNotification
  // (TriggerMessage) e devolve o resultado + o estado dos conectores.
  fastify.post('/api/chargers/:chargePointId/probe-status', async (request, reply) => {
    const { chargePointId } = request.params as { chargePointId: string };
    if (!components.wsServer.isConnected(chargePointId)) {
      await reply.code(409).send({ error: 'not_connected', chargePointId });
      return;
    }
    const probe = await components.availabilityProbe.probeSingleNow(chargePointId, 'StatusNotification');
    return {
      chargePointId,
      requestedMessage: 'StatusNotification',
      probe,
      connectorStatuses: components.availabilityProbe.get(chargePointId),
      charger: chargerStateView(components, chargePointId),
    };
  });

  fastify.get('/api/chargers/:chargePointId/diagnosis', async (request) => {
    const { chargePointId } = request.params as { chargePointId: string };
    return buildDiagnosis(chargePointId, components);
  });

  fastify.get('/api/heartbeat', async () => ({ heartbeat: components.heartbeat.snapshot() }));

  fastify.get('/api/connections', async () => ({ connections: components.connections.snapshot() }));

  fastify.get('/api/messages', async (request) => {
    const query = parseMessageQuery((request.query as Record<string, unknown>) ?? {});
    return { messages: components.messageLog.recent(query), stats: components.messageLog.stats() };
  });

  fastify.get('/api/messages/stats', async () => components.messageLog.stats());

  fastify.get('/api/messages/export', async (request, reply) => {
    const query = parseMessageQuery((request.query as Record<string, unknown>) ?? {});
    const format = asString((request.query as Record<string, unknown>)?.format) ?? 'json';
    const messages = components.messageLog.recent({ ...query, limit: query.limit ?? 5000 });
    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    if (format === 'csv') {
      await reply
        .header('content-type', 'text/csv; charset=utf-8')
        .header('content-disposition', `attachment; filename="ocpp-messages-${stamp}.csv"`)
        .send(toCsv(messages));
      return;
    }
    await reply
      .header('content-type', 'application/json; charset=utf-8')
      .header('content-disposition', `attachment; filename="ocpp-messages-${stamp}.json"`)
      .send({ generatedAt: new Date().toISOString(), count: messages.length, messages });
  });

  fastify.post('/api/chargers/:chargePointId/commands', async (request, reply) => {
    const { chargePointId } = request.params as { chargePointId: string };
    const body = (request.body ?? {}) as { action?: unknown; payload?: unknown; timeoutMs?: unknown };
    if (typeof body.action !== 'string' || body.action.length === 0) {
      await reply.code(400).send({ error: 'bad_request', message: 'Campo "action" e obrigatorio' });
      return;
    }
    try {
      const result = await components.wsServer.sendCall(
        chargePointId,
        body.action,
        body.payload ?? {},
        typeof body.timeoutMs === 'number' ? body.timeoutMs : 30_000,
      );
      return { chargePointId, action: body.action, result };
    } catch (err) {
      await reply.code(502).send({ error: 'command_failed', message: String((err as Error)?.message ?? err) });
    }
  });

  // Stream em tempo real (SSE): mensagens OCPP, eventos de conexao e heartbeat.
  fastify.get('/api/stream', (request, reply) => {
    reply.hijack();
    const res = reply.raw;
    res.writeHead(200, {
      'content-type': 'text/event-stream; charset=utf-8',
      'cache-control': 'no-cache, no-transform',
      connection: 'keep-alive',
      'x-accel-buffering': 'no',
    });
    res.write(': connected\n\n');

    const send = (event: string, data: unknown): void => {
      try {
        res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
      } catch {
        /* cliente desconectou */
      }
    };

    send('snapshot', {
      chargers: mergeOverview(components),
      messageStats: components.messageLog.stats(),
      heartbeat: components.heartbeat.snapshot(),
      availabilityProbeEvents: components.availabilityProbe.recent(undefined, 100),
      fleet: components.reconnectionWatcher.snapshot(),
    });

    const unsubscribeLog = components.messageLog.subscribe((entry) => send('ocpp-message', entry));
    const unsubscribeConn = components.connections.subscribe((event) => send('connection', event));
    const unsubscribeCharger = components.chargers.subscribe((chargePointId) =>
      send('charger-state', { chargePointId, charger: chargerStateView(components, chargePointId) }),
    );
    const unsubscribeProbe = components.availabilityProbe.subscribe((event, snapshot) =>
      send('availability-probe', { event, snapshot }),
    );
    const unsubscribeFleet = components.reconnectionWatcher.subscribe((event) => send('fleet-status', event));
    const heartbeatTimer = setInterval(() => {
      send('heartbeat', components.heartbeat.snapshot() satisfies HeartbeatStatus[]);
    }, HEARTBEAT_PUSH_INTERVAL_MS);
    heartbeatTimer.unref?.();
    const keepaliveTimer = setInterval(() => {
      try {
        res.write(': ping\n\n');
      } catch {
        /* noop */
      }
    }, SSE_KEEPALIVE_MS);
    keepaliveTimer.unref?.();

    request.raw.on('close', () => {
      clearInterval(heartbeatTimer);
      clearInterval(keepaliveTimer);
      unsubscribeLog();
      unsubscribeConn();
      unsubscribeCharger();
      unsubscribeProbe();
      unsubscribeFleet();
      try {
        res.end();
      } catch {
        /* noop */
      }
    });
  });
}
