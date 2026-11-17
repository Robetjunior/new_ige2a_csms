/**
 * Composicao da aplicacao: monta os componentes, o servidor HTTP (Fastify) e
 * roteia os upgrades WebSocket entre o endpoint OCPP e o canal de monitoramento.
 */
import path from 'node:path';
import type { IncomingMessage } from 'node:http';
import type { Duplex } from 'node:stream';
import Fastify, { type FastifyInstance } from 'fastify';
import cors from '@fastify/cors';
import fastifyStatic from '@fastify/static';
import type { AppConfig } from './config/env';
import { loadConfig } from './config/env';
import type { ConnectionEvent } from './domain/types';
import type { AvailabilityProbeEvent } from './availability/availabilityProbe';
import { ChargerStateService } from './chargers/chargerState';
import { HeartbeatMonitor } from './heartbeat/heartbeatMonitor';
import { OcppMessageLogEngine } from './logging/messageLogEngine';
import { toBrasiliaIso } from './logging/time';
import { ConnectionManager } from './transport/connectionManager';
import { OcppWebSocketServer } from './transport/ocppWebSocketServer';
import type { CsmsComponents } from './components';
import { registerRoutes } from './http/routes';
import { createIntegrationModule } from './integration';
import { registerIntegrationRoutes } from './integration/routes';
import { MonitorWebSocketServer } from './http/monitorWebSocket';
import {
  createPersistenceService,
  type PersistenceDependencies,
  type PersistenceService,
} from './persistence/persistenceService';
import { AvailabilityProbeService } from './availability/availabilityProbe';
import { FleetRegistry, loadFleetFromFile, type FleetSeed } from './fleet/fleetRegistry';
import { ReconnectionWatcher } from './fleet/reconnectionWatcher';
import { FleetStateSyncer } from './fleet/fleetStateSyncer';

export interface CsmsApp {
  fastify: FastifyInstance;
  components: CsmsComponents;
  monitorWs: MonitorWebSocketServer;
  start(): Promise<void>;
  stop(): Promise<void>;
}

/** Injeção de dependências para testes (ex.: cliente Supabase falso). */
export interface AppDependencies {
  persistence?: PersistenceDependencies;
}

/** Liga o estado do socket (unica fonte de OFFLINE) ao monitor de heartbeat. */
function linkHeartbeatToConnections(components: CsmsComponents): void {
  const { connections, heartbeat } = components;
  connections.subscribe((event) => {
    if (!event.chargePointId) return;
    switch (event.eventType) {
      case 'accepted':
        heartbeat.setConnectionStatus(event.chargePointId, true);
        break;
      case 'closed':
      case 'rejected':
      case 'watchdog_timeout':
      case 'pong_timeout':
        heartbeat.setConnectionStatus(event.chargePointId, false);
        break;
      default:
        break;
    }
  });
}

export function buildComponents(config: AppConfig): CsmsComponents {
  const chargers = new ChargerStateService();
  const heartbeat = new HeartbeatMonitor({
    defaultIntervalSeconds: config.ocpp.defaultHeartbeatIntervalSeconds,
    toleranceSeconds: config.ocpp.heartbeatToleranceSeconds,
    criticalSeconds: config.ocpp.heartbeatCriticalSeconds,
  });
  const messageLog = new OcppMessageLogEngine();
  const connections = new ConnectionManager();
  const sessionDeps = { config, chargers, heartbeat, messageLog, connections };
  const wsServer = new OcppWebSocketServer({ config, connections, sessionDeps });
  const availabilityProbe = new AvailabilityProbeService({
    config,
    connections,
    messageLog,
    wsServer,
  });
  const fleet = new FleetRegistry();
  const reconnectionWatcher = new ReconnectionWatcher(
    { fleet, connections, wsServer, heartbeat },
    {
      enabled: config.ocpp.fleetEnabled,
      intervalMs: config.ocpp.reconnectionCheckIntervalMs,
      graceMultiplier: config.ocpp.reconnectionGraceMultiplier,
      toleranceSeconds: config.ocpp.reconnectionToleranceSeconds,
    },
  );
  const fleetStateSyncer = new FleetStateSyncer({ fleet, connections, chargers, heartbeat });

  const components: CsmsComponents = {
    config,
    chargers,
    heartbeat,
    messageLog,
    connections,
    wsServer,
    availabilityProbe,
    fleet,
    reconnectionWatcher,
    fleetStateSyncer,
  };
  linkHeartbeatToConnections(components);
  return components;
}

/**
 * Semeia a frota conhecida nos registros de runtime SEM marcar nenhum
 * carregador como conectado (plano de reconexao, passo 1/2). A identidade e
 * copiada para o ConnectionManager e o HeartbeatMonitor apenas como historico.
 */
function seedFleet(components: CsmsComponents, entries: readonly FleetSeed[]): void {
  if (entries.length === 0) return;
  components.fleet.seedAll(entries);
  for (const entry of entries) {
    components.connections.seedIdentity({
      chargePointId: entry.chargePointId,
      vendor: entry.vendor,
      model: entry.model,
      firmwareVersion: entry.firmwareVersion,
      negotiatedProtocol: entry.negotiatedProtocol,
      // `connectedAt` e semeado como "ultima conexao": permite que
      // `reconnectCount` continue acumulando a partir do valor persistido.
      connectedAt: entry.lastConnectedAt,
      lastBootAt: entry.lastBootAt,
      lastHeartbeatAt: entry.lastHeartbeatAt,
      lastPongAt: entry.lastPongAt,
      lastMessageAt: entry.lastMessageAt,
      disconnectReason: entry.lastDisconnectReason,
      reconnectCount: entry.reconnectCount,
    });
    components.heartbeat.seedIdentity(entry.chargePointId, {
      negotiatedIntervalSeconds: entry.negotiatedIntervalSeconds,
    });
    // Estado operacional: o ultimo StatusNotification conhecido e o ultimo boot.
    // `activeTransactions` NAO e semeado (pertence a sessao anterior).
    components.chargers.seedIdentity(entry.chargePointId, {
      vendor: entry.vendor,
      model: entry.model,
      serialNumber: entry.serialNumber,
      firmwareVersion: entry.firmwareVersion,
      bootAt: entry.bootAt,
      lastStatusAt: entry.lastStatusAt,
      connectors: entry.connectors,
    });
  }
}

export async function createApp(config: AppConfig = loadConfig(), deps: AppDependencies = {}): Promise<CsmsApp> {
  const components = buildComponents(config);
  const persistence = createPersistenceService(config, components, deps.persistence);
  if (persistence.messageSink) components.messageLog.addSink(persistence.messageSink);

  // Frota conhecida: carregar ANTES de escutar, para reconhecer a reconexao
  // (arquivo de frota + leitura read-only da tabela persistida). A leitura
  // nunca e fatal: sem credenciais/fonte a frota simplesmente nao e semeada.
  const fileFleet = loadFleetFromFile(config.ocpp.fleetFile);
  if (fileFleet.error) {
    // eslint-disable-next-line no-console
    console.warn(`[fleet] fonte local ignorada: ${fileFleet.error}`);
  }
  seedFleet(components, fileFleet.entries);
  seedFleet(components, await persistence.loadFleet());

  persistence.start();
  const monitorWs = new MonitorWebSocketServer(components);

  const fastify = Fastify({
    logger: {
      level: config.logLevel,
      // Timestamp legivel no horario de Brasilia. O padrao do pino seria epoch
      // em ms (ilegivel e sempre em UTC).
      timestamp: () => `,"time":"${toBrasiliaIso(Date.now())}"`,
    },
    // O corpo dos comandos OCPP e pequeno; limite defensivo.
    bodyLimit: 1024 * 1024,
  });
  // Logger da sonda de disponibilidade.
  // A sonda roda ciclicamente e gera varios eventos por carregador. Para nao
  // poluir o terminal com ruido repetitivo (trigger_rejected, cycle_completed,
  // trigger_sent, cycle_started), estes eventos sao logados em nivel 'debug'.
  // Apenas eventos que indicam mudanca de estado relevante ou possivel problema
  // sao logados em nivel 'info', com mensagem clara em portugues.
  const PROBE_EVENT_LABEL: Record<AvailabilityProbeEvent['event'], string> = {
    cycle_started: 'inicio do ciclo de sondagem',
    cycle_completed: 'fim do ciclo de sondagem',
    trigger_sent: 'pedido de status enviado',
    trigger_accepted: 'pedido de status aceito',
    trigger_rejected: 'pedido de status recusado (comportamento normal para carregadores NeoCharge/MyTeison)',
    unsupported: 'mensagem nao suportada pelo carregador',
    report_received: 'resposta recebida do carregador',
    trigger_timeout: 'timeout ao enviar pedido de status',
    report_timeout: 'timeout esperando resposta do carregador',
    disconnected: 'carregador desconectado durante a sondagem',
    not_registered: 'carregador ainda nao registrado (BootNotification pendente)',
    error: 'erro durante a sondagem',
  };
  // Eventos que geram muito ruido e sao normais na operacao: vao para 'debug'.
  const PROBE_DEBUG_EVENTS: ReadonlySet<AvailabilityProbeEvent['event']> = new Set([
    'cycle_started',
    'cycle_completed',
    'trigger_sent',
    'trigger_accepted',
    'trigger_rejected',
  ]);
  components.availabilityProbe.setLogger((event) => {
    const label = PROBE_EVENT_LABEL[event.event] ?? event.event;
    const requested = event.requestedMessage ? ` (${event.requestedMessage})` : '';
    const message = `[OCPP] sonda: ${event.chargePointId} - ${label}${requested}`;
    if (PROBE_DEBUG_EVENTS.has(event.event)) {
      fastify.log.debug({ probe: event }, message);
    } else {
      fastify.log.info({ probe: event }, message);
    }
  });
  // Toda tentativa de handshake OCPP passa a ser visivel, incluindo as que sao
  // rejeitadas (404 de path errado, subprotocolo invalido). Sem isto nao era
  // possivel distinguir "carregador nao chegou" de "chegou e foi rejeitado".
  components.wsServer.setLogger((event) => {
    fastify.log.info({ upgrade: event }, '[OCPP] tentativa de handshake');
  });
  // Ciclo de vida das conexoes visivel no terminal (alem do handshake): permite
  // observar conexao/desconexao de cada carregador e o total conectado no
  // momento. O timestamp de cada linha vem no horario de Brasilia.
  const CONNECTION_EVENT_LABEL: Record<ConnectionEvent['eventType'], string> = {
    attempt: 'tentativa de conexao',
    accepted: 'conectado',
    replaced: 'conexao substituida',
    closed: 'desconectado',
    rejected: 'conexao rejeitada',
    watchdog_timeout: 'timeout de inatividade',
    pong_timeout: 'timeout de keepalive',
    error: 'erro de transporte',
  };
  const connectedChargePointIds = (): string[] =>
    components.connections
      .snapshot()
      .filter((record) => record.transport === 'CONNECTED')
      .map((record) => record.chargePointId)
      .sort();
  const logConnectedChargers = (): void => {
    const ids = connectedChargePointIds();
    const total = components.fleet.size();
    fastify.log.info(
      { connected: ids, connectedCount: ids.length, fleetSize: total },
      `[CSMS] carregadores conectados (${ids.length}/${total}): ${ids.length ? ids.join(', ') : 'nenhum'}`,
    );
  };
  components.connections.subscribe((event) => {
    if (!event.chargePointId || event.eventType === 'attempt') return;
    // 'accepted' ocorre duas vezes por sessao (socket e BootNotification);
    // o segundo traz `metadata.kind === 'boot'`.
    const label = event.metadata.kind === 'boot' ? 'registrado (BootNotification)' : CONNECTION_EVENT_LABEL[event.eventType];
    fastify.log.info(
      {
        ocpp: {
          chargePointId: event.chargePointId,
          eventType: event.eventType,
          connectionId: event.connectionId,
          reason: event.disconnectReason,
          ...event.metadata,
        },
      },
      `[OCPP] carregador ${label}: ${event.chargePointId}`,
    );
    logConnectedChargers();
  });
  await fastify.register(cors, { origin: true });
  await registerRoutes(fastify, components);
  // Modulo de integracao (F2): consome apenas superficies publicas ja existentes
  // (`components.chargers`, `components.messageLog`, `components.wsServer`) sem
  // tocar o OCPP. As rotas ficam FORA de `/api/` (auth dedicada por
  // `x-integration-key`); se o modulo estiver desabilitado, `repository` e nulo e
  // as rotas respondem 503.
  const integration = createIntegrationModule({
    config,
    chargers: components.chargers,
    messages: components.messageLog,
    log: (msg, meta) => fastify.log.info({ integration: true, meta }, msg),
  });
  await registerIntegrationRoutes(fastify, {
    config,
    repository: integration.repository,
    wsServer: components.wsServer,
    outbox: integration.outbox,
    log: (msg, meta) => fastify.log.info({ integration: true, meta }, msg),
  });
  // Central de Monitoramento (UI estatica) servida na raiz. `..` funciona tanto
  // em dev (src/) quanto compilado (dist/), apontando para a pasta `public/`.
  await fastify.register(fastifyStatic, {
    root: path.resolve(__dirname, '..', 'public'),
    prefix: '/',
    index: ['index.html'],
  });

  // Roteamento dos upgrades WebSocket: /ocpp/* -> OCPP; /ws/monitor -> monitor.
  fastify.server.on('upgrade', (request: IncomingMessage, socket: Duplex, head: Buffer) => {
    const pathname = new URL(`http://x${request.url ?? '/'}`).pathname;
    if (pathname === '/ocpp' || pathname.startsWith('/ocpp/')) {
      components.wsServer.handleUpgrade(request, socket, head);
      return;
    }
    if (pathname === '/ws/monitor') {
      monitorWs.handleUpgrade(request, socket, head);
      return;
    }
    try {
      socket.write('HTTP/1.1 404 Not Found\r\nConnection: close\r\nContent-Length: 0\r\n\r\n');
    } catch {
      /* noop */
    }
    try {
      socket.destroy();
    } catch {
      /* noop */
    }
  });

  return {
    fastify,
    components,
    monitorWs,
    async start(): Promise<void> {
      // Observa o runtime antes de ligar a persistencia, para que o primeiro
      // snapshot ja grave a identidade/estado operacional atualizados.
      components.fleetStateSyncer.start();
      components.availabilityProbe.start();
      components.reconnectionWatcher.start();
      integration.start();
      await fastify.listen({ port: config.port, host: config.host });
      fastify.log.info(
        {
          host: config.host,
          port: config.port,
          ocppPath: '/ocpp/CentralSystemService/{chargePointId}',
          monitorWsPath: '/ws/monitor',
          ssePath: '/api/stream',
          persistenceEnabled: persistence.enabled,
          statusProbeEnabled: config.ocpp.statusProbeEnabled,
          statusProbeIntervalMs: config.ocpp.statusProbeIntervalMs,
          statusProbeTimeoutMs: config.ocpp.statusProbeTimeoutMs,
          fleetEnabled: config.ocpp.fleetEnabled,
          fleetSize: components.fleet.size(),
        },
        '[CSMS] servidor iniciado',
      );
      logConnectedChargers();
    },
    async stop(): Promise<void> {
      components.reconnectionWatcher.stop();
      components.fleetStateSyncer.stop();
      components.availabilityProbe.stop();
      // Integracao: para o push/produtor e drena a serie temporal (F4).
      await integration.stop();
      components.wsServer.closeAll();
      monitorWs.closeAll();
      // Drena a fila de persistencia antes de encerrar (spec §6).
      await persistence.stop();
      await fastify.close();
    },
  };
}
