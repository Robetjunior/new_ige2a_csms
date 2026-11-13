import test from 'node:test';
import assert from 'node:assert/strict';
import type { AddressInfo } from 'node:net';
import { WebSocket } from 'ws';
import { createApp, type CsmsApp } from '../src/app';
import { OcppSimulator } from '../scripts/ocpp-simulator';
import { makeTestConfig, waitFor, delay } from './helpers';
import type { AppConfig } from '../src/config/env';

/** Config com a frota habilitada e janela de tolerancia curta (testes rapidos). */
function fleetConfig(): AppConfig {
  const base = makeTestConfig();
  return Object.freeze({
    ...base,
    ocpp: Object.freeze({
      ...base.ocpp,
      fleetEnabled: true,
      // O BootNotification devolve este intervalo; mante-lo baixo reduz a
      // janela de tolerancia da frota para ~1s (teste rapido e deterministico).
      defaultHeartbeatIntervalSeconds: 1,
      reconnectionGraceMultiplier: 1,
      reconnectionToleranceSeconds: 0,
      reconnectionCheckIntervalMs: 1000,
    }),
  });
}

async function startApp(app: CsmsApp): Promise<{ baseUrl: string; httpUrl: string }> {
  await app.start();
  const { port } = app.fastify.server.address() as AddressInfo;
  return { baseUrl: `ws://127.0.0.1:${port}`, httpUrl: `http://127.0.0.1:${port}` };
}

function openClient(url: string, protocol?: string | null): Promise<WebSocket> {
  return new Promise((resolve, reject) => {
    const ws = protocol ? new WebSocket(url, protocol) : new WebSocket(url);
    ws.once('open', () => resolve(ws));
    ws.once('error', reject);
  });
}

test('frota: reconhece queda e retorno do carregador e /api/fleet reflete o status', async (t) => {
  const app = await createApp(fleetConfig());
  app.components.fleet.seed({ chargePointId: 'CP-REC', negotiatedIntervalSeconds: 1 });
  app.components.heartbeat.seedIdentity('CP-REC', { negotiatedIntervalSeconds: 1 });
  const { baseUrl, httpUrl } = await startApp(app);

  const sim = new OcppSimulator({
    baseUrl,
    chargePointId: 'CP-REC',
    heartbeatIntervalMs: 250,
    respectServerInterval: false,
    autoReconnect: false,
  });
  t.after(async () => {
    sim.stop();
    await app.stop();
  });

  await sim.connect();
  await waitFor(() => app.components.connections.get('CP-REC')?.registration === 'ACCEPTED', 4000, 20, 'boot aceito');
  await waitFor(
    () => app.components.reconnectionWatcher.getStatus('CP-REC').status === 'CONNECTED',
    4000,
    20,
    'status CONNECTED',
  );

  const online = (await (await fetch(`${httpUrl}/api/fleet`)).json()) as {
    summary: { enabled: boolean; total: number; connected: number };
    members: Array<{ chargePointId: string; status: string }>;
  };
  assert.equal(online.summary.enabled, true);
  assert.equal(online.summary.total, 1);
  assert.equal(online.summary.connected, 1);
  assert.equal(online.members[0].status, 'CONNECTED');

  // Queda do socket: apos a janela de tolerancia (1s) o carregador fica AWAITING.
  app.components.wsServer.getSession('CP-REC')?.close(1012, 'test disconnect');
  await waitFor(
    () => app.components.reconnectionWatcher.getStatus('CP-REC').status === 'AWAITING_RECONNECTION',
    6000,
    50,
    'aguardando reconexao',
  );

  const offline = (await (await fetch(`${httpUrl}/api/fleet`)).json()) as {
    summary: { awaitingReconnection: number; connected: number };
    members: Array<{ chargePointId: string; status: string; awaitingSince: string | null }>;
  };
  assert.equal(offline.summary.awaitingReconnection, 1);
  assert.equal(offline.summary.connected, 0);
  assert.equal(offline.members[0].status, 'AWAITING_RECONNECTION');
  assert.ok(offline.members[0].awaitingSince, 'registra desde quando aguarda');

  // O proprio carregador reconecta (o CSMS nunca disca para o carregador).
  await sim.connect();
  await waitFor(
    () => app.components.reconnectionWatcher.getStatus('CP-REC').status === 'CONNECTED',
    4000,
    20,
    'reconectado',
  );
  assert.equal(app.components.messageLog.stats().pendingCorrelations, 0);
});

test('C10 flapping: ciclos repetidos nao duplicam registro nem deixam sessao orfa', async (t) => {
  const app = await createApp(fleetConfig());
  app.components.fleet.seed({ chargePointId: 'CP-FLAP', negotiatedIntervalSeconds: 1 });
  const { baseUrl } = await startApp(app);
  t.after(async () => {
    await app.stop();
  });

  for (let i = 0; i < 25; i += 1) {
    const ws = await openClient(`${baseUrl}/ocpp/CentralSystemService/CP-FLAP`, 'ocpp1.6');
    await delay(10);
    ws.close();
    await waitFor(() => !app.components.wsServer.isConnected('CP-FLAP'), 3000, 10, `ciclo ${i} encerrado`);
  }

  const records = app.components.connections.snapshot().filter((c) => c.chargePointId === 'CP-FLAP');
  assert.equal(records.length, 1, 'exatamente um registro por chargePointId');
  assert.equal(app.components.connections.get('CP-FLAP')?.transport, 'DISCONNECTED');
  assert.equal(
    app.components.wsServer.connectedChargePointIds().filter((id) => id === 'CP-FLAP').length,
    0,
    'nenhuma sessao orfa',
  );
  assert.equal(app.components.messageLog.stats().pendingCorrelations, 0);

  // A frota continua acompanhando o carregador que flapou (sem duplicar membro).
  const members = app.components.reconnectionWatcher.snapshot().filter((m) => m.chargePointId === 'CP-FLAP');
  assert.equal(members.length, 1);
});
