import test from 'node:test';
import assert from 'node:assert/strict';
import type { AddressInfo } from 'node:net';
import { createApp, type CsmsApp } from '../src/app';
import { OcppSimulator } from '../scripts/ocpp-simulator';
import { makeTestConfig, waitFor, delay } from './helpers';

function probeConfig(options: { intervalMs?: number; timeoutMs?: number } = {}) {
  const base = makeTestConfig();
  return Object.freeze({
    ...base,
    ocpp: Object.freeze({
      ...base.ocpp,
      statusProbeEnabled: true,
      statusProbeIntervalMs: options.intervalMs ?? 60_000,
      statusProbeTimeoutMs: options.timeoutMs ?? 500,
    }),
  });
}

async function startApp(app: CsmsApp): Promise<{ baseUrl: string; httpUrl: string }> {
  await app.start();
  const { port } = app.fastify.server.address() as AddressInfo;
  return { baseUrl: `ws://127.0.0.1:${port}`, httpUrl: `http://127.0.0.1:${port}` };
}

function makeSimulator(baseUrl: string, chargePointId: string, options: ConstructorParameters<typeof OcppSimulator>[0] = {}) {
  return new OcppSimulator({
    baseUrl,
    chargePointId,
    respectServerInterval: true,
    autoReconnect: false,
    ...options,
  });
}

test('TriggerMessage solicita Heartbeat, StatusNotification e MeterValues e atualiza o estado/API', async (t) => {
  const app = await createApp(probeConfig());
  const { baseUrl, httpUrl } = await startApp(app);
  const sim = makeSimulator(baseUrl, 'CP-PROBE-OK');
  t.after(async () => {
    sim.stop();
    await app.stop();
  });

  await sim.connect();
  await waitFor(
    () =>
      ['Heartbeat', 'StatusNotification', 'MeterValues'].every(
        (action) => app.components.availabilityProbe.get('CP-PROBE-OK').actions[
          action as 'Heartbeat' | 'StatusNotification' | 'MeterValues'
        ].outcome === 'received',
      ),
    4000,
    10,
    'respostas às sondagens OCPP',
  );

  const logs = app.components.messageLog.recent({ chargePointId: 'CP-PROBE-OK', limit: 100 });
  const sentCalls = logs.filter((entry) => entry.direction === 'sent' && entry.messageType === 'CALL');
  assert.equal(sentCalls.filter((entry) => entry.action === 'TriggerMessage').length, 3);
  assert.equal(sentCalls.filter((entry) => entry.action === 'Heartbeat').length, 0);
  assert.ok(logs.some((entry) => entry.direction === 'received' && entry.action === 'Heartbeat'));
  assert.equal(app.components.chargers.get('CP-PROBE-OK')?.connectors.get(1)?.status, 'Available');
  assert.equal(
    app.components.chargers.get('CP-PROBE-OK')?.meterReadings.get('1|Power.Active.Import||')?.value,
    '7000',
  );
  assert.equal(
    app.components.chargers.get('CP-PROBE-OK')?.meterReadings.get('1|Temperature||')?.value,
    '28.5',
  );

  const response = await fetch(`${httpUrl}/api/chargers/CP-PROBE-OK`);
  const body = (await response.json()) as {
    online: boolean;
    charger: { meterReadings: Array<{ measurand: string; value: string }> };
    availabilityProbe: { actions: Record<string, { outcome: string }> };
  };
  assert.equal(body.online, true);
  assert.equal(body.charger.meterReadings.length, 2);
  assert.equal(body.availabilityProbe.actions.MeterValues?.outcome, 'received');
});

test('TriggerMessage NotImplemented é registrado e suprimido até reconectar', async (t) => {
  const app = await createApp(probeConfig({ intervalMs: 80, timeoutMs: 200 }));
  const { baseUrl } = await startApp(app);
  const sim = makeSimulator(baseUrl, 'CP-PROBE-UNSUPPORTED', { triggerMessageSupport: false });
  t.after(async () => {
    sim.stop();
    await app.stop();
  });

  await sim.connect();
  await waitFor(
    () =>
      app.components.availabilityProbe.get('CP-PROBE-UNSUPPORTED').actions.Heartbeat.outcome === 'unsupported' &&
      app.components.availabilityProbe.get('CP-PROBE-UNSUPPORTED').actions.StatusNotification.outcome === 'unsupported' &&
      app.components.availabilityProbe.get('CP-PROBE-UNSUPPORTED').actions.MeterValues.outcome === 'unsupported',
    3000,
    10,
    'classificação de TriggerMessage não suportado',
  );
  await delay(220);

  const triggers = app.components.messageLog
    .recent({ chargePointId: 'CP-PROBE-UNSUPPORTED', action: 'TriggerMessage', limit: 100 })
    .filter((entry) => entry.direction === 'sent');
  assert.equal(triggers.length, 3, 'cada ação não suportada só deve ser consultada uma vez por sessão');
  assert.equal(app.components.availabilityProbe.recent('CP-PROBE-UNSUPPORTED').filter((event) => event.event === 'unsupported').length, 3);
});

test('TriggerMessage aceito sem relatório termina em timeout sem retransmitir Heartbeat', async (t) => {
  const app = await createApp(probeConfig({ timeoutMs: 100 }));
  const { baseUrl } = await startApp(app);
  const sim = makeSimulator(baseUrl, 'CP-PROBE-TIMEOUT', {
    ignoreTriggeredMessages: ['Heartbeat'],
  });
  t.after(async () => {
    sim.stop();
    await app.stop();
  });

  await sim.connect();
  await waitFor(
    () => app.components.availabilityProbe.get('CP-PROBE-TIMEOUT').actions.Heartbeat.outcome === 'report_timeout',
    3000,
    10,
    'timeout do relatório Heartbeat solicitado',
  );
  const heartbeatCalls = app.components.messageLog
    .recent({ chargePointId: 'CP-PROBE-TIMEOUT', action: 'Heartbeat', limit: 100 })
    .filter((entry) => entry.direction === 'sent' && entry.messageType === 'CALL');
  assert.equal(heartbeatCalls.length, 0);
  assert.ok(
    app.components.availabilityProbe
      .recent('CP-PROBE-TIMEOUT')
      .some((event) => event.event === 'report_timeout' && event.requestedMessage === 'Heartbeat'),
  );
});

test('sondagem manual e reconexão iniciam novo ciclo com a nova sessão', async (t) => {
  const app = await createApp(probeConfig({ timeoutMs: 300 }));
  const { baseUrl, httpUrl } = await startApp(app);
  const sim = makeSimulator(baseUrl, 'CP-PROBE-RECONNECT', {
    autoReconnect: true,
    reconnectDelayMs: 30,
    maxReconnectDelayMs: 100,
  });
  t.after(async () => {
    sim.stop();
    await app.stop();
  });

  await sim.connect();
  await waitFor(
    () =>
      PROBE_ACTIONS.every(
        (action) => app.components.availabilityProbe.get('CP-PROBE-RECONNECT').actions[action].outcome === 'received',
      ),
    3000,
    10,
    'primeira sondagem completa',
  );
  await waitFor(
    () => !app.components.availabilityProbe.get('CP-PROBE-RECONNECT').running,
    1000,
    10,
    'fim do ciclo inicial',
  );

  const cyclesBeforeManual = app.components.availabilityProbe
    .recent('CP-PROBE-RECONNECT')
    .filter((event) => event.event === 'cycle_started').length;
  const manual = await fetch(`${httpUrl}/api/chargers/CP-PROBE-RECONNECT/probe`, { method: 'POST' });
  assert.equal(manual.status, 202);
  await waitFor(
    () =>
      app.components.availabilityProbe
        .recent('CP-PROBE-RECONNECT')
        .filter((event) => event.event === 'cycle_started').length > cyclesBeforeManual,
    1000,
    10,
    'sondagem manual agendada',
  );
  await waitFor(
    () => !app.components.availabilityProbe.get('CP-PROBE-RECONNECT').running,
    3000,
    10,
    'fim da sondagem manual',
  );

  const previousConnectionId = app.components.connections.get('CP-PROBE-RECONNECT')?.connectionId;
  app.components.wsServer.getSession('CP-PROBE-RECONNECT')?.close(1012, 'probe reconnect test');
  await waitFor(
    () =>
      app.components.connections.get('CP-PROBE-RECONNECT')?.registration === 'ACCEPTED' &&
      app.components.connections.get('CP-PROBE-RECONNECT')?.connectionId !== previousConnectionId,
    4000,
    10,
    'nova sessão após reconexão',
  );
  const nextConnectionId = app.components.connections.get('CP-PROBE-RECONNECT')?.connectionId;
  await waitFor(
    () =>
      app.components.availabilityProbe
        .recent('CP-PROBE-RECONNECT')
        .filter((event) => event.event === 'report_received' && event.connectionId === nextConnectionId).length >= 3,
    4000,
    10,
    'relatórios recebidos na sessão reconectada',
  );
  assert.equal(sim.reconnectCount, 1);
  assert.equal(app.components.wsServer.isConnected('CP-PROBE-RECONNECT'), true);
});

test('sondagem concorrente mantém respostas OCPP correlacionadas em múltiplas sessões', async (t) => {
  const app = await createApp(probeConfig({ timeoutMs: 500 }));
  const { baseUrl } = await startApp(app);
  const simulators = Array.from({ length: 12 }, (_, index) =>
    makeSimulator(baseUrl, `CP-PROBE-LOAD-${index}`, { triggerMessageSupport: true }),
  );
  t.after(async () => {
    for (const simulator of simulators) simulator.stop();
    await app.stop();
  });

  await Promise.all(simulators.map((simulator) => simulator.connect()));
  await waitFor(
    () =>
      simulators.every((simulator) => {
        const probe = app.components.availabilityProbe.get(
          `CP-PROBE-LOAD-${simulators.indexOf(simulator)}`,
        );
        return PROBE_ACTIONS.every((action) => probe.actions[action].outcome === 'received');
      }),
    8000,
    20,
    'ciclo completo das sondagens concorrentes',
  );

  await waitFor(
    () => app.components.messageLog.stats().pendingCorrelations === 0,
    2000,
    10,
    'correlações concluídas sob carga',
  );
  assert.equal(app.components.messageLog.stats().pendingCorrelations, 0);
  const heartbeatLatencies = simulators.flatMap((_, index) =>
    app.components.messageLog
      .recent({ chargePointId: `CP-PROBE-LOAD-${index}`, action: 'Heartbeat', limit: 20 })
      .filter((entry) => entry.direction === 'received' && entry.messageType === 'CALL')
      .map((entry) => entry.latencyMs)
      .filter((latency): latency is number => latency !== null),
  );
  assert.equal(heartbeatLatencies.length, simulators.length);
  assert.ok(Math.max(...heartbeatLatencies) < 500, 'latência Heartbeat deve permanecer abaixo de 500ms no teste local');
  for (let index = 0; index < simulators.length; index += 1) {
    const id = `CP-PROBE-LOAD-${index}`;
    assert.equal(app.components.wsServer.isConnected(id), true);
    const failed = app.components.messageLog.recent({ chargePointId: id, onlyErrors: true });
    assert.equal(failed.length, 0, `${id} não deve ter erros de protocolo`);
  }
});

const PROBE_ACTIONS = ['Heartbeat', 'StatusNotification', 'MeterValues'] as const;
