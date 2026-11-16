import test from 'node:test';
import assert from 'node:assert/strict';
import type { AddressInfo } from 'node:net';
import { createApp, type CsmsApp } from '../src/app';
import { OcppSimulator } from '../scripts/ocpp-simulator';
import { makeTestConfig, waitFor } from './helpers';

async function startApp(app: CsmsApp): Promise<string> {
  await app.start();
  const { port } = app.fastify.server.address() as AddressInfo;
  return `http://127.0.0.1:${port}`;
}

test('status publico lista apenas IDs permitidos e nao remove auth das rotas privadas', async (t) => {
  const base = makeTestConfig();
  const config = Object.freeze({
    ...base,
    apiKey: 'test-secret',
    ocpp: Object.freeze({
      ...base.ocpp,
      publicStatusChargePointIds: ['CP-PUBLIC'],
    }),
  });
  const app = await createApp(config);
  const httpUrl = await startApp(app);
  const publicCharger = new OcppSimulator({
    baseUrl: httpUrl.replace(/^http/, 'ws'),
    chargePointId: 'CP-PUBLIC',
    autoReconnect: false,
  });
  const privateCharger = new OcppSimulator({
    baseUrl: httpUrl.replace(/^http/, 'ws'),
    chargePointId: 'CP-NOT-PUBLIC',
    autoReconnect: false,
  });
  t.after(async () => {
    publicCharger.stop();
    privateCharger.stop();
    await app.stop();
  });

  await Promise.all([publicCharger.connect(), privateCharger.connect()]);
  await waitFor(
    () =>
      app.components.connections.get('CP-PUBLIC')?.registration === 'ACCEPTED' &&
      app.components.connections.get('CP-NOT-PUBLIC')?.registration === 'ACCEPTED',
    3000,
    20,
    'BootNotification aceito dos simuladores',
  );

  const publicResponse = await fetch(`${httpUrl}/api/public/status`);
  assert.equal(publicResponse.status, 200);
  assert.equal(publicResponse.headers.get('cache-control'), 'no-store');
  const body = (await publicResponse.json()) as {
    chargers: Array<Record<string, unknown> & { chargePointId: string; connected: boolean; registration: string }>;
  };
  assert.deepEqual(body.chargers.map((charger) => charger.chargePointId), ['CP-PUBLIC']);
  assert.equal(body.chargers[0]?.connected, true);
  assert.equal(body.chargers[0]?.registration, 'ACCEPTED');
  assert.equal('remoteIp' in (body.chargers[0] ?? {}), false);
  assert.equal('firmwareVersion' in (body.chargers[0] ?? {}), false);
  assert.equal('messages' in (body.chargers[0] ?? {}), false);

  const privateResponse = await fetch(`${httpUrl}/api/chargers`);
  assert.equal(privateResponse.status, 401);
  const queryKeyResponse = await fetch(`${httpUrl}/api/chargers?api_key=test-secret`);
  assert.equal(queryKeyResponse.status, 401);
});
