import test from 'node:test';
import assert from 'node:assert/strict';
import { ChargerStateService } from '../src/chargers/chargerState';
import {
  TelemetryProducer,
  type TelemetryMessage,
  type TelemetryOutbox,
  type TelemetrySessionLookup,
} from '../src/integration/telemetryProducer';
import type { IntegrationEvent, IntegrationMeterSample } from '../src/integration/types';

/**
 * Emissor FALSO de mensagens OCPP com a mesma superficie de
 * `OcppMessageLogEngine.subscribe(...)`. Cada `emit` representa um frame
 * registrado no log; `processedAt` nulo simula o frame PRE-handler e
 * preenchido, o POS-handler.
 */
function fakeMessages() {
  const listeners = new Set<(entry: TelemetryMessage) => void>();
  return {
    emit(entry: Partial<TelemetryMessage> & { chargePointId: string; action: string }): void {
      const full: TelemetryMessage = {
        chargePointId: entry.chargePointId,
        direction: entry.direction ?? 'received',
        action: entry.action,
        payload: entry.payload ?? {},
        observedAt: entry.observedAt ?? '2026-01-01T00:00:00.000Z',
        processedAt: entry.processedAt ?? null,
      };
      for (const listener of listeners) listener(full);
    },
    messages: {
      subscribe(listener: (entry: TelemetryMessage) => void): () => void {
        listeners.add(listener);
        return () => {
          listeners.delete(listener);
        };
      },
    },
  };
}

function setup(sessionMap = new Map<string, string>([['CP-1|1', 'sess-1']])) {
  const chargers = new ChargerStateService();
  const msgs = fakeMessages();
  const events: IntegrationEvent[] = [];
  const outbox: TelemetryOutbox = {
    async enqueue(event) {
      events.push(event);
    },
  };
  const sessions: TelemetrySessionLookup = {
    async getOpenSessionByConnector(chargePointId, connectorId) {
      return sessionMap.get(`${chargePointId}|${connectorId}`) ?? null;
    },
  };
  const producer = new TelemetryProducer({
    chargers,
    messages: msgs.messages,
    sessions,
    outbox,
    log: () => undefined,
  });
  return { chargers, msgs, events, producer };
}

function tick(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

/**
 * Fidelidade ao fluxo OCPP real: `startTransaction`/`stopTransaction` NAO
 * notificam o subscribe; a revelacao da transacao vem do frame POS-handler do
 * `messageLog`. Por isso o teste emite o `StartTransaction`/`StopTransaction`
 * ANTES e DEPOIS de mutar o `ChargerStateService`.
 */
async function openSession(ctx: ReturnType<typeof setup>): Promise<number> {
  const payload = { connectorId: 1, meterStart: 100, idTag: 'TAG-1', timestamp: '2026-01-01T00:00:01.000Z' };
  ctx.msgs.emit({ chargePointId: 'CP-1', action: 'StartTransaction', payload });
  const txId = ctx.chargers.startTransaction('CP-1', 1);
  ctx.msgs.emit({
    chargePointId: 'CP-1',
    action: 'StartTransaction',
    processedAt: '2026-01-01T00:00:01.000Z',
    payload,
  });
  await tick();
  return txId;
}

test('StartTransaction pos-handler emite session.started com a sessao correlacionada', async () => {
  const ctx = setup();
  ctx.producer.start();

  const txId = await openSession(ctx);

  const started = ctx.events.filter((event) => event.eventType === 'session.started');
  assert.equal(started.length, 1);
  assert.equal(started[0].integrationSessionId, 'sess-1');
  assert.equal(started[0].ocppTransactionId, txId);
  assert.equal(started[0].chargePointId, 'CP-1');
  assert.equal(started[0].connectorId, 1);
  assert.equal(started[0].occurredAt, '2026-01-01T00:00:01.000Z');
  assert.deepEqual(started[0].data, { meterStart: 100, idTag: 'TAG-1' });

  ctx.producer.stop();
});

test('sem sessao de negocio nao emite session.started', async () => {
  const ctx = setup(new Map());
  ctx.producer.start();

  await openSession(ctx);

  assert.equal(ctx.events.length, 0);

  ctx.producer.stop();
});

test('MeterValues emite exatamente um meter.sampled (apenas pre-handler)', async () => {
  const ctx = setup();
  ctx.producer.start();
  const txId = await openSession(ctx);

  const payload = {
    connectorId: 1,
    transactionId: txId,
    meterValue: [
      {
        timestamp: '2026-01-01T00:00:02.000Z',
        sampledValue: [
          { value: '1200', measurand: 'Energy.Active.Import.Register', unit: 'Wh' },
          { value: '230', measurand: 'Voltage', unit: 'V' },
        ],
      },
    ],
  };

  ctx.msgs.emit({ chargePointId: 'CP-1', action: 'MeterValues', payload });
  await tick();

  const meters = ctx.events.filter((event) => event.eventType === 'meter.sampled');
  assert.equal(meters.length, 1);
  assert.equal(meters[0].integrationSessionId, 'sess-1');
  assert.equal(meters[0].ocppTransactionId, txId);
  assert.equal(meters[0].occurredAt, '2026-01-01T00:00:02.000Z');
  const samples = meters[0].data.samples as IntegrationMeterSample[];
  assert.equal(samples.length, 2);
  assert.equal(samples[0].value, '1200');
  assert.equal(samples[0].unit, 'Wh');
  assert.equal(samples[1].measurand, 'Voltage');

  // Frame POS-handler do mesmo MeterValues NAO duplica o evento.
  ctx.msgs.emit({
    chargePointId: 'CP-1',
    action: 'MeterValues',
    processedAt: '2026-01-01T00:00:03.000Z',
    payload,
  });
  await tick();
  assert.equal(ctx.events.filter((event) => event.eventType === 'meter.sampled').length, 1);

  ctx.producer.stop();
});

test('MeterValues fora de uma sessao correlacionada nao emite', async () => {
  const ctx = setup();
  ctx.producer.start();

  ctx.msgs.emit({
    chargePointId: 'CP-1',
    action: 'MeterValues',
    payload: { connectorId: 1, meterValue: [{ sampledValue: [{ value: '1' }] }] },
  });
  await tick();

  assert.equal(ctx.events.length, 0);
  ctx.producer.stop();
});

test('StatusNotification emite connector.status (com sessao nula quando ocioso)', async () => {
  const ctx = setup();
  ctx.producer.start();

  ctx.msgs.emit({
    chargePointId: 'CP-1',
    action: 'StatusNotification',
    payload: { connectorId: 2, status: 'Available', errorCode: 'NoError' },
  });
  await tick();

  const statuses = ctx.events.filter((event) => event.eventType === 'connector.status');
  assert.equal(statuses.length, 1);
  assert.equal(statuses[0].connectorId, 2);
  assert.equal(statuses[0].integrationSessionId, null);
  assert.equal(statuses[0].ocppTransactionId, null);
  assert.deepEqual(statuses[0].data, { status: 'Available', errorCode: 'NoError', info: null });

  // Frame POS-handler nao duplica.
  ctx.msgs.emit({
    chargePointId: 'CP-1',
    action: 'StatusNotification',
    processedAt: '2026-01-01T00:00:05.000Z',
    payload: { connectorId: 2, status: 'Available' },
  });
  await tick();
  assert.equal(ctx.events.filter((event) => event.eventType === 'connector.status').length, 1);

  ctx.producer.stop();
});

test('StopTransaction emite session.stopped com a sessao anterior', async () => {
  const ctx = setup();
  ctx.producer.start();
  const txId = await openSession(ctx);

  ctx.chargers.stopTransaction('CP-1', txId, 1);
  ctx.msgs.emit({
    chargePointId: 'CP-1',
    action: 'StopTransaction',
    payload: { transactionId: txId, meterStop: 150, reason: 'Local' },
  });
  await tick();

  const stopped = ctx.events.filter((event) => event.eventType === 'session.stopped');
  assert.equal(stopped.length, 1);
  assert.equal(stopped[0].integrationSessionId, 'sess-1');
  assert.equal(stopped[0].ocppTransactionId, txId);
  assert.deepEqual(stopped[0].data, { meterStop: 150, reason: 'Local' });

  ctx.producer.stop();
});

test('start e stop sao idempotentes e param de produzir apos o stop', async () => {
  const ctx = setup();
  ctx.producer.start();
  ctx.producer.start(); // nao assina duas vezes

  const txId = await openSession(ctx);
  assert.equal(ctx.events.length, 1, 'uma unica assinatura');

  ctx.producer.stop();
  ctx.producer.stop(); // idempotente

  ctx.chargers.startTransaction('CP-1', 2);
  ctx.msgs.emit({
    chargePointId: 'CP-1',
    action: 'StartTransaction',
    processedAt: '2026-01-01T00:00:09.000Z',
    payload: { connectorId: 2, meterStart: 0 },
  });
  await tick();

  assert.equal(ctx.events.length, 1, 'apos o stop nenhum evento novo e produzido');
  assert.equal(txId, 1);
});
