import test from 'node:test';
import assert from 'node:assert/strict';
import { OcppMessageLogEngine } from '../src/logging/messageLogEngine';
import type { MessageLogSink } from '../src/logging/messageLogEngine';
import { parseOcppFrame } from '../src/ocpp/protocol';

function frameOf(raw: string) {
  const parsed = parseOcppFrame(raw);
  assert.equal(parsed.ok, true, `frame deveria parsear: ${raw}`);
  if (!parsed.ok) throw new Error('unreachable');
  return parsed.frame;
}

test('captura 100% das mensagens e preserva ordem cronologica', () => {
  const engine = new OcppMessageLogEngine(100, 'test');
  engine.logFrame({ chargePointId: 'CP1', direction: 'received', frame: frameOf('[2,"1","BootNotification",{}]'), raw: '[2,"1","BootNotification",{}]', nowMs: 1000 });
  engine.logFrame({ chargePointId: 'CP1', direction: 'sent', frame: frameOf('[3,"1",{"status":"Accepted"}]'), raw: '[3,"1",{"status":"Accepted"}]', nowMs: 1001 });
  engine.logFrame({ chargePointId: 'CP1', direction: 'received', frame: frameOf('[2,"2","Heartbeat",{}]'), raw: '[2,"2","Heartbeat",{}]', nowMs: 2000 });

  const recent = engine.recent({ chargePointId: 'CP1' });
  assert.equal(recent.length, 3);
  // recent() devolve do mais novo para o mais antigo
  assert.equal(recent[0]?.uniqueId, '2');
  assert.equal(recent[2]?.uniqueId, '1');
  assert.equal(engine.stats().received, 2);
  assert.equal(engine.stats().sent, 1);
});

test('correlaciona request/response e calcula latencia', () => {
  const engine = new OcppMessageLogEngine(100, 'test');
  engine.logFrame({ chargePointId: 'CP1', direction: 'received', frame: frameOf('[2,"9","StatusNotification",{}]'), raw: '[2,"9","StatusNotification",{}]', nowMs: 5000 });
  engine.logFrame({ chargePointId: 'CP1', direction: 'sent', frame: frameOf('[3,"9",{}]'), raw: '[3,"9",{}]', nowMs: 5045 });

  const request = engine.recent({ chargePointId: 'CP1' }).find((m) => m.direction === 'received');
  assert.ok(request);
  assert.equal(request?.latencyMs, 45);
  assert.equal(request?.status, 'succeeded');
  assert.equal(request?.respondedAt, new Date(5045).toISOString());
  assert.equal(engine.stats().pendingCorrelations, 0);
});

test('CALLERROR marca a requisicao como falha e propaga codigo de erro', () => {
  const engine = new OcppMessageLogEngine(100, 'test');
  engine.logFrame({ chargePointId: 'CP1', direction: 'received', frame: frameOf('[2,"7","Unknown",{}]'), raw: '[2,"7","Unknown",{}]', nowMs: 100 });
  engine.logFrame({ chargePointId: 'CP1', direction: 'sent', frame: frameOf('[4,"7","NotImplemented","nao suportada",{}]'), raw: '[4,"7","NotImplemented","nao suportada",{}]', nowMs: 130 });

  const request = engine.recent({ chargePointId: 'CP1' }).find((m) => m.uniqueId === '7' && m.direction === 'received');
  assert.ok(request);
  assert.equal(request?.status, 'failed');
  assert.equal(request?.errorCode, 'NotImplemented');
  assert.equal(request?.latencyMs, 30);
});

test('correlaciona tambem comandos originados pelo CSMS', () => {
  const engine = new OcppMessageLogEngine(100, 'test');
  engine.logFrame({ chargePointId: 'CP1', direction: 'sent', frame: frameOf('[2,"csms-1","Reset",{}]'), raw: '[2,"csms-1","Reset",{}]', nowMs: 10 });
  engine.logFrame({ chargePointId: 'CP1', direction: 'received', frame: frameOf('[3,"csms-1",{"status":"Accepted"}]'), raw: '[3,"csms-1",{"status":"Accepted"}]', nowMs: 60 });

  const outbound = engine.recent({ chargePointId: 'CP1' }).find((m) => m.direction === 'sent');
  assert.ok(outbound);
  assert.equal(outbound?.latencyMs, 50);
  assert.equal(outbound?.status, 'succeeded');
});

test('frame invalido nunca e descartado e carrega parseError', () => {
  const engine = new OcppMessageLogEngine(100, 'test');
  engine.logFrame({
    chargePointId: 'CP1',
    direction: 'received',
    raw: '<<lixo>>',
    parseError: 'json',
    status: 'failed',
  });

  const [record] = engine.recent({ chargePointId: 'CP1' });
  assert.ok(record);
  assert.equal(record?.messageType, 'UNKNOWN');
  assert.equal(record?.parseError, 'json');
  assert.equal(record?.status, 'failed');
  assert.equal(engine.stats().errors, 1);
});

test('filtros de consulta funcionam (action, direcao, apenasErros)', () => {
  const engine = new OcppMessageLogEngine(100, 'test');
  engine.logFrame({ chargePointId: 'CP1', direction: 'received', frame: frameOf('[2,"1","Heartbeat",{}]'), raw: '[2,"1","Heartbeat",{}]' });
  engine.logFrame({ chargePointId: 'CP2', direction: 'received', frame: frameOf('[2,"2","BootNotification",{}]'), raw: '[2,"2","BootNotification",{}]' });
  engine.logFrame({ chargePointId: 'CP1', direction: 'received', raw: 'xxx', parseError: 'json', status: 'failed' });

  assert.equal(engine.recent({ action: 'Heartbeat' }).length, 1);
  assert.equal(engine.recent({ chargePointId: 'CP2' }).length, 1);
  assert.equal(engine.recent({ onlyErrors: true }).length, 1);
  assert.equal(engine.recent({ direction: 'sent' }).length, 0);
});

test('sinks recebem registros e sao idempotentes por id (reemissao com latencia)', () => {
  const engine = new OcppMessageLogEngine(100, 'test');
  const writes: Array<{ id: string; latencyMs: number | null }> = [];
  const sink: MessageLogSink = {
    name: 'memory',
    write(entry) {
      writes.push({ id: entry.id, latencyMs: entry.latencyMs });
    },
  };
  engine.addSink(sink);

  engine.logFrame({ chargePointId: 'CP1', direction: 'received', frame: frameOf('[2,"1","Heartbeat",{}]'), raw: '[2,"1","Heartbeat",{}]', nowMs: 0 });
  engine.logFrame({ chargePointId: 'CP1', direction: 'sent', frame: frameOf('[3,"1",{}]'), raw: '[3,"1",{}]', nowMs: 25 });

  // O mesmo id do request foi reemitido com a latencia preenchida.
  const requestWrites = writes.filter((w) => w.id.endsWith('-1'));
  assert.equal(requestWrites.length, 2);
  assert.equal(requestWrites[0]?.latencyMs, null);
  assert.equal(requestWrites[1]?.latencyMs, 25);
});
