import test from 'node:test';
import assert from 'node:assert/strict';
import { parseOcppFrame, serializeCall, serializeCallError, serializeCallResult } from '../src/ocpp/protocol';

test('parse: CALL valido', () => {
  const result = parseOcppFrame('[2,"abc-1","BootNotification",{"chargePointVendor":"IGE2A"}]');
  assert.equal(result.ok, true);
  if (!result.ok) return;
  assert.equal(result.frame.messageTypeId, 2);
  assert.equal(result.frame.uniqueId, 'abc-1');
  assert.equal(result.frame.action, 'BootNotification');
  assert.deepEqual(result.frame.payload, { chargePointVendor: 'IGE2A' });
});

test('parse: CALLRESULT valido', () => {
  const result = parseOcppFrame('[3,"abc-1",{"status":"Accepted","interval":60}]');
  assert.equal(result.ok, true);
  if (!result.ok) return;
  assert.equal(result.frame.messageTypeId, 3);
  assert.equal(result.frame.action, null);
  assert.deepEqual(result.frame.payload, { status: 'Accepted', interval: 60 });
});

test('parse: CALLERROR valido', () => {
  const result = parseOcppFrame('[4,"abc-1","NotImplemented","Acao nao suportada",{}]');
  assert.equal(result.ok, true);
  if (!result.ok) return;
  assert.equal(result.frame.messageTypeId, 4);
  assert.equal(result.frame.error?.code, 'NotImplemented');
  assert.equal(result.frame.error?.description, 'Acao nao suportada');
});

test('parse: JSON invalido nao descarta e informa motivo', () => {
  const result = parseOcppFrame('isto-nao-e-json');
  assert.equal(result.ok, false);
  if (result.ok) return;
  assert.equal(result.reason, 'json');
  assert.equal(result.raw, 'isto-nao-e-json');
});

test('parse: estrutura invalida', () => {
  const result = parseOcppFrame('{"a":1}');
  assert.equal(result.ok, false);
  if (result.ok) return;
  assert.equal(result.reason, 'structure');
});

test('parse: messageTypeId desconhecido', () => {
  const result = parseOcppFrame('[9,"abc-1","Foo",{}]');
  assert.equal(result.ok, false);
  if (result.ok) return;
  assert.equal(result.reason, 'unsupported_type');
  assert.equal(result.uniqueId, 'abc-1');
});

test('serialize: round-trip das tres mensagens', () => {
  const call = parseOcppFrame(serializeCall('id-1', 'Heartbeat', {}));
  const result = parseOcppFrame(serializeCallResult('id-2', { status: 'Accepted' }));
  const error = parseOcppFrame(serializeCallError('id-3', 'InternalError', 'falhou'));

  assert.equal(call.ok && call.frame.action, 'Heartbeat');
  assert.equal(result.ok && result.frame.messageTypeId, 3);
  assert.equal(error.ok && error.frame.error?.code, 'InternalError');
});
