import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { FleetRegistry, loadFleetFromFile } from '../src/fleet/fleetRegistry';

test('FleetRegistry: seed cria identidade com defaults e nao regride para null', () => {
  const registry = new FleetRegistry();
  const created = registry.seed({ chargePointId: 'CP-1', vendor: 'IGE2A', negotiatedIntervalSeconds: 60 });
  assert.equal(created.vendor, 'IGE2A');
  assert.equal(created.model, null);
  assert.equal(created.reconnectCount, 0);
  assert.equal(registry.has('CP-1'), true);
  assert.deepEqual(registry.ids(), ['CP-1']);
  assert.equal(registry.size(), 1);

  // Um segundo seed com campos ausentes/nulos NAO apaga o que ja existia.
  registry.seed({ chargePointId: 'CP-1', vendor: null, model: 'TEISON-7KW' });
  const entry = registry.get('CP-1');
  assert.equal(entry?.vendor, 'IGE2A', 'vendor conhecido preservado');
  assert.equal(entry?.model, 'TEISON-7KW', 'campo novo aplicado');
  assert.equal(entry?.negotiatedIntervalSeconds, 60, 'intervalo conhecido preservado');
});

test('FleetRegistry: seed sem chargePointId lanca', () => {
  const registry = new FleetRegistry();
  assert.throws(() => registry.seed({ chargePointId: '' }), /chargePointId/);
});

test('FleetRegistry: snapshot e uma copia defensiva', () => {
  const registry = new FleetRegistry();
  registry.seed({ chargePointId: 'CP-1', vendor: 'IGE2A' });
  const snapshot = registry.snapshot();
  snapshot[0].vendor = 'ALTERADO';
  assert.equal(registry.get('CP-1')?.vendor, 'IGE2A');
});

test('FleetRegistry: seedAll ignora entradas sem id e acumula ids', () => {
  const registry = new FleetRegistry();
  registry.seedAll([{ chargePointId: 'CP-1' }, { chargePointId: '' }, { chargePointId: 'CP-2' }]);
  assert.deepEqual(registry.ids().sort(), ['CP-1', 'CP-2']);
});

test('loadFleetFromFile: aceita lista de strings, lista de objetos e { chargers }', () => {
  const dir = mkdtempSync(path.join(tmpdir(), 'fleet-'));

  const stringsFile = path.join(dir, 'strings.json');
  writeFileSync(stringsFile, JSON.stringify(['CP-1', 'CP-2']));
  const strings = loadFleetFromFile(stringsFile);
  assert.equal(strings.error, null);
  assert.deepEqual(strings.entries.map((e) => e.chargePointId), ['CP-1', 'CP-2']);

  const objectsFile = path.join(dir, 'objects.json');
  writeFileSync(
    objectsFile,
    JSON.stringify([{ charge_point_id: 'CP-3', vendor: 'IGE2A', negotiated_interval_seconds: 30, reconnect_count: 4 }]),
  );
  const objects = loadFleetFromFile(objectsFile);
  assert.equal(objects.error, null);
  assert.equal(objects.entries[0].chargePointId, 'CP-3');
  assert.equal(objects.entries[0].vendor, 'IGE2A');
  assert.equal(objects.entries[0].negotiatedIntervalSeconds, 30);
  assert.equal(objects.entries[0].reconnectCount, 4);

  const wrappedFile = path.join(dir, 'wrapped.json');
  writeFileSync(wrappedFile, JSON.stringify({ chargers: [{ chargePointId: 'CP-9' }] }));
  const wrapped = loadFleetFromFile(wrappedFile);
  assert.equal(wrapped.error, null);
  assert.deepEqual(wrapped.entries.map((e) => e.chargePointId), ['CP-9']);
});

test('loadFleetFromFile: arquivo ausente/formato invalido nao lanca (error + lista vazia)', () => {
  const missing = loadFleetFromFile('/caminho/que/nao/existe-frota.json');
  assert.equal(missing.entries.length, 0);
  assert.ok(missing.error, 'deve reportar erro sem lancar');

  const dir = mkdtempSync(path.join(tmpdir(), 'fleet-'));
  const invalidFile = path.join(dir, 'invalid.json');
  writeFileSync(invalidFile, JSON.stringify({ inesperado: true }));
  const invalid = loadFleetFromFile(invalidFile);
  assert.equal(invalid.entries.length, 0);
  assert.ok(invalid.error);

  // Caminho vazio simplesmente desabilita a fonte local (sem erro).
  const empty = loadFleetFromFile('');
  assert.deepEqual(empty, { entries: [], error: null });
});
