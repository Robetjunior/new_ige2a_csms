import test from 'node:test';
import assert from 'node:assert/strict';
import { ChargerStateService } from '../src/chargers/chargerState';

test('seedIdentity: semeia o estado operacional do ultimo StatusNotification', () => {
  const service = new ChargerStateService();
  const bootAt = '2026-11-13T10:00:00.000Z';
  const lastStatusAt = '2026-11-13T10:05:00.000Z';
  service.seedIdentity('CP-1', {
    vendor: 'IGE2A',
    model: 'TEISON-7KW',
    serialNumber: 'SN-1',
    firmwareVersion: '1.2.3',
    bootAt,
    lastStatusAt,
    connectors: [
      { connectorId: 1, status: 'Available', errorCode: 'NoError', info: null, updatedAt: lastStatusAt },
      { connectorId: 2, status: 'Faulted', errorCode: 'OtherError', info: 'ground', updatedAt: lastStatusAt },
    ],
  });

  const rec = service.get('CP-1');
  assert.equal(rec?.vendor, 'IGE2A');
  assert.equal(rec?.serialNumber, 'SN-1');
  assert.equal(rec?.bootAt, bootAt);
  assert.equal(rec?.lastStatusAt, lastStatusAt);
  assert.equal(rec?.connectors.get(1)?.status, 'Available');
  assert.equal(rec?.connectors.get(2)?.errorCode, 'OtherError');
  assert.equal(rec?.connectors.get(2)?.info, 'ground');

  // Nao reidrata sessao: nada de transacao ativa nem leitura de medidor.
  assert.equal(rec?.activeTransactions.size, 0);
  assert.equal(rec?.meterReadings.size, 0);
});

test('seedIdentity: nao regride campo conhecido com null e ignora conector invalido', () => {
  const service = new ChargerStateService();
  service.registerBoot('CP-1', { vendor: 'IGE2A', model: 'TEISON-7KW' });
  service.setConnectorStatus('CP-1', 1, 'Charging', 'NoError', null);

  service.seedIdentity('CP-1', {
    vendor: null,
    model: 'OUTRO-MODELO',
    connectors: [{ connectorId: Number.NaN, status: 'Invalido' }],
  });

  const rec = service.get('CP-1');
  assert.equal(rec?.vendor, 'IGE2A', 'valor conhecido preservado');
  assert.equal(rec?.model, 'OUTRO-MODELO', 'campo novo aplicado');
  assert.equal(rec?.connectors.get(1)?.status, 'Charging', 'estado vivo preservado');
  assert.equal(rec?.connectors.size, 1, 'conector invalido ignorado');
});
