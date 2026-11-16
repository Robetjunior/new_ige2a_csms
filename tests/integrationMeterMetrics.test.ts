import test from 'node:test';
import assert from 'node:assert/strict';
import {
  computeDurationSeconds,
  computeEnergyWh,
  computeLatestPowerW,
  computePowerW,
  computeSessionMetrics,
  computeSocPercent,
  parseSampleValue,
  samplesByMeasurand,
} from '../src/integration/meterMetrics';
import type { IntegrationMeterSample } from '../src/integration/types';

function sample(
  measurand: string,
  value: string,
  timestamp: string,
  extra: Partial<IntegrationMeterSample> = {},
): IntegrationMeterSample {
  return { measurand, value, unit: null, phase: null, location: null, context: null, timestamp, ...extra };
}

const T0 = '2026-01-01T00:00:00.000Z';
const T2 = '2026-01-01T00:00:02.000Z';
const T4 = '2026-01-01T00:00:04.000Z';

test('parseSampleValue converte numericos e rejeita nao-numericos', () => {
  assert.equal(parseSampleValue(sample('SoC', '55', T0)), 55);
  assert.equal(parseSampleValue(sample('SoC', '  12.5 ', T0)), 12.5);
  assert.equal(parseSampleValue(sample('x', 'abc', T0)), null);
  assert.equal(parseSampleValue(sample('x', '', T0)), null);
});

test('computePowerW: potencia = dE/dt (Wh -> W)', () => {
  const a = sample('Energy.Active.Import.Register', '1000', T0);
  const b = sample('Energy.Active.Import.Register', '1001', T2);
  // dE = 1 Wh em dt = 2 s => 1/2 * 3600 = 1800 W
  assert.equal(computePowerW(a, b), 1800);
});

test('computePowerW rejeita intervalo nao positivo e valor nao numerico', () => {
  const a = sample('Energy.Active.Import.Register', '1000', T0);
  assert.equal(computePowerW(a, a), null);
  assert.equal(computePowerW(a, sample('Energy.Active.Import.Register', 'abc', T2)), null);
});

test('computeEnergyWh: ultima leitura menos meterStart', () => {
  const samples = [
    sample('Energy.Active.Import.Register', '1000', T0),
    sample('Energy.Active.Import.Register', '1001', T2),
    sample('Power.Active.Import', '7000', T2),
  ];
  assert.equal(computeEnergyWh(samples), 1001);
  assert.equal(computeEnergyWh(samples, 950), 51);
  // meterStart acima da ultima leitura nao produz energia negativa.
  assert.equal(computeEnergyWh(samples, 2000), 0);
});

test('computeSocPercent devolve a leitura mais recente; null sem SoC', () => {
  assert.equal(computeSocPercent([sample('SoC', '40', T0), sample('SoC', '62', T2)]), 62);
  assert.equal(computeSocPercent([sample('Voltage', '230', T0)]), null);
});

test('computeLatestPowerW prioriza Power e cai para dE/dt', () => {
  // Prioridade: measurand de potencia.
  assert.equal(
    computeLatestPowerW([
      sample('Power.Active.Import', '6900', T0),
      sample('Power.Active.Import', '7200', T2),
      sample('Energy.Active.Import.Register', '1000', T0),
      sample('Energy.Active.Import.Register', '1001', T2),
    ]),
    7200,
  );
  // Fallback: delta de energia quando nao ha amostra de potencia.
  assert.equal(
    computeLatestPowerW([
      sample('Energy.Active.Import.Register', '1000', T0),
      sample('Energy.Active.Import.Register', '1001', T2),
    ]),
    1800,
  );
  // Uma unica amostra de energia nao permite derivar potencia.
  assert.equal(computeLatestPowerW([sample('Energy.Active.Import.Register', '1000', T0)]), null);
});

test('computeDurationSeconds calcula o tempo e ignora intervalos invalidos', () => {
  assert.equal(computeDurationSeconds(T0, T4), 4);
  assert.equal(computeDurationSeconds(T4, T0), null);
  assert.equal(computeDurationSeconds(T0, null), null);
  assert.equal(computeDurationSeconds(null, T4), null);
});

test('samplesByMeasurand filtra, descarta timestamp invalido e ordena', () => {
  const samples = [
    sample('SoC', '50', T4),
    sample('SoC', '20', T0),
    sample('SoC', '30', 'invalido'),
    sample('Voltage', '230', T0),
  ];
  const soc = samplesByMeasurand(samples, 'SoC');
  assert.deepEqual(
    soc.map((item) => item.value),
    ['20', '50'],
  );
});

test('computeSessionMetrics agrega energia, potencia, tempo e SoC', () => {
  const samples = [
    sample('Energy.Active.Import.Register', '1000', T0),
    sample('Energy.Active.Import.Register', '1001', T2),
    sample('SoC', '58', T2),
  ];
  const metrics = computeSessionMetrics({
    samples,
    meterStartWh: 950,
    startedAt: T0,
    endedAt: T4,
  });
  assert.equal(metrics.energyWh, 51);
  assert.equal(metrics.energyKwh, 0.051);
  assert.equal(metrics.powerW, 1800);
  assert.equal(metrics.socPercent, 58);
  assert.equal(metrics.durationSeconds, 4);
});
