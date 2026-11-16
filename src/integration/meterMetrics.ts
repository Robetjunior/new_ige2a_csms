/**
 * Metricas derivadas das amostras de `MeterValues` (F4 - secao 6.1 do documento
 * de arquitetura).
 *
 * Funcoes PURAS: nao acessam banco nem relogio. Toda metrica abaixo e derivada
 * das amostras ja normalizadas pelo NOVO no formato `IntegrationMeterSample`.
 *
 *   | Metrica            | Origem OCPP 1.6                            | Calculo            |
 *   |--------------------|--------------------------------------------|--------------------|
 *   | Energia consumida  | `Energy.Active.Import.Register` (Wh)       | ultima - meterStart|
 *   | Potencia instantanea | `Power.Active.Import` (W) ou delta energia | dE/dt entre duas   |
 *   | SoC                | `SoC` (Percent)                            | valor direto       |
 *   | Tempo de sessao    | inicio/fim                                 | fim - inicio       |
 *
 * Convencoes de unidade:
 *   - `Energy.Active.Import.Register` e lido em Wh (unidade padrao do OCPP 1.6);
 *   - `Power.Active.Import`, quando derivado de energia, sai em W:
 *     `(dE_Wh / dt_s) * 3600`.
 */
import type { IntegrationMeterSample } from './types';

export const ENERGY_IMPORT_MEASURAND = 'Energy.Active.Import.Register';
export const POWER_IMPORT_MEASURAND = 'Power.Active.Import';
export const SOC_MEASURAND = 'SoC';

/** Metricas de uma sessao/recorte de amostras. `null` = nao disponivel. */
export interface SessionMetrics {
  /** Energia acumulada (Wh): ultima leitura menos `meterStartWh` (se informado). */
  readonly energyWh: number | null;
  /** Energia acumulada (kWh), arredondada a 3 casas. */
  readonly energyKwh: number | null;
  /** Potencia instantanea (W) na ultima amostra com base temporal valida. */
  readonly powerW: number | null;
  /** Estado de carga (%) mais recente; `null` quando o firmware nao envia `SoC`. */
  readonly socPercent: number | null;
  /** Duracao (s) entre `startedAt` e `endedAt`. */
  readonly durationSeconds: number | null;
}

/** Converte o `value` textual do OCPP em numero; `null` se nao for numerico. */
export function parseSampleValue(sample: IntegrationMeterSample): number | null {
  if (typeof sample.value !== 'string' || sample.value.trim().length === 0) return null;
  const parsed = Number(sample.value);
  return Number.isFinite(parsed) ? parsed : null;
}

/** Instante (ms) da amostra; `null` quando o timestamp e invalido. */
function sampleTimeMs(sample: IntegrationMeterSample): number | null {
  const parsed = Date.parse(sample.timestamp);
  return Number.isFinite(parsed) ? parsed : null;
}

/**
 * Amostras de um measurand ordenadas por instante crescente. Amostras com
 * timestamp invalido sao descartadas.
 */
export function samplesByMeasurand(
  samples: readonly IntegrationMeterSample[],
  measurand: string,
): IntegrationMeterSample[] {
  return samples
    .filter((sample) => sample.measurand === measurand && sampleTimeMs(sample) !== null)
    .slice()
    .sort((a, b) => (sampleTimeMs(a) ?? 0) - (sampleTimeMs(b) ?? 0));
}

/**
 * Potencia instantanea (W) entre duas leituras de energia (Wh):
 * `(dE_Wh / dt_s) * 3600`. `null` quando o intervalo nao e positivo ou alguma
 * leitura nao e numerica.
 */
export function computePowerW(
  previous: IntegrationMeterSample,
  current: IntegrationMeterSample,
): number | null {
  const prev = parseSampleValue(previous);
  const curr = parseSampleValue(current);
  const prevAt = sampleTimeMs(previous);
  const currAt = sampleTimeMs(current);
  if (prev === null || curr === null || prevAt === null || currAt === null) return null;
  const deltaSeconds = (currAt - prevAt) / 1000;
  if (deltaSeconds <= 0) return null;
  return ((curr - prev) / deltaSeconds) * 3600;
}

/**
 * Energia acumulada (Wh). Quando `meterStartWh` e informado, devolve a energia
 * consumida (`ultima - meterStart`); caso contrario, devolve a ultima leitura
 * do registrador. `null` sem amostras numericas.
 */
export function computeEnergyWh(
  samples: readonly IntegrationMeterSample[],
  meterStartWh?: number | null,
): number | null {
  const energy = samplesByMeasurand(samples, ENERGY_IMPORT_MEASURAND);
  let last: number | null = null;
  for (const sample of energy) {
    const value = parseSampleValue(sample);
    if (value !== null) last = value;
  }
  if (last === null) return null;
  if (typeof meterStartWh === 'number' && Number.isFinite(meterStartWh)) {
    return Math.max(0, last - meterStartWh);
  }
  return last;
}

/** SoC (%) mais recente; `null` quando o firmware nao envia `SoC`. */
export function computeSocPercent(samples: readonly IntegrationMeterSample[]): number | null {
  const soc = samplesByMeasurand(samples, SOC_MEASURAND);
  let last: number | null = null;
  for (const sample of soc) {
    const value = parseSampleValue(sample);
    if (value !== null) last = value;
  }
  return last;
}

/** Potencia instantanea (W) a partir da ultima amostra de potencia ou de energia. */
export function computeLatestPowerW(samples: readonly IntegrationMeterSample[]): number | null {
  // 1) preferencia: potencia reportada diretamente pelo firmware.
  const power = samplesByMeasurand(samples, POWER_IMPORT_MEASURAND);
  for (let i = power.length - 1; i >= 0; i -= 1) {
    const sample = power[i];
    if (!sample) continue;
    const value = parseSampleValue(sample);
    if (value !== null) return value;
  }
  // 2) fallback: delta de energia entre as duas ultimas leituras.
  const energy = samplesByMeasurand(samples, ENERGY_IMPORT_MEASURAND);
  if (energy.length < 2) return null;
  const previous = energy[energy.length - 2];
  const current = energy[energy.length - 1];
  if (!previous || !current) return null;
  return computePowerW(previous, current);
}

/** Duracao (s) entre dois instantes ISO; `null` se algum for invalido ou negativo. */
export function computeDurationSeconds(
  startedAt: string | null | undefined,
  endedAt: string | null | undefined,
): number | null {
  if (!startedAt || !endedAt) return null;
  const start = Date.parse(startedAt);
  const end = Date.parse(endedAt);
  if (!Number.isFinite(start) || !Number.isFinite(end) || end < start) return null;
  return (end - start) / 1000;
}

/** Metricas completas de um recorte de amostras. */
export function computeSessionMetrics(input: {
  samples: readonly IntegrationMeterSample[];
  meterStartWh?: number | null;
  startedAt?: string | null;
  endedAt?: string | null;
}): SessionMetrics {
  const energyWh = computeEnergyWh(input.samples, input.meterStartWh);
  return {
    energyWh,
    energyKwh: energyWh === null ? null : Math.round((energyWh / 1000) * 1000) / 1000,
    powerW: computeLatestPowerW(input.samples),
    socPercent: computeSocPercent(input.samples),
    durationSeconds: computeDurationSeconds(input.startedAt, input.endedAt),
  };
}
