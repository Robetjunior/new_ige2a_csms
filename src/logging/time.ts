/**
 * Fuso horario oficial do CSMS.
 *
 * Toda representacao "local" (timestamp do log do terminal e o campo
 * `observedAtLocal`) e fixada no horario de Brasilia, de forma deterministica e
 * independente do fuso do sistema operacional da VM. Os campos ISO terminados
 * em `Z` (`observedAt`, `processedAt`, `respondedAt`) permanecem em UTC.
 */
export const BRASILIA_TIME_ZONE = 'America/Sao_Paulo';

const isoFormatter = new Intl.DateTimeFormat('en-GB', {
  timeZone: BRASILIA_TIME_ZONE,
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  fractionalSecondDigits: 3,
  hourCycle: 'h23',
  timeZoneName: 'longOffset',
});

/** Instante em ISO 8601 local de Brasilia com offset explicito (ex.: 2026-11-16T09:30:00.123-03:00). */
export function toBrasiliaIso(ms: number = Date.now()): string {
  const parts: Record<string, string> = {};
  for (const { type, value } of isoFormatter.formatToParts(new Date(ms))) parts[type] = value;
  const rawOffset = parts.timeZoneName ?? 'GMT-03:00';
  const offset = rawOffset === 'GMT' ? '+00:00' : rawOffset.replace('GMT', '');
  return `${parts.year}-${parts.month}-${parts.day}T${parts.hour}:${parts.minute}:${parts.second}.${parts.fractionalSecond}${offset}`;
}
