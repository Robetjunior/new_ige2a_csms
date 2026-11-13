/**
 * Camada de protocolo OCPP-J: parsing defensivo e serializacao dos frames
 * CALL [2, id, action, payload], CALLRESULT [3, id, payload] e
 * CALLERROR [4, id, errorCode, errorDescription, errorDetails].
 *
 * Puro: nao depende de WebSocket, banco ou servidor HTTP.
 */
import type { OcppFrame, ParseResult } from '../domain/types';

function normalizeUniqueId(value: unknown): string | null {
  if (typeof value === 'string' && value.length > 0) return value.slice(0, 128);
  if (typeof value === 'number' && Number.isFinite(value)) return String(value).slice(0, 128);
  return null;
}

export function parseOcppFrame(raw: string): ParseResult {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return { ok: false, reason: 'json', raw, uniqueId: null };
  }

  if (!Array.isArray(parsed) || parsed.length < 3) {
    return { ok: false, reason: 'structure', raw, uniqueId: null };
  }

  const messageTypeId = parsed[0];
  const uniqueId = normalizeUniqueId(parsed[1]);

  if (typeof messageTypeId !== 'number') {
    return { ok: false, reason: 'structure', raw, uniqueId };
  }
  if (messageTypeId !== 2 && messageTypeId !== 3 && messageTypeId !== 4) {
    return { ok: false, reason: 'unsupported_type', raw, uniqueId };
  }
  if (!uniqueId) {
    return { ok: false, reason: 'structure', raw, uniqueId: null };
  }

  if (messageTypeId === 2) {
    const action = parsed[2];
    if (typeof action !== 'string' || action.length === 0) {
      return { ok: false, reason: 'structure', raw, uniqueId };
    }
    const frame: OcppFrame = {
      messageTypeId,
      uniqueId,
      action: action.slice(0, 128),
      payload: parsed[3] ?? {},
      raw,
    };
    return { ok: true, frame };
  }

  if (messageTypeId === 3) {
    const frame: OcppFrame = {
      messageTypeId,
      uniqueId,
      action: null,
      payload: parsed[2] ?? {},
      raw,
    };
    return { ok: true, frame };
  }

  // CALLERROR
  const code = typeof parsed[2] === 'string' ? parsed[2] : 'GenericError';
  const description = typeof parsed[3] === 'string' ? parsed[3] : '';
  const frame: OcppFrame = {
    messageTypeId,
    uniqueId,
    action: null,
    payload: parsed[4] ?? {},
    raw,
    error: { code, description, details: parsed[4] ?? {} },
  };
  return { ok: true, frame };
}

export function serializeCallResult(uniqueId: string, payload: unknown): string {
  return JSON.stringify([3, uniqueId, payload ?? {}]);
}

export function serializeCallError(
  uniqueId: string,
  code: string,
  description: string,
  details: unknown = {},
): string {
  return JSON.stringify([4, uniqueId, code, description, details ?? {}]);
}

export function serializeCall(uniqueId: string, action: string, payload: unknown): string {
  return JSON.stringify([2, uniqueId, action, payload ?? {}]);
}
