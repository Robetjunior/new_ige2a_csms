/**
 * Sanitizacao defensiva de payloads OCPP para auditoria.
 *
 * Regras (spec §11): nunca registrar credenciais, tokens ou dados sensiveis sem
 * necessidade. Campos sensiveis sao redigidos; profundidade, tamanho de string,
 * itens de colecao e tamanho total do payload sao limitados.
 */

const MAX_PAYLOAD_CHARS = 4096;
const MAX_STRING_CHARS = 512;
const MAX_DEPTH = 6;
const MAX_COLLECTION_ITEMS = 50;

const SENSITIVE_FIELD = /password|token|authorization|credential|secret|api.?key|id.?tag|serial.?number|imsi|iccid|imei/i;
const SENSITIVE_TEXT =
  /\b(password|token|authorization|credential|secret|api[_ -]?key|id[_ -]?tag|serial[_ -]?number)\b(\s*[:=]\s*)("[^"]*"|'[^']*'|[^\s,;]+)/gi;

export function sanitizeText(value: string): string {
  return value
    .replace(/\bBearer\s+[A-Za-z0-9._~+/-]+=*/gi, 'Bearer [REDACTED]')
    .replace(SENSITIVE_TEXT, '$1$2[REDACTED]')
    .replace(/\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/gi, '[REDACTED:EMAIL]')
    .replace(/\bAKIA[0-9A-Z]{16}\b/g, '[REDACTED:CLOUD_KEY]');
}

function sanitizeValue(value: unknown, depth: number, seen: WeakSet<object>): unknown {
  if (value === null || typeof value === 'boolean' || typeof value === 'number') return value;
  if (typeof value === 'string') {
    const sanitized = sanitizeText(value);
    return sanitized.length > MAX_STRING_CHARS
      ? `${sanitized.slice(0, MAX_STRING_CHARS)}…[truncated ${sanitized.length - MAX_STRING_CHARS} chars]`
      : sanitized;
  }
  if (typeof value !== 'object') return `[${typeof value}]`;
  if (depth >= MAX_DEPTH) return '[MAX_DEPTH]';
  if (seen.has(value)) return '[CIRCULAR]';
  seen.add(value);

  if (Array.isArray(value)) {
    const items = value.slice(0, MAX_COLLECTION_ITEMS).map((item) => sanitizeValue(item, depth + 1, seen));
    if (value.length > items.length) items.push(`[${value.length - items.length} items omitted]`);
    return items;
  }

  const result: Record<string, unknown> = {};
  const keys = Object.keys(value as Record<string, unknown>);
  for (const key of keys.slice(0, MAX_COLLECTION_ITEMS)) {
    const field = (value as Record<string, unknown>)[key];
    if (SENSITIVE_FIELD.test(key)) {
      const type = field === null ? 'null' : Array.isArray(field) ? 'array' : typeof field;
      const size = typeof field === 'string' || Array.isArray(field) ? `; length=${field.length}` : '';
      result[key] = `[REDACTED:${type}${size}]`;
    } else {
      result[key] = sanitizeValue(field, depth + 1, seen);
    }
  }
  if (keys.length > MAX_COLLECTION_ITEMS) {
    result.__omittedFields = keys.length - MAX_COLLECTION_ITEMS;
  }
  return result;
}

/** Sanitiza e limita o payload. Retorna preview truncado se exceder o limite. */
export function sanitizePayload(value: unknown): unknown {
  try {
    const sanitized = sanitizeValue(value, 0, new WeakSet<object>());
    const serialized = JSON.stringify(sanitized);
    if (serialized.length <= MAX_PAYLOAD_CHARS) return sanitized;
    return {
      truncated: true,
      serializedLength: serialized.length,
      sanitizedPreview: serialized.slice(0, MAX_PAYLOAD_CHARS),
    };
  } catch {
    return '[UNAVAILABLE]';
  }
}

/** Representacao segura de um frame que nao pode ser parseado como JSON. */
export function boundedRawPayload(raw: string): unknown {
  const preview = raw.slice(0, MAX_PAYLOAD_CHARS);
  const sanitized = preview.replace(/[^\[\]{}:, \t\r\n]+/g, '[REDACTED]');
  return {
    parseFailed: true,
    messageBytes: Buffer.byteLength(raw),
    sanitizedPreview: sanitized.slice(0, MAX_PAYLOAD_CHARS),
    truncated: raw.length > MAX_PAYLOAD_CHARS || sanitized.length > MAX_PAYLOAD_CHARS,
  };
}
