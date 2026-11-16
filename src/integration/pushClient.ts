/**
 * Cliente HTTP de telemetria NOVO -> ANTIGO (F3).
 *
 * Contrato (secao 4.2 e 4.4 do documento de arquitetura):
 *   - `POST {ANTIGO}/v1/integration/ocpp/events`;
 *   - corpo = JSON do envelope do evento;
 *   - `x-signature` = HMAC-SHA256 hex do CORPO exato, com segredo compartilhado;
 *   - `x-request-id` = `eventId` (rastreabilidade ponta a ponta).
 *
 * O cliente NUNCA lanca: falha de rede/timeout/HTTP vira um resultado
 * observavel, pois quem decide o reenvio e o dispatcher do outbox.
 */
import { createHmac } from 'node:crypto';
import type { IntegrationEvent } from './types';

/** Resultado do envio. `ok = false` sempre acompanha um motivo observavel. */
export interface PushResult {
  ok: boolean;
  status?: number;
  error?: string;
}

/** Resposta minima consumida do `fetch` (facilita o seam de teste). */
export interface PushFetchResponse {
  ok: boolean;
  status: number;
}

export type PushFetchLike = (
  url: string,
  init: { method: string; headers: Record<string, string>; body: string; signal?: AbortSignal },
) => Promise<PushFetchResponse>;

export interface IntegrationPushClientOptions {
  /** Base do ANTIGO, ex.: `http://127.0.0.1:3000`. */
  targetUrl: string;
  /** Segredo compartilhado do HMAC (distinto de `INTEGRATION_API_KEY`). */
  hmacSecret: string;
  timeoutMs: number;
  /** Caminho do endpoint de ingresso; default: `/v1/integration/ocpp/events`. */
  path?: string;
  /** Seam de teste (default: `globalThis.fetch`). */
  fetchImpl?: PushFetchLike;
}

const DEFAULT_PATH = '/v1/integration/ocpp/events';

/** Assinatura HMAC-SHA256 (hex) do corpo com o segredo compartilhado. */
export function signBody(secret: string, body: string): string {
  return createHmac('sha256', secret).update(body, 'utf8').digest('hex');
}

function errorMessage(error: unknown): string {
  if (error && typeof error === 'object' && 'message' in error) {
    const message = (error as { message?: unknown }).message;
    if (typeof message === 'string' && message.length > 0) return message;
  }
  return 'erro desconhecido';
}

export class IntegrationPushClient {
  private readonly url: string;
  private readonly secret: string;
  private readonly timeoutMs: number;
  private readonly fetchImpl: PushFetchLike;

  constructor(options: IntegrationPushClientOptions) {
    const base = options.targetUrl.replace(/\/+$/, '');
    this.url = `${base}${options.path ?? DEFAULT_PATH}`;
    this.secret = options.hmacSecret;
    this.timeoutMs = options.timeoutMs;
    this.fetchImpl = options.fetchImpl ?? ((globalThis.fetch as unknown) as PushFetchLike);
  }

  /** Assina e envia um evento. Nunca lanca. */
  async push(event: IntegrationEvent): Promise<PushResult> {
    const body = JSON.stringify(event);
    const signature = signBody(this.secret, body);
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeoutMs);
    try {
      const response = await this.fetchImpl(this.url, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'x-signature': signature,
          'x-request-id': event.eventId,
        },
        body,
        signal: controller.signal,
      });
      if (!response.ok) {
        return { ok: false, status: response.status, error: `http_${response.status}` };
      }
      return { ok: true, status: response.status };
    } catch (err) {
      return { ok: false, error: errorMessage(err) };
    } finally {
      clearTimeout(timer);
    }
  }
}
