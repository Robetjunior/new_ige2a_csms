/**
 * Rotas HTTP do modulo de integracao (F2): expõe ao CSMS ANTIGO os comandos de
 * recarga `POST /integration/v1/sessions/start` e `/sessions/stop`.
 *
 * Caracteristicas:
 *   - autenticacao por API key DEDICADA (`x-integration-key`), distinta de
 *     `CSMS_API_KEY`. Comparacao em tempo constante;
 *   - NAO altera nenhuma superficie OCPP: apenas CONSOME `wsServer.isConnected`
 *     e `wsServer.sendCall`, ambas publicas;
 *   - o inbox de idempotencia (`IntegrationInboxRepository.claim/complete`)
 *     decide o dono do start; retries devolvem a MESMA resposta sem reenviar o
 *     comando ao carregador;
 *   - nenhum caminho de erro lanca para o Fastify: cada falha vira o status
 *     HTTP adequado.
 */
import { createHash, randomUUID, timingSafeEqual } from 'node:crypto';
import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import { z } from 'zod';
import type { AppConfig } from '../config/env';
import type { IntegrationInboxRepository } from './inboxRepository';
import type { IntegrationOutboxRepository } from './outboxRepository';
import type { IntegrationEvent } from './types';

/** Superficie minima e publica do servidor OCPP consumida pelas rotas. */
export interface IntegrationWsServer {
  isConnected(chargePointId: string): boolean;
  sendCall(chargePointId: string, action: string, payload: unknown, timeoutMs?: number): Promise<unknown>;
}

export interface IntegrationRoutesDeps {
  config: AppConfig;
  repository: IntegrationInboxRepository | null;
  wsServer: IntegrationWsServer;
  /** Outbox de telemetria. Usado para emitir `session.stopped` quando uma
   *  sessao em PENDING_ACK e cancelada via API (sem StartTransaction). */
  outbox?: IntegrationOutboxRepository | null;
  log?: (msg: string, meta?: unknown) => void;
}

/** Timeout do comando OCPP de start. Espelha o default de `wsServer.sendCall`. */
const COMMAND_TIMEOUT_MS = 30_000;

const startBodySchema = z.object({
  integrationSessionId: z.string().min(1).max(128),
  idempotencyKey: z.string().min(1).max(128),
  chargePointId: z.string().min(1).max(64),
  connectorId: z.number().int().min(0).max(64),
  // 64 e nao 32: o ANTIGO envia `idTag = userId` e o id de usuario do Supabase
  // e um UUID de 36 caracteres. Com 32 a rota devolvia 400 `invalid_body` antes
  // de gravar qualquer linha, bloqueando todo start real (secao 4.1).
  idTag: z.string().min(1).max(64),
});

const stopBodySchema = z.object({
  integrationSessionId: z.string().min(1).max(128),
});

/** Comparacao em tempo constante; tamanhos diferentes sao tratados como invalidos. */
function safeEqual(presented: string, expected: string): boolean {
  const a = Buffer.from(presented, 'utf8');
  const b = Buffer.from(expected, 'utf8');
  if (a.length !== b.length) return false;
  return timingSafeEqual(a, b);
}

function formatIssues(error: z.ZodError): string {
  return error.issues.map((issue) => `${issue.path.join('.') || '(corpo)'}: ${issue.message}`).join('; ');
}

/** Hash canonico do corpo do start, com ordem de campos fixa e estavel. */
function startRequestHash(input: {
  integrationSessionId: string;
  chargePointId: string;
  connectorId: number;
  idTag: string;
}): string {
  const canonical = JSON.stringify({
    integrationSessionId: input.integrationSessionId,
    chargePointId: input.chargePointId,
    connectorId: input.connectorId,
    idTag: input.idTag,
  });
  return createHash('sha256').update(canonical, 'utf8').digest('hex');
}

function errorMessage(error: unknown): string {
  if (error && typeof error === 'object' && 'message' in error) {
    const message = (error as { message?: unknown }).message;
    if (typeof message === 'string' && message.length > 0) return message;
  }
  return 'erro desconhecido';
}

export async function registerIntegrationRoutes(
  fastify: FastifyInstance,
  deps: IntegrationRoutesDeps,
): Promise<void> {
  const repository = deps.repository;
  const { wsServer } = deps;
  const integrationKey = deps.config.integrationKey;
  const log = deps.log ?? (() => undefined);

  async function ensureAuthorized(request: FastifyRequest, reply: FastifyReply): Promise<boolean> {
    if (integrationKey.length === 0) {
      await reply.code(503).send({ error: 'integration_disabled' });
      return false;
    }
    const raw = request.headers['x-integration-key'];
    const presented = typeof raw === 'string' ? raw.trim() : '';
    if (!safeEqual(presented, integrationKey)) {
      await reply.code(401).send({ error: 'unauthorized' });
      return false;
    }
    return true;
  }

  fastify.post('/integration/v1/sessions/start', async (request, reply) => {
    if (!(await ensureAuthorized(request, reply))) return;
    if (!repository) {
      await reply.code(503).send({ error: 'integration_disabled' });
      return;
    }

    const parsed = startBodySchema.safeParse(request.body ?? {});
    if (!parsed.success) {
      await reply.code(400).send({ error: 'invalid_body', message: formatIssues(parsed.error) });
      return;
    }
    const body = parsed.data;

    const claim = await repository.claim({
      idempotencyKey: body.idempotencyKey,
      chargePointId: body.chargePointId,
      connectorId: body.connectorId,
      requestHash: startRequestHash(body),
    });

    switch (claim.outcome) {
      case 'error':
        // Erro de infraestrutura: detalhe so no log interno (nao vaza ao cliente).
        log('[integration] falha no inbox do start', {
          idempotencyKey: body.idempotencyKey,
          message: claim.message,
        });
        await reply.code(500).send({ error: 'inbox_error' });
        return;
      case 'hash_mismatch':
        await reply.code(409).send({ error: 'hash_mismatch' });
        return;
      case 'in_progress':
        await reply.code(409).send({ error: 'duplicate_in_progress' });
        return;
      case 'duplicate':
        // Retry de um start ja concluido: devolve a MESMA resposta, sem reenviar.
        await reply.code(200).send(claim.response);
        return;
      case 'owner':
        break;
    }

    if (!wsServer.isConnected(body.chargePointId)) {
      await repository.complete(body.idempotencyKey, { ok: false, reason: 'not_connected' }, 'FAILED');
      await reply.code(409).send({ error: 'not_connected', chargePointId: body.chargePointId });
      return;
    }

    await repository.upsertSession({
      integrationSessionId: body.integrationSessionId,
      idempotencyKey: body.idempotencyKey,
      chargePointId: body.chargePointId,
      connectorId: body.connectorId,
      state: 'PENDING_ACK',
      idTag: body.idTag,
    });

    try {
      await wsServer.sendCall(
        body.chargePointId,
        'RemoteStartTransaction',
        { connectorId: body.connectorId, idTag: body.idTag },
        COMMAND_TIMEOUT_MS,
      );
      const response = { ok: true, integrationSessionId: body.integrationSessionId, status: 'accepted' };
      await repository.complete(body.idempotencyKey, response, 'COMPLETED');
      await reply.code(202).send(response);
    } catch (err) {
      const message = errorMessage(err);
      log('[integration] falha ao enviar RemoteStartTransaction', {
        chargePointId: body.chargePointId,
        integrationSessionId: body.integrationSessionId,
        message,
      });
      // Resposta persistida sem o detalhe de infraestrutura (retries devolvem-na).
      const response = { ok: false, integrationSessionId: body.integrationSessionId, reason: 'command_failed' };
      await repository.complete(body.idempotencyKey, response, 'FAILED');
      await reply.code(502).send({ error: 'command_failed' });
    }
  });

  fastify.post('/integration/v1/sessions/stop', async (request, reply) => {
    if (!(await ensureAuthorized(request, reply))) return;
    if (!repository) {
      await reply.code(503).send({ error: 'integration_disabled' });
      return;
    }

    const parsed = stopBodySchema.safeParse(request.body ?? {});
    if (!parsed.success) {
      await reply.code(400).send({ error: 'invalid_body', message: formatIssues(parsed.error) });
      return;
    }
    const { integrationSessionId } = parsed.data;

    const session = await repository.getSession(integrationSessionId);
    if (!session) {
      await reply.code(404).send({ error: 'not_found', integrationSessionId });
      return;
    }
    if (session.ocpp_transaction_id == null) {
      // A sessao ainda nao recebeu o StartTransaction do carregador
      // (estado PENDING_ACK). O usuario esta cancelando antes do inicio
      // efetivo da carga. Marcamos como STOPPED sem enviar
      // RemoteStopTransaction (que exigiria um transactionId) e emitimos
      // session.stopped para o ANTIGO finalizar a sessao local (sem cobranca,
      // pois nenhuma energia foi entregue).
      const stoppedAt = new Date().toISOString();
      await repository.upsertSession({
        integrationSessionId,
        idempotencyKey: session.idempotency_key,
        chargePointId: session.charge_point_id,
        connectorId: session.connector_id,
        ocppTransactionId: null,
        state: 'STOPPED',
        idTag: session.id_tag,
        startedAt: session.started_at,
        stoppedAt,
      });
      if (deps.outbox) {
        const event: IntegrationEvent = {
          eventId: randomUUID(),
          eventType: 'session.stopped',
          integrationSessionId,
          ocppTransactionId: null,
          chargePointId: session.charge_point_id,
          connectorId: session.connector_id,
          occurredAt: stoppedAt,
          data: { meterStop: null, reason: 'cancelled_before_start' },
        };
        try {
          await deps.outbox.enqueue(event);
        } catch (err) {
          log?.('[integration] falha ao enfileirar session.stopped (cancelamento)', {
            integrationSessionId,
            message: err instanceof Error ? err.message : String(err),
          });
        }
      }
      await reply.code(202).send({ ok: true, integrationSessionId, status: 'cancelled' });
      return;
    }

    await repository.upsertSession({
      integrationSessionId,
      idempotencyKey: session.idempotency_key,
      chargePointId: session.charge_point_id,
      connectorId: session.connector_id,
      ocppTransactionId: session.ocpp_transaction_id,
      state: 'STOPPING',
      idTag: session.id_tag,
      startedAt: session.started_at,
      stoppedAt: session.stopped_at,
    });

    try {
      await wsServer.sendCall(session.charge_point_id, 'RemoteStopTransaction', {
        transactionId: session.ocpp_transaction_id,
      });
      await reply.code(202).send({ ok: true, integrationSessionId, status: 'stopping' });
    } catch (err) {
      const message = errorMessage(err);
      log('[integration] falha ao enviar RemoteStopTransaction', {
        chargePointId: session.charge_point_id,
        integrationSessionId,
        message,
      });
      await reply.code(502).send({ error: 'command_failed' });
    }
  });
}
