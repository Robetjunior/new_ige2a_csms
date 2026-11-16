import test from 'node:test';
import assert from 'node:assert/strict';
import { ChargerStateService } from '../src/chargers/chargerState';
import { SessionCorrelator, type SessionCorrelatorMessage } from '../src/integration/sessionCorrelator';

interface LinkCall {
  cp: string;
  connectorId: number;
  txId: number;
}

/** Repositorio FALSO que registra as chamadas de correlacao. */
function fakeRepository() {
  const links: LinkCall[] = [];
  const stops: Array<{ cp: string; connectorId: number }> = [];
  return {
    links,
    stops,
    repository: {
      async linkOcppTransaction(cp: string, connectorId: number, txId: number): Promise<void> {
        links.push({ cp, connectorId, txId });
      },
      async markSessionStopped(cp: string, connectorId: number): Promise<void> {
        stops.push({ cp, connectorId });
      },
    },
  };
}

/**
 * Emissor FALSO de mensagens OCPP com a mesma superficie de
 * `OcppMessageLogEngine.subscribe(...)`. `emit(action, cp)` simula um frame
 * registrado no log (CALL/CALLRESULT) com a acao e o charge point id.
 */
function fakeMessages() {
  const listeners = new Set<(entry: SessionCorrelatorMessage) => void>();
  return {
    emit(action: string, chargePointId: string): void {
      for (const listener of listeners) listener({ chargePointId, action });
    },
    messages: {
      subscribe(listener: (entry: SessionCorrelatorMessage) => void): () => void {
        listeners.add(listener);
        return () => {
          listeners.delete(listener);
        };
      },
    },
  };
}

/** Aguarda o microtask/timer para o fire-and-forget do correlator. */
function tick(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

function setup() {
  const chargers = new ChargerStateService();
  const msgs = fakeMessages();
  const repo = fakeRepository();
  const correlator = new SessionCorrelator({
    chargers,
    messages: msgs.messages,
    repository: repo.repository,
    log: () => undefined,
  });
  return { chargers, msgs, repo, correlator };
}

/**
 * Fidelidade ao fluxo OCPP real: o `ChargerStateService` NAO notifica o
 * subscribe em `startTransaction`/`stopTransaction` (apenas seedIdentity,
 * registerBoot, setConnectorStatus e recordMeterReadings chamam `notify`). A
 * revelacao da transacao vem do `messageLog.subscribe`, que emite um frame por
 * mensagem: o CALL recebido chega ANTES do handler (estado ainda inalterado) e o
 * frame pos-handler (CALLRESULT/re-notificacao) chega DEPOIS (estado atualizado).
 * Os testes reproduzem exatamente essa ordem.
 */

test('StartTransaction no messageLog dispara linkOcppTransaction uma unica vez', async () => {
  const { chargers, msgs, repo, correlator } = setup();
  correlator.start();

  // Frame recebido antes do handler: estado ainda sem transacao (no-op).
  msgs.emit('StartTransaction', 'CP-1');
  const txId = chargers.startTransaction('CP-1', 1);
  // Frame pos-handler revela a transacao aberta.
  msgs.emit('StartTransaction', 'CP-1');
  await tick();

  assert.equal(repo.links.length, 1);
  assert.deepEqual(repo.links[0], { cp: 'CP-1', connectorId: 1, txId });
  assert.equal(correlator.linkedCount(), 1);

  correlator.stop();
});

test('StopTransaction no messageLog dispara markSessionStopped', async () => {
  const { chargers, msgs, repo, correlator } = setup();
  correlator.start();

  chargers.startTransaction('CP-1', 1);
  msgs.emit('StartTransaction', 'CP-1');
  await tick();
  assert.equal(repo.links.length, 1);

  // Frame recebido antes do handler: transacao ainda ativa (no-op).
  msgs.emit('StopTransaction', 'CP-1');
  chargers.stopTransaction('CP-1', null, 1);
  // Frame pos-handler revela a transacao encerrada.
  msgs.emit('StopTransaction', 'CP-1');
  await tick();

  assert.equal(repo.stops.length, 1);
  assert.deepEqual(repo.stops[0], { cp: 'CP-1', connectorId: 1 });
  assert.equal(correlator.linkedCount(), 0);

  correlator.stop();
});

test('acoes irrelevantes no messageLog NAO disparam correlacao', async () => {
  const { chargers, msgs, repo, correlator } = setup();
  correlator.start();

  chargers.startTransaction('CP-1', 1);
  msgs.emit('MeterValues', 'CP-1');
  msgs.emit('Heartbeat', 'CP-1');
  await tick();
  assert.equal(repo.links.length, 0, 'apenas StartTransaction/StopTransaction interessam');

  // O caminho `chargers.subscribe` continua complementar (setConnectorStatus notifica).
  chargers.setConnectorStatus('CP-1', 1, 'Charging', 'NoError', null);
  await tick();
  assert.equal(repo.links.length, 1);

  correlator.stop();
});

test('mensagens repetidas sem mudanca NAO repetem o link', async () => {
  const { chargers, msgs, repo, correlator } = setup();
  correlator.start();

  chargers.startTransaction('CP-1', 1);
  msgs.emit('StartTransaction', 'CP-1');
  await tick();
  assert.equal(repo.links.length, 1);

  msgs.emit('StartTransaction', 'CP-1');
  msgs.emit('StartTransaction', 'CP-1');
  await tick();

  assert.equal(repo.links.length, 1, 'link nao se repete sem mudanca de transacao');
  correlator.stop();
});

test('linkedCount reflete o estado (sobe ao abrir, zera ao fechar)', async () => {
  const { chargers, msgs, repo, correlator } = setup();
  correlator.start();

  assert.equal(correlator.linkedCount(), 0);

  chargers.startTransaction('CP-1', 1);
  msgs.emit('StartTransaction', 'CP-1');
  await tick();
  assert.equal(correlator.linkedCount(), 1);

  chargers.stopTransaction('CP-1', null, 1);
  msgs.emit('StopTransaction', 'CP-1');
  await tick();
  assert.equal(correlator.linkedCount(), 0);
  assert.equal(repo.stops.length, 1);

  correlator.stop();
});

test('start e stop sao idempotentes e stop para de correlacionar', async () => {
  const { chargers, msgs, repo, correlator } = setup();

  correlator.start();
  correlator.start(); // nao deve assinar duas vezes
  chargers.startTransaction('CP-1', 1);
  msgs.emit('StartTransaction', 'CP-1');
  await tick();
  assert.equal(repo.links.length, 1, 'uma unica assinatura');

  correlator.stop();
  correlator.stop(); // idempotente

  // Apos o stop nenhuma nova mudanca deve ser correlacionada.
  chargers.startTransaction('CP-1', 2);
  msgs.emit('StartTransaction', 'CP-1');
  await tick();
  assert.equal(repo.links.length, 1);
});
