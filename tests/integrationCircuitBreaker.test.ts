import test from 'node:test';
import assert from 'node:assert/strict';
import { CircuitBreaker } from '../src/integration/circuitBreaker';

function makeBreaker(overrides: Partial<{ failureThreshold: number; openMs: number; halfOpenMax: number }> = {}) {
  let now = 0;
  const breaker = new CircuitBreaker({
    failureThreshold: overrides.failureThreshold ?? 3,
    openMs: overrides.openMs ?? 30_000,
    halfOpenMax: overrides.halfOpenMax,
    now: () => now,
  });
  return {
    breaker,
    advance(ms: number) {
      now += ms;
    },
  };
}

test('estado inicial fechado permite envio', () => {
  const { breaker } = makeBreaker();
  assert.equal(breaker.getState(), 'closed');
  assert.equal(breaker.allowAttempt(), true);
});

test('abre apos N falhas consecutivas e bloqueia o envio', () => {
  const { breaker } = makeBreaker({ failureThreshold: 3 });
  breaker.onFailure();
  breaker.onFailure();
  assert.equal(breaker.getState(), 'closed', 'ainda abaixo do limiar');
  assert.equal(breaker.allowAttempt(), true);
  breaker.onFailure();
  assert.equal(breaker.getState(), 'open');
  assert.equal(breaker.allowAttempt(), false, 'circuito aberto adia o envio');
});

test('sucesso zera as falhas consecutivas', () => {
  const { breaker } = makeBreaker({ failureThreshold: 3 });
  breaker.onFailure();
  breaker.onFailure();
  breaker.onSuccess();
  breaker.onFailure();
  assert.equal(breaker.getState(), 'closed', 'o sucesso reiniciou a contagem');
  assert.equal(breaker.getFailures(), 1);
});

test('apos o cooldown sonda em half_open e reabre se a sonda falhar', () => {
  const { breaker, advance } = makeBreaker({ failureThreshold: 2, openMs: 10_000 });
  breaker.onFailure();
  breaker.onFailure();
  assert.equal(breaker.getState(), 'open');
  assert.equal(breaker.allowAttempt(), false);

  advance(10_000);
  assert.equal(breaker.allowAttempt(), true, 'primeira sonda apos o cooldown');
  assert.equal(breaker.getState(), 'half_open');
  assert.equal(breaker.allowAttempt(), false, 'segunda sonda bloqueada pelo halfOpenMax');

  breaker.onFailure();
  assert.equal(breaker.getState(), 'open', 'falha na sonda reabre o circuito');
  assert.equal(breaker.allowAttempt(), false);
});

test('sonda bem-sucedida fecha o circuito', () => {
  const { breaker, advance } = makeBreaker({ failureThreshold: 2, openMs: 5_000 });
  breaker.onFailure();
  breaker.onFailure();
  advance(5_000);
  assert.equal(breaker.allowAttempt(), true);
  breaker.onSuccess();
  assert.equal(breaker.getState(), 'closed');
  assert.equal(breaker.allowAttempt(), true);
  assert.equal(breaker.getFailures(), 0);
});

test('halfOpenMax permite mais de uma sonda por janela', () => {
  const { breaker, advance } = makeBreaker({ failureThreshold: 1, openMs: 1_000, halfOpenMax: 2 });
  breaker.onFailure();
  assert.equal(breaker.getState(), 'open');
  advance(1_000);
  assert.equal(breaker.allowAttempt(), true);
  assert.equal(breaker.allowAttempt(), true);
  assert.equal(breaker.allowAttempt(), false, 'esgotou as duas sondas');
});
