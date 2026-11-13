/**
 * Ponto de entrada do CSMS Next Generation.
 * Inicia o servidor HTTP+WebSocket e trata desligamento gracioso.
 */
import { createApp } from './app';
import { loadConfig } from './config/env';

async function main(): Promise<void> {
  const config = loadConfig();
  const app = await createApp(config);

  let shuttingDown = false;
  const shutdown = async (signal: string): Promise<void> => {
    if (shuttingDown) return;
    shuttingDown = true;
    app.fastify.log.info({ signal }, '[CSMS] encerrando...');
    try {
      await app.stop();
      process.exit(0);
    } catch (err) {
      app.fastify.log.error({ err: String((err as Error)?.message ?? err) }, '[CSMS] falha ao encerrar');
      process.exit(1);
    }
  };

  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
  process.on('unhandledRejection', (reason) => {
    app.fastify.log.error({ reason: String(reason) }, '[CSMS] unhandledRejection');
  });
  process.on('uncaughtException', (err) => {
    app.fastify.log.error({ err: String(err?.message ?? err) }, '[CSMS] uncaughtException');
  });

  await app.start();
}

void main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error('[CSMS] falha fatal na inicializacao:', err);
  process.exit(1);
});
