# Diagnostico de conexao OCPP dos simuladores

Data: 2026-11-13

## Escopo

Investigacao dos endpoints:

- `ws://192.168.0.197:3100/ocpp/CentralSystemService/0312209102324230529`
- `ws://192.168.0.197:3100/ocpp/CentralSystemService/0312209102324230435`

Os testes descritos neste relatorio usam o simulador local do projeto. Eles nao
comprovam a conexao dos equipamentos fisicos.

## Causa identificada

As sessoes antigas visiveis no CSMS pertenciam aos simuladores `TEISON-0435` e
`TEISON-0529`, nao aos IDs completos acima. O campo `firmwareVersion` era
`1.0.0-sim` e a desconexao registrada era `1000:simulator stopped`, indicando
fechamento normal iniciado pelo proprio CLI durante a limpeza do teste.

O simulador nao possuia reconexao automatica depois de uma queda inesperada.
Seu fluxo CLI iniciava `BootNotification` e Heartbeats, mas nao emitia
`StatusNotification` automaticamente. A ausencia de mensagens dos IDs completos
antes do novo ensaio tambem foi confirmada pela API de logs do CSMS.

## Hipoteses e evidencias

| Hipotese | Conclusao | Evidencia |
|---|---|---|
| O CSMS esta preso ao loopback | Rejeitada para o runtime verificado | Listener `*:3100`; health e conexoes via IP LAN funcionaram. |
| Caminho OCPP ou subprotocolo impede o handshake | Rejeitada para os simuladores | Os dois IDs completos negociaram `ocpp1.6`, tiveram Boot aceito e mensagens registradas. |
| O simulador foi encerrado e nao retomou a conexao | Confirmada para as sessoes de teste anteriores | Logs antigos mostram `1000:simulator stopped`; o codigo anterior nao tinha rotina de reconexao. |
| Watchdog do CSMS encerrou as sessoes anteriores | Rejeitada para essas sessoes | Fechamento normal `1000` foi iniciado pelo CLI; watchdog e de 300 segundos. |
| Firmware/rede do carregador fisico causa a falha | Inconclusiva | Nenhuma sessao fisica nem tentativa de handshake dos equipamentos foi observada. |

## Correcoes

- O simulador agora tenta reconectar automaticamente apos fechamento inesperado,
  com backoff exponencial configuravel e limite superior.
- `stop()` continua sendo um encerramento intencional e cancela qualquer
  reconexao pendente.
- Apos receber `BootNotification.conf` com status `Accepted`, o simulador envia
  `StatusNotification` para o conector 1 com status `Available`.
- Cada reconexao envia novo `BootNotification`; heartbeats reiniciam e o estado
  da sessao/socket antigo nao e reutilizado.
- O teste de integracao agora provoca uma queda temporaria e verifica novo
  `connectionId`, re-registro, novo status, Heartbeat retomado e ausencia de
  erros ou correlacoes pendentes.

## Evidencias

Validacoes automatizadas:

- `npm run typecheck`: aprovado.
- `npm test`: 31 testes aprovados, incluindo reconexao automatica.
- Teste de integracao: apos fechar o socket do simulador com codigo 1012, uma
  nova sessao foi aceita e voltou a transmitir StatusNotification e Heartbeat.

Ensaio pela interface LAN com os dois IDs completos:

- CSMS: `192.168.0.197:3100`, escutando em todas as interfaces.
- IDs: `0312209102324230529` e `0312209102324230435`.
- Ambos negociaram `ocpp1.6`, ficaram `CONNECTED` e `ACCEPTED`.
- Para ambos foram registrados `BootNotification` e `StatusNotification` de
  entrada, com respostas `CALLRESULT` de saida e sem erro de protocolo.
- Ensaio continuo de 60 segundos, amostrado a cada 10 segundos (7 amostras):
  ambos permaneceram `CONNECTED` e `ACCEPTED` em todas as amostras.
- Cada simulador avancou de 4 para 10 Heartbeats no periodo amostrado; a leitura
  final posterior mostrou 11 Heartbeats. Todos os Heartbeats receberam
  `CALLRESULT`.
- Na leitura final havia 13 mensagens recebidas e 13 enviadas por carregador,
  cobrindo `BootNotification`, `StatusNotification` e `Heartbeat`, com zero
  registros `failed` e zero correlacoes pendentes.
- O teste de reconexao forcou o fechamento do socket do CSMS com codigo 1012.
  O simulador reconectou, obteve novo `connectionId`, repetiu Boot e status e
  retomou Heartbeat.
- Os simuladores foram encerrados intencionalmente depois do ensaio para nao
  ocupar as identidades configuradas nos carregadores fisicos.

## Limites e proximo passo para hardware

Nenhum evento de conexao dos carregadores fisicos foi observado durante esta
investigacao. O fato de um simulador usar o mesmo Charge Point ID nao autentica
nem comprova a identidade do equipamento. Para validar os carregadores reais,
encerre os simuladores, confirme que cada URL do firmware contem exatamente o
Charge Point ID enviado no `BootNotification`, e entao acompanhe a primeira
tentativa de conexao pelo terminal ao vivo e pelos eventos do CSMS.

O ensaio de rede local tambem nao prova conectividade a partir de um celular ou
da rede Wi-Fi dos carregadores. Essa verificacao requer que os dispositivos
estejam ligados e acessiveis na mesma LAN, sem isolamento entre clientes.
