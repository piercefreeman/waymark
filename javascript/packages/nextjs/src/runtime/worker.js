'use strict';

const process = require('node:process');
const { parseArgs } = require('node:util');

const grpc = require('@grpc/grpc-js');

const services = require('../generated/messages_grpc_pb.js');
const messages = require('../generated/messages_pb.js');
const { loadBootstrap } = require('./bootstrap.js');
const { executeActionDispatch } = require('./actions.js');

async function runWorker(options) {
  if (!options || !options.bridge) {
    throw new Error('runWorker requires a bridge address');
  }
  if (!Number.isInteger(options.workerId) || options.workerId < 0) {
    throw new Error(`workerId must be a non-negative integer, received ${options.workerId}`);
  }

  const bootstrapPath = await loadBootstrap({
    bootstrapPath: options.bootstrapPath,
    cwd: options.cwd,
    env: options.env
  });

  const ownClient = !options.client;
  const client = options.client || new services.WorkerBridgeClient(
    options.bridge,
    grpc.credentials.createInsecure()
  );
  const stream = client.attach();
  const writeEnvelope = createEnvelopeWriter(stream);
  const pendingTasks = new Set();

  return await new Promise((resolve, reject) => {
    let settled = false;

    const finish = (callback) => {
      if (settled) {
        return;
      }
      settled = true;
      if (ownClient && typeof client.close === 'function') {
        client.close();
      }
      callback();
    };

    stream.on('data', (envelope) => {
      const task = handleIncomingEnvelope(envelope, writeEnvelope);
      pendingTasks.add(task);
      task
        .catch((error) => finish(() => reject(error)))
        .finally(() => pendingTasks.delete(task));
    });

    stream.on('error', (error) => {
      finish(() => reject(new Error(`worker attach failed: ${error.message}`)));
    });

    stream.on('end', () => {
      Promise.allSettled(Array.from(pendingTasks))
        .then(() => finish(() => resolve({ bootstrapPath })))
        .catch((error) => finish(() => reject(error)));
    });

    writeEnvelope(createHelloEnvelope(options.workerId)).catch((error) => {
      finish(() => reject(error));
    });
  });
}

async function handleIncomingEnvelope(envelope, writeEnvelope) {
  switch (envelope.getKind()) {
    case messages.MessageKind.MESSAGE_KIND_ACTION_DISPATCH:
      await handleDispatchEnvelope(envelope, writeEnvelope);
      return;
    case messages.MessageKind.MESSAGE_KIND_HEARTBEAT:
      await writeEnvelope(createAckEnvelope(envelope));
      return;
    default:
      await writeEnvelope(createAckEnvelope(envelope));
  }
}

async function handleDispatchEnvelope(envelope, writeEnvelope) {
  await writeEnvelope(createAckEnvelope(envelope));

  const dispatch = messages.ActionDispatch.deserializeBinary(envelope.getPayload_asU8());
  const workerStartNs = Number(process.hrtime.bigint());
  const execution = await executeActionDispatch(dispatch);
  const workerEndNs = Number(process.hrtime.bigint());

  await writeEnvelope(
    createActionResultEnvelope(envelope, dispatch, execution, workerStartNs, workerEndNs)
  );
}

function createHelloEnvelope(workerId) {
  const hello = new messages.WorkerHello();
  hello.setWorkerId(workerId);

  const envelope = new messages.Envelope();
  envelope.setDeliveryId(0);
  envelope.setPartitionId(0);
  envelope.setKind(messages.MessageKind.MESSAGE_KIND_WORKER_HELLO);
  envelope.setPayload(hello.serializeBinary());
  return envelope;
}

function createAckEnvelope(envelope) {
  const ack = new messages.Ack();
  ack.setAckedDeliveryId(envelope.getDeliveryId());

  const ackEnvelope = new messages.Envelope();
  ackEnvelope.setDeliveryId(envelope.getDeliveryId());
  ackEnvelope.setPartitionId(envelope.getPartitionId());
  ackEnvelope.setKind(messages.MessageKind.MESSAGE_KIND_ACK);
  ackEnvelope.setPayload(ack.serializeBinary());
  return ackEnvelope;
}

function createActionResultEnvelope(envelope, dispatch, execution, workerStartNs, workerEndNs) {
  const actionResult = new messages.ActionResult();
  actionResult.setActionId(dispatch.getActionId());
  actionResult.setSuccess(execution.success);
  actionResult.setPayload(execution.payload);
  actionResult.setWorkerStartNs(workerStartNs);
  actionResult.setWorkerEndNs(workerEndNs);
  if (dispatch.hasDispatchToken()) {
    actionResult.setDispatchToken(dispatch.getDispatchToken());
  }
  if (execution.errorType) {
    actionResult.setErrorType(execution.errorType);
  }
  if (execution.errorMessage) {
    actionResult.setErrorMessage(execution.errorMessage);
  }

  const responseEnvelope = new messages.Envelope();
  responseEnvelope.setDeliveryId(envelope.getDeliveryId());
  responseEnvelope.setPartitionId(envelope.getPartitionId());
  responseEnvelope.setKind(messages.MessageKind.MESSAGE_KIND_ACTION_RESULT);
  responseEnvelope.setPayload(actionResult.serializeBinary());
  return responseEnvelope;
}

function createEnvelopeWriter(stream) {
  let chain = Promise.resolve();

  return (envelope) => {
    const writePromise = chain.then(
      () =>
        new Promise((resolve, reject) => {
          stream.write(envelope, (error) => {
            if (error) {
              reject(error);
              return;
            }
            resolve();
          });
        })
    );

    chain = writePromise.catch(() => {});
    return writePromise;
  };
}

function parseWorkerArgs(argv) {
  const { values } = parseArgs({
    args: argv,
    allowPositionals: false,
    options: {
      bootstrap: {
        type: 'string'
      },
      bridge: {
        type: 'string'
      },
      help: {
        short: 'h',
        type: 'boolean'
      },
      'worker-id': {
        type: 'string'
      }
    }
  });

  if (values.help) {
    return { help: true };
  }

  if (!values.bridge) {
    throw new Error('--bridge is required');
  }
  if (!values['worker-id']) {
    throw new Error('--worker-id is required');
  }

  const workerId = Number.parseInt(values['worker-id'], 10);
  if (!Number.isInteger(workerId) || workerId < 0) {
    throw new Error(`--worker-id must be a non-negative integer, received ${values['worker-id']}`);
  }

  return {
    bootstrapPath: values.bootstrap,
    bridge: values.bridge,
    workerId
  };
}

function workerUsage() {
  return [
    'Usage: waymark-worker-node --bridge <host:port> --worker-id <id> [--bootstrap <path>]',
    '',
    'If --bootstrap is omitted, the worker walks upward from the current working directory',
    `looking for ${require('./bootstrap.js').BOOTSTRAP_RELATIVE_PATH}.`
  ].join('\n');
}

async function main(argv = process.argv.slice(2)) {
  const parsed = parseWorkerArgs(argv);
  if (parsed.help) {
    process.stdout.write(`${workerUsage()}\n`);
    return;
  }

  await runWorker(parsed);
}

module.exports = {
  createAckEnvelope,
  createActionResultEnvelope,
  createEnvelopeWriter,
  createHelloEnvelope,
  handleDispatchEnvelope,
  handleIncomingEnvelope,
  main,
  parseWorkerArgs,
  runWorker,
  workerUsage
};
