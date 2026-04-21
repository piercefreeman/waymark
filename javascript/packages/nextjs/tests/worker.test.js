'use strict';

const { spawn } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const grpc = require('@grpc/grpc-js');

const { syncActionBootstrap } = require('../src/compiler/bootstrap.js');
const services = require('../src/generated/messages_grpc_pb.js');
const messages = require('../src/generated/messages_pb.js');
const { resolveBootstrapPath } = require('../src/runtime/bootstrap.js');
const {
  deserializeWorkflowResultPayload,
  serializeWorkflowArguments
} = require('../src/runtime/serialization.js');

function makeTempProject() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'waymark-nextjs-worker-'));
}

function packageRoot() {
  return path.resolve(__dirname, '..');
}

function workerBinPath() {
  return path.join(packageRoot(), 'src', 'bin', 'waymark-worker-node.js');
}

function linkPackageIntoProject(projectRoot) {
  const packageLinkPath = path.join(projectRoot, 'node_modules', '@waymark', 'nextjs');
  fs.mkdirSync(path.dirname(packageLinkPath), { recursive: true });
  fs.symlinkSync(packageRoot(), packageLinkPath, 'dir');
}

function startWorkerBridgeServer() {
  const received = {
    ackedDeliveryId: null,
    helloWorkerId: null
  };

  return new Promise((resolve, reject) => {
    const server = new grpc.Server();

    server.addService(services.WorkerBridgeService, {
      attach(call) {
        call.on('data', (envelope) => {
          switch (envelope.getKind()) {
            case messages.MessageKind.MESSAGE_KIND_WORKER_HELLO: {
              const hello = messages.WorkerHello.deserializeBinary(envelope.getPayload_asU8());
              received.helloWorkerId = hello.getWorkerId();
              call.write(buildDispatchEnvelope());
              return;
            }
            case messages.MessageKind.MESSAGE_KIND_ACK: {
              const ack = messages.Ack.deserializeBinary(envelope.getPayload_asU8());
              received.ackedDeliveryId = ack.getAckedDeliveryId();
              return;
            }
            case messages.MessageKind.MESSAGE_KIND_ACTION_RESULT: {
              const actionResult = messages.ActionResult.deserializeBinary(envelope.getPayload_asU8());
              received.actionResult = actionResult;
              call.end();
              return;
            }
            default:
              received.unexpectedKind = envelope.getKind();
          }
        });

        call.on('error', (error) => {
          reject(error);
        });
      }
    });

    server.bindAsync(
      '127.0.0.1:0',
      grpc.ServerCredentials.createInsecure(),
      (error, port) => {
        if (error) {
          reject(error);
          return;
        }

        resolve({
          address: `127.0.0.1:${port}`,
          received,
          shutdown: async () =>
            await new Promise((shutdownResolve) => {
              server.tryShutdown(() => shutdownResolve());
            })
        });
      }
    );
  });
}

function buildDispatchEnvelope() {
  const dispatch = new messages.ActionDispatch();
  dispatch.setActionId('action-1');
  dispatch.setInstanceId('instance-1');
  dispatch.setSequence(1);
  dispatch.setActionName('computeDouble');
  dispatch.setModuleName('lib/actions/math.ts');
  dispatch.setKwargs(serializeWorkflowArguments({ value: 5 }));
  dispatch.setDispatchToken('dispatch-1');

  const envelope = new messages.Envelope();
  envelope.setDeliveryId(42);
  envelope.setPartitionId(7);
  envelope.setKind(messages.MessageKind.MESSAGE_KIND_ACTION_DISPATCH);
  envelope.setPayload(dispatch.serializeBinary());
  return envelope;
}

function waitForChildExit(child) {
  return new Promise((resolve, reject) => {
    let stderr = '';
    let stdout = '';

    child.stdout?.on('data', (chunk) => {
      stdout += chunk.toString();
    });
    child.stderr?.on('data', (chunk) => {
      stderr += chunk.toString();
    });

    child.on('error', reject);
    child.on('exit', (code, signal) => {
      resolve({ code, signal, stderr, stdout });
    });
  });
}

describe('bootstrap auto-discovery', () => {
  test('walks upward from cwd and honors explicit env override', () => {
    const projectRoot = makeTempProject();
    const bootstrapPath = path.join(projectRoot, '.waymark', 'actions-bootstrap.mjs');
    const nestedDirectory = path.join(projectRoot, 'app', 'api');

    fs.mkdirSync(path.dirname(bootstrapPath), { recursive: true });
    fs.mkdirSync(nestedDirectory, { recursive: true });
    fs.writeFileSync(bootstrapPath, '// generated\n', 'utf8');

    expect(resolveBootstrapPath({ cwd: nestedDirectory })).toBe(bootstrapPath);

    const explicitBootstrapPath = path.join(projectRoot, 'custom-bootstrap.mjs');
    fs.writeFileSync(explicitBootstrapPath, '// explicit\n', 'utf8');

    expect(
      resolveBootstrapPath({
        cwd: nestedDirectory,
        env: { WAYMARK_JS_BOOTSTRAP: explicitBootstrapPath }
      })
    ).toBe(explicitBootstrapPath);
  });
});

describe('standalone worker runtime', () => {
  test('spawns a separate node worker that auto-discovers and loads the generated bootstrap', async () => {
    const projectRoot = makeTempProject();
    const nestedCwd = path.join(projectRoot, 'app', 'api');
    const bridge = await startWorkerBridgeServer();

    fs.mkdirSync(path.join(projectRoot, 'lib', 'helpers'), { recursive: true });
    fs.mkdirSync(path.join(projectRoot, 'lib', 'actions'), { recursive: true });
    fs.mkdirSync(nestedCwd, { recursive: true });
    fs.writeFileSync(
      path.join(projectRoot, 'tsconfig.json'),
      JSON.stringify(
        {
          compilerOptions: {
            baseUrl: '.',
            paths: {
              '@/*': ['./*']
            }
          }
        },
        null,
        2
      ),
      'utf8'
    );
    fs.writeFileSync(
      path.join(projectRoot, 'lib', 'helpers', 'double.ts'),
      [
        'export function double(value: number): number {',
        '  return value * 2;',
        '}',
        ''
      ].join('\n'),
      'utf8'
    );
    fs.writeFileSync(
      path.join(projectRoot, 'lib', 'actions', 'math.ts'),
      [
        'import { double } from "@/lib/helpers/double";',
        '',
        '// use action',
        'export async function computeDouble(value: number): Promise<number> {',
        '  return double(value);',
        '}',
        ''
      ].join('\n'),
      'utf8'
    );

    linkPackageIntoProject(projectRoot);
    syncActionBootstrap(projectRoot);

    const child = spawn(
      process.execPath,
      [workerBinPath(), '--bridge', bridge.address, '--worker-id', '7'],
      {
        cwd: nestedCwd,
        env: process.env
      }
    );

    try {
      const exit = await waitForChildExit(child);

      expect(exit.code).toBe(0);
      expect(exit.signal).toBeNull();
      expect(exit.stderr).not.toMatch(/MODULE_TYPELESS_PACKAGE_JSON/);
      expect(exit.stderr).not.toMatch(/Reparsing as ES module/);
      expect(bridge.received.helloWorkerId).toBe(7);
      expect(bridge.received.ackedDeliveryId).toBe(42);
      expect(bridge.received.unexpectedKind).toBeUndefined();
      expect(bridge.received.actionResult).toBeDefined();
      expect(bridge.received.actionResult.getDispatchToken()).toBe('dispatch-1');
      expect(bridge.received.actionResult.getSuccess()).toBe(true);
      expect(
        deserializeWorkflowResultPayload(bridge.received.actionResult.getPayload().serializeBinary())
      ).toBe(10);
    } finally {
      child.kill('SIGKILL');
      await bridge.shutdown();
    }
  });
});
