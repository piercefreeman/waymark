'use strict';

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const messages = require('../src/generated/messages_pb.js');
const { bootstrapPathForProject } = require('../src/compiler/bootstrap.js');
const {
  buildWorkflowRegistration,
  bridgeTarget,
  isBridgeConnectionError,
  skipWaitForInstance
} = require('../src/runtime/bridge.js');
const { deserializeWorkflowResultPayload } = require('../src/runtime/serialization.js');
const { withWaymark } = require('../src/with-waymark.js');

function makeTempProject() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'waymark-nextjs-bridge-'));
}

describe('bridge runtime helpers', () => {
  test('buildWorkflowRegistration serializes IR and initial context', () => {
    const registration = buildWorkflowRegistration(
      {
        workflowName: 'demo',
        workflowVersion: 'hash-123',
        irHash: 'hash-123',
        programBase64: Buffer.from('ir-bytes').toString('base64'),
        inputNames: ['userId', 'count'],
        concurrent: false
      },
      ['user-1', 3]
    );

    expect(registration.getWorkflowName()).toBe('demo');
    expect(Buffer.from(registration.getIr_asU8()).toString('utf8')).toBe('ir-bytes');
    expect(registration.getWorkflowVersion()).toBe('hash-123');
    expect(registration.getIrHash()).toBe('hash-123');

    const initialContext = registration.getInitialContext();
    const entries = Object.fromEntries(
      initialContext.getArgumentsList().map((entry) => [
        entry.getKey(),
        entry.getValue().getPrimitive()
      ])
    );
    expect(entries.userId.getStringValue()).toBe('user-1');
    expect(entries.count.getIntValue()).toBe(3);
  });

  test('deserializeWorkflowResultPayload returns the result value', () => {
    const payload = new messages.WorkflowArguments();
    const entry = new messages.WorkflowArgument();
    entry.setKey('result');

    const value = new messages.WorkflowArgumentValue();
    const primitive = new messages.PrimitiveWorkflowArgument();
    primitive.setStringValue('done');
    value.setPrimitive(primitive);
    entry.setValue(value);
    payload.addArguments(entry);

    expect(deserializeWorkflowResultPayload(payload.serializeBinary())).toBe('done');
  });

  test('bridgeTarget prefers explicit address and skipWaitForInstance follows python semantics', () => {
    const originalAddr = process.env.WAYMARK_BRIDGE_GRPC_ADDR;
    const originalSkipWait = process.env.WAYMARK_SKIP_WAIT_FOR_INSTANCE;

    process.env.WAYMARK_BRIDGE_GRPC_ADDR = 'bridge.internal:9999';
    process.env.WAYMARK_SKIP_WAIT_FOR_INSTANCE = 'true';

    expect(bridgeTarget()).toBe('bridge.internal:9999');
    expect(skipWaitForInstance()).toBe(true);

    process.env.WAYMARK_SKIP_WAIT_FOR_INSTANCE = 'false';
    expect(skipWaitForInstance()).toBe(false);

    if (originalAddr === undefined) {
      delete process.env.WAYMARK_BRIDGE_GRPC_ADDR;
    } else {
      process.env.WAYMARK_BRIDGE_GRPC_ADDR = originalAddr;
    }
    if (originalSkipWait === undefined) {
      delete process.env.WAYMARK_SKIP_WAIT_FOR_INSTANCE;
    } else {
      process.env.WAYMARK_SKIP_WAIT_FOR_INSTANCE = originalSkipWait;
    }
  });

  test('detects retryable bridge connection errors', () => {
    expect(
      isBridgeConnectionError(
        new Error('executeWorkflow failed: 14 UNAVAILABLE: No connection established.')
      )
    ).toBe(true);
    expect(isBridgeConnectionError(new Error('connect ECONNREFUSED 127.0.0.1:24117'))).toBe(true);
    expect(isBridgeConnectionError(new Error('bridge running in memory mode'))).toBe(false);
  });
});

describe('withWaymark', () => {
  test('adds the server-side loader rule, pre-generates the bootstrap, and preserves user webpack hooks', () => {
    const projectRoot = makeTempProject();
    fs.mkdirSync(path.join(projectRoot, 'lib', 'actions'), { recursive: true });
    fs.writeFileSync(
      path.join(projectRoot, 'lib', 'actions', 'math.ts'),
      ['// use action', 'export async function double(value) {', '  return value * 2;', '}', ''].join('\n')
    );

    const wrapped = withWaymark(
      {
        webpack(config) {
          config.resolve = config.resolve || {};
          config.resolve.alias = { demo: true };
          return config;
        }
      },
      {
        projectRoot
      }
    );

    const config = wrapped.webpack(
      {
        module: {
          rules: []
        },
        plugins: []
      },
      {
        isServer: true
      }
    );

    expect(fs.existsSync(bootstrapPathForProject(projectRoot))).toBe(true);
    expect(config.module.rules).toHaveLength(1);
    expect(config.module.rules[0].use[0].options.projectRoot).toBe(projectRoot);
    expect(config.plugins.some((plugin) => plugin.constructor.name === 'WaymarkBootstrapPlugin')).toBe(true);
    expect(config.resolve.alias).toEqual({ demo: true });
  });
});
