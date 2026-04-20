'use strict';

const crypto = require('node:crypto');

const grpc = require('@grpc/grpc-js');

const messages = require('../generated/messages_pb.js');
const services = require('../generated/messages_grpc_pb.js');
const { deserializeWorkflowResultPayload, serializeWorkflowArguments } = require('./serialization.js');

let cachedClient = null;
let cachedTarget = null;

async function runCompiledWorkflow(workflowCtor, args) {
  const metadata = getWorkflowMetadata(workflowCtor);
  const registration = buildWorkflowRegistration(metadata, args);
  const client = getWorkflowClient();
  const instance = await registerWorkflow(client, registration);

  if (skipWaitForInstance()) {
    return null;
  }

  const payload = await waitForInstance(client, instance.workflowInstanceId);
  if (!payload) {
    throw new Error(`workflow instance ${instance.workflowInstanceId} did not complete`);
  }
  return deserializeWorkflowResultPayload(payload);
}

function buildWorkflowRegistration(metadata, args) {
  const registration = new messages.WorkflowRegistration();
  registration.setWorkflowName(metadata.workflowName);
  registration.setIr(Buffer.from(metadata.programBase64, 'base64'));
  registration.setIrHash(metadata.irHash);
  registration.setWorkflowVersion(metadata.workflowVersion);
  registration.setConcurrent(Boolean(metadata.concurrent));
  registration.setInitialContext(buildInitialContext(metadata, args));
  return registration;
}

function buildInitialContext(metadata, args) {
  const kwargs = {};
  for (let index = 0; index < metadata.inputNames.length; index += 1) {
    kwargs[metadata.inputNames[index]] = args[index];
  }
  return serializeWorkflowArguments(kwargs);
}

function registerWorkflow(client, registration) {
  const request = new messages.RegisterWorkflowRequest();
  request.setRegistration(registration);

  return new Promise((resolve, reject) => {
    client.registerWorkflow(request, (error, response) => {
      if (error) {
        reject(new Error(`registerWorkflow failed: ${error.message}`));
        return;
      }

      resolve({
        workflowInstanceId: response.getWorkflowInstanceId(),
        workflowVersionId: response.getWorkflowVersionId()
      });
    });
  });
}

function waitForInstance(client, instanceId) {
  const request = new messages.WaitForInstanceRequest();
  request.setInstanceId(instanceId);
  request.setPollIntervalSecs(1.0);

  return new Promise((resolve, reject) => {
    client.waitForInstance(request, (error, response) => {
      if (error) {
        reject(new Error(`waitForInstance failed: ${error.message}`));
        return;
      }

      resolve(Buffer.from(response.getPayload_asU8()));
    });
  });
}

function getWorkflowClient() {
  const target = bridgeTarget();
  if (cachedClient && cachedTarget === target) {
    return cachedClient;
  }

  cachedTarget = target;
  cachedClient = new services.WorkflowServiceClient(
    target,
    grpc.credentials.createInsecure()
  );
  return cachedClient;
}

function getWorkflowMetadata(workflowCtor) {
  const metadata = workflowCtor.__waymarkCompiledWorkflow;
  if (!metadata) {
    throw new Error('Workflow class is missing compiled Waymark metadata');
  }

  return metadata;
}

function bridgeTarget() {
  if (process.env.WAYMARK_BRIDGE_GRPC_ADDR) {
    return process.env.WAYMARK_BRIDGE_GRPC_ADDR;
  }

  const host = process.env.WAYMARK_BRIDGE_GRPC_HOST || '127.0.0.1';
  const port = process.env.WAYMARK_BRIDGE_GRPC_PORT || '24117';
  return `${host}:${port}`;
}

function skipWaitForInstance() {
  const value = process.env.WAYMARK_SKIP_WAIT_FOR_INSTANCE;
  if (!value) {
    return false;
  }

  return !['0', 'false', 'no'].includes(value.trim().toLowerCase());
}

function hashProgramBytes(programBytes) {
  return crypto.createHash('sha256').update(programBytes).digest('hex');
}

function resetClientCache() {
  cachedClient = null;
  cachedTarget = null;
}

module.exports = {
  bridgeTarget,
  buildInitialContext,
  buildWorkflowRegistration,
  getWorkflowClient,
  hashProgramBytes,
  registerWorkflow,
  resetClientCache,
  runCompiledWorkflow,
  skipWaitForInstance,
  waitForInstance
};
