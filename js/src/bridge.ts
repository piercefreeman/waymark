import { createRequire } from "node:module";
import grpc from "@grpc/grpc-js";

const require = createRequire(import.meta.url);
const proto = require("../proto/messages_grpc_pb.js");

let clientPromise = null;

export async function getWorkflowClient() {
  if (!clientPromise) {
    clientPromise = Promise.resolve(createWorkflowClient());
  }
  return clientPromise;
}

function createWorkflowClient() {
  const { WorkflowServiceClient } = proto;
  if (!WorkflowServiceClient) {
    throw new Error("unable to load WorkflowService client");
  }
  return new WorkflowServiceClient(
    resolveGrpcTarget(),
    grpc.credentials.createInsecure()
  );
}

export function resolveGrpcTarget() {
  const explicit = process.env.RAPPEL_BRIDGE_GRPC_ADDR;
  if (explicit) {
    return explicit;
  }
  const host = process.env.RAPPEL_BRIDGE_GRPC_HOST || "127.0.0.1";
  const port = process.env.RAPPEL_BRIDGE_GRPC_PORT || "24117";
  return `${host}:${port}`;
}

export function callUnary(client, method, request) {
  return new Promise((resolve, reject) => {
    client[method](request, (err, response) => {
      if (err) {
        reject(err);
        return;
      }
      resolve(response);
    });
  });
}
