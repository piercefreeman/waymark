import { credentials, type ChannelCredentials } from "@grpc/grpc-js";

import { WorkflowServiceClient } from "./internal/proto/messages.js";

export function createWorkflowClient(
  target: string,
  channelCredentials: ChannelCredentials = credentials.createInsecure(),
): WorkflowServiceClient {
  return new WorkflowServiceClient(target, channelCredentials);
}
