// GENERATED CODE -- DO NOT EDIT!

'use strict';
var grpc = require('@grpc/grpc-js');
var messages_pb = require('./messages_pb.js');
var google_protobuf_struct_pb = require('google-protobuf/google/protobuf/struct_pb.js');

function serialize_waymark_messages_DeleteScheduleRequest(arg) {
  if (!(arg instanceof messages_pb.DeleteScheduleRequest)) {
    throw new Error('Expected argument of type waymark.messages.DeleteScheduleRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_waymark_messages_DeleteScheduleRequest(buffer_arg) {
  return messages_pb.DeleteScheduleRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_waymark_messages_DeleteScheduleResponse(arg) {
  if (!(arg instanceof messages_pb.DeleteScheduleResponse)) {
    throw new Error('Expected argument of type waymark.messages.DeleteScheduleResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_waymark_messages_DeleteScheduleResponse(buffer_arg) {
  return messages_pb.DeleteScheduleResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_waymark_messages_Envelope(arg) {
  if (!(arg instanceof messages_pb.Envelope)) {
    throw new Error('Expected argument of type waymark.messages.Envelope');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_waymark_messages_Envelope(buffer_arg) {
  return messages_pb.Envelope.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_waymark_messages_ListSchedulesRequest(arg) {
  if (!(arg instanceof messages_pb.ListSchedulesRequest)) {
    throw new Error('Expected argument of type waymark.messages.ListSchedulesRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_waymark_messages_ListSchedulesRequest(buffer_arg) {
  return messages_pb.ListSchedulesRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_waymark_messages_ListSchedulesResponse(arg) {
  if (!(arg instanceof messages_pb.ListSchedulesResponse)) {
    throw new Error('Expected argument of type waymark.messages.ListSchedulesResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_waymark_messages_ListSchedulesResponse(buffer_arg) {
  return messages_pb.ListSchedulesResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_waymark_messages_RegisterScheduleRequest(arg) {
  if (!(arg instanceof messages_pb.RegisterScheduleRequest)) {
    throw new Error('Expected argument of type waymark.messages.RegisterScheduleRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_waymark_messages_RegisterScheduleRequest(buffer_arg) {
  return messages_pb.RegisterScheduleRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_waymark_messages_RegisterScheduleResponse(arg) {
  if (!(arg instanceof messages_pb.RegisterScheduleResponse)) {
    throw new Error('Expected argument of type waymark.messages.RegisterScheduleResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_waymark_messages_RegisterScheduleResponse(buffer_arg) {
  return messages_pb.RegisterScheduleResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_waymark_messages_RegisterWorkflowBatchRequest(arg) {
  if (!(arg instanceof messages_pb.RegisterWorkflowBatchRequest)) {
    throw new Error('Expected argument of type waymark.messages.RegisterWorkflowBatchRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_waymark_messages_RegisterWorkflowBatchRequest(buffer_arg) {
  return messages_pb.RegisterWorkflowBatchRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_waymark_messages_RegisterWorkflowBatchResponse(arg) {
  if (!(arg instanceof messages_pb.RegisterWorkflowBatchResponse)) {
    throw new Error('Expected argument of type waymark.messages.RegisterWorkflowBatchResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_waymark_messages_RegisterWorkflowBatchResponse(buffer_arg) {
  return messages_pb.RegisterWorkflowBatchResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_waymark_messages_RegisterWorkflowRequest(arg) {
  if (!(arg instanceof messages_pb.RegisterWorkflowRequest)) {
    throw new Error('Expected argument of type waymark.messages.RegisterWorkflowRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_waymark_messages_RegisterWorkflowRequest(buffer_arg) {
  return messages_pb.RegisterWorkflowRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_waymark_messages_RegisterWorkflowResponse(arg) {
  if (!(arg instanceof messages_pb.RegisterWorkflowResponse)) {
    throw new Error('Expected argument of type waymark.messages.RegisterWorkflowResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_waymark_messages_RegisterWorkflowResponse(buffer_arg) {
  return messages_pb.RegisterWorkflowResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_waymark_messages_UpdateScheduleStatusRequest(arg) {
  if (!(arg instanceof messages_pb.UpdateScheduleStatusRequest)) {
    throw new Error('Expected argument of type waymark.messages.UpdateScheduleStatusRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_waymark_messages_UpdateScheduleStatusRequest(buffer_arg) {
  return messages_pb.UpdateScheduleStatusRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_waymark_messages_UpdateScheduleStatusResponse(arg) {
  if (!(arg instanceof messages_pb.UpdateScheduleStatusResponse)) {
    throw new Error('Expected argument of type waymark.messages.UpdateScheduleStatusResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_waymark_messages_UpdateScheduleStatusResponse(buffer_arg) {
  return messages_pb.UpdateScheduleStatusResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_waymark_messages_WaitForInstanceRequest(arg) {
  if (!(arg instanceof messages_pb.WaitForInstanceRequest)) {
    throw new Error('Expected argument of type waymark.messages.WaitForInstanceRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_waymark_messages_WaitForInstanceRequest(buffer_arg) {
  return messages_pb.WaitForInstanceRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_waymark_messages_WaitForInstanceResponse(arg) {
  if (!(arg instanceof messages_pb.WaitForInstanceResponse)) {
    throw new Error('Expected argument of type waymark.messages.WaitForInstanceResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_waymark_messages_WaitForInstanceResponse(buffer_arg) {
  return messages_pb.WaitForInstanceResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_waymark_messages_WorkflowStreamRequest(arg) {
  if (!(arg instanceof messages_pb.WorkflowStreamRequest)) {
    throw new Error('Expected argument of type waymark.messages.WorkflowStreamRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_waymark_messages_WorkflowStreamRequest(buffer_arg) {
  return messages_pb.WorkflowStreamRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_waymark_messages_WorkflowStreamResponse(arg) {
  if (!(arg instanceof messages_pb.WorkflowStreamResponse)) {
    throw new Error('Expected argument of type waymark.messages.WorkflowStreamResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_waymark_messages_WorkflowStreamResponse(buffer_arg) {
  return messages_pb.WorkflowStreamResponse.deserializeBinary(new Uint8Array(buffer_arg));
}


// =============================================================================
// gRPC Service Definitions
// =============================================================================
//
// Bidirectional streaming service for worker communication.
// Workers connect and maintain a persistent stream for action dispatch/results.
var WorkerBridgeService = exports.WorkerBridgeService = {
  attach: {
    path: '/waymark.messages.WorkerBridge/Attach',
    requestStream: true,
    responseStream: true,
    requestType: messages_pb.Envelope,
    responseType: messages_pb.Envelope,
    requestSerialize: serialize_waymark_messages_Envelope,
    requestDeserialize: deserialize_waymark_messages_Envelope,
    responseSerialize: serialize_waymark_messages_Envelope,
    responseDeserialize: deserialize_waymark_messages_Envelope,
  },
};

exports.WorkerBridgeClient = grpc.makeGenericClientConstructor(WorkerBridgeService, 'WorkerBridge');
// Workflow management service for client operations.
var WorkflowServiceService = exports.WorkflowServiceService = {
  registerWorkflow: {
    path: '/waymark.messages.WorkflowService/RegisterWorkflow',
    requestStream: false,
    responseStream: false,
    requestType: messages_pb.RegisterWorkflowRequest,
    responseType: messages_pb.RegisterWorkflowResponse,
    requestSerialize: serialize_waymark_messages_RegisterWorkflowRequest,
    requestDeserialize: deserialize_waymark_messages_RegisterWorkflowRequest,
    responseSerialize: serialize_waymark_messages_RegisterWorkflowResponse,
    responseDeserialize: deserialize_waymark_messages_RegisterWorkflowResponse,
  },
  registerWorkflowBatch: {
    path: '/waymark.messages.WorkflowService/RegisterWorkflowBatch',
    requestStream: false,
    responseStream: false,
    requestType: messages_pb.RegisterWorkflowBatchRequest,
    responseType: messages_pb.RegisterWorkflowBatchResponse,
    requestSerialize: serialize_waymark_messages_RegisterWorkflowBatchRequest,
    requestDeserialize: deserialize_waymark_messages_RegisterWorkflowBatchRequest,
    responseSerialize: serialize_waymark_messages_RegisterWorkflowBatchResponse,
    responseDeserialize: deserialize_waymark_messages_RegisterWorkflowBatchResponse,
  },
  waitForInstance: {
    path: '/waymark.messages.WorkflowService/WaitForInstance',
    requestStream: false,
    responseStream: false,
    requestType: messages_pb.WaitForInstanceRequest,
    responseType: messages_pb.WaitForInstanceResponse,
    requestSerialize: serialize_waymark_messages_WaitForInstanceRequest,
    requestDeserialize: deserialize_waymark_messages_WaitForInstanceRequest,
    responseSerialize: serialize_waymark_messages_WaitForInstanceResponse,
    responseDeserialize: deserialize_waymark_messages_WaitForInstanceResponse,
  },
  executeWorkflow: {
    path: '/waymark.messages.WorkflowService/ExecuteWorkflow',
    requestStream: true,
    responseStream: true,
    requestType: messages_pb.WorkflowStreamRequest,
    responseType: messages_pb.WorkflowStreamResponse,
    requestSerialize: serialize_waymark_messages_WorkflowStreamRequest,
    requestDeserialize: deserialize_waymark_messages_WorkflowStreamRequest,
    responseSerialize: serialize_waymark_messages_WorkflowStreamResponse,
    responseDeserialize: deserialize_waymark_messages_WorkflowStreamResponse,
  },
  // Schedule management
registerSchedule: {
    path: '/waymark.messages.WorkflowService/RegisterSchedule',
    requestStream: false,
    responseStream: false,
    requestType: messages_pb.RegisterScheduleRequest,
    responseType: messages_pb.RegisterScheduleResponse,
    requestSerialize: serialize_waymark_messages_RegisterScheduleRequest,
    requestDeserialize: deserialize_waymark_messages_RegisterScheduleRequest,
    responseSerialize: serialize_waymark_messages_RegisterScheduleResponse,
    responseDeserialize: deserialize_waymark_messages_RegisterScheduleResponse,
  },
  updateScheduleStatus: {
    path: '/waymark.messages.WorkflowService/UpdateScheduleStatus',
    requestStream: false,
    responseStream: false,
    requestType: messages_pb.UpdateScheduleStatusRequest,
    responseType: messages_pb.UpdateScheduleStatusResponse,
    requestSerialize: serialize_waymark_messages_UpdateScheduleStatusRequest,
    requestDeserialize: deserialize_waymark_messages_UpdateScheduleStatusRequest,
    responseSerialize: serialize_waymark_messages_UpdateScheduleStatusResponse,
    responseDeserialize: deserialize_waymark_messages_UpdateScheduleStatusResponse,
  },
  deleteSchedule: {
    path: '/waymark.messages.WorkflowService/DeleteSchedule',
    requestStream: false,
    responseStream: false,
    requestType: messages_pb.DeleteScheduleRequest,
    responseType: messages_pb.DeleteScheduleResponse,
    requestSerialize: serialize_waymark_messages_DeleteScheduleRequest,
    requestDeserialize: deserialize_waymark_messages_DeleteScheduleRequest,
    responseSerialize: serialize_waymark_messages_DeleteScheduleResponse,
    responseDeserialize: deserialize_waymark_messages_DeleteScheduleResponse,
  },
  listSchedules: {
    path: '/waymark.messages.WorkflowService/ListSchedules',
    requestStream: false,
    responseStream: false,
    requestType: messages_pb.ListSchedulesRequest,
    responseType: messages_pb.ListSchedulesResponse,
    requestSerialize: serialize_waymark_messages_ListSchedulesRequest,
    requestDeserialize: deserialize_waymark_messages_ListSchedulesRequest,
    responseSerialize: serialize_waymark_messages_ListSchedulesResponse,
    responseDeserialize: deserialize_waymark_messages_ListSchedulesResponse,
  },
};

exports.WorkflowServiceClient = grpc.makeGenericClientConstructor(WorkflowServiceService, 'WorkflowService');
