import path from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import protobuf from "protobufjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);
const messagesProto = require("../proto/messages_pb.js");
const grpcProto = require("../proto/messages_grpc_pb.js");
const jsRoot = path.resolve(__dirname, "..", "..");
const repoRoot = path.resolve(jsRoot, "..");
const protoRoot = resolveProtoRoot();
const googleProtoRoot = path.join(jsRoot, "proto");

const astProtoPath = path.join(protoRoot, "ast.proto");
const messagesProtoPath = path.join(protoRoot, "messages.proto");

let astRootPromise = null;
let messagesRootPromise = null;

export async function loadAstRoot() {
  if (!astRootPromise) {
    astRootPromise = loadRoot([astProtoPath]);
  }
  return astRootPromise;
}

export async function loadMessagesRoot() {
  if (!messagesRootPromise) {
    messagesRootPromise = loadRoot([messagesProtoPath]);
  }
  return messagesRootPromise;
}

export function getMessagesProto() {
  return messagesProto;
}

export function getGrpcProto() {
  return grpcProto;
}

export async function encodeMessage(typeName, payload) {
  const root = await loadMessagesRoot();
  const Type = root.lookupType(typeName);
  const message = Type.create(payload ?? {});
  return Type.encode(message).finish();
}

export async function decodeMessage(typeName, bytes) {
  const root = await loadMessagesRoot();
  const Type = root.lookupType(typeName);
  const decoded = Type.decode(bytes);
  return Type.toObject(decoded, {
    longs: String,
    defaults: true,
  });
}

export async function toProtoMessage(typeName, payload) {
  const MessageClass = resolveMessageClass(typeName);
  if (!MessageClass) {
    throw new Error(`unknown protobuf message: ${typeName}`);
  }
  const bytes = await encodeMessage(typeName, payload);
  return MessageClass.deserializeBinary(bytes);
}

export async function fromProtoMessage(typeName, message) {
  if (!message || typeof message.serializeBinary !== "function") {
    throw new Error(`invalid protobuf message for ${typeName}`);
  }
  return decodeMessage(typeName, message.serializeBinary());
}

function resolveMessageClass(typeName) {
  const shortName = typeName.split(".").pop();
  return messagesProto?.[shortName] ?? null;
}

function resolveProtoRoot() {
  const explicitRoot = process.env.RAPPEL_PROTO_ROOT;
  if (explicitRoot) {
    return path.resolve(explicitRoot);
  }
  return path.join(repoRoot, "proto");
}

async function loadRoot(paths) {
  const root = new protobuf.Root();
  root.resolvePath = (origin, target) => {
    if (target.startsWith("google/protobuf/")) {
      return path.join(googleProtoRoot, target);
    }
    if (path.isAbsolute(target)) {
      return target;
    }
    if (!origin) {
      return path.join(protoRoot, target);
    }
    return path.join(path.dirname(origin), target);
  };

  await root.load(paths, { keepCase: true });
  root.resolveAll();
  return root;
}
