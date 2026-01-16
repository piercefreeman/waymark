import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
process.env.RAPPEL_PROTO_ROOT = path.resolve(__dirname, "..", "..", "proto");
