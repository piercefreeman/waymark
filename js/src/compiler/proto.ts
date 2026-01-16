import { loadAstRoot } from "../proto.js";

export async function encodeProgram(program) {
  const root = await loadAstRoot();
  const Program = root.lookupType("rappel.ast.Program");
  const err = Program.verify(program);
  if (err) {
    throw new Error(`invalid workflow IR: ${err}`);
  }
  return Program.encode(program).finish();
}
