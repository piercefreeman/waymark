// Stands in for `vite build`: checks the arguments it receives and writes a
// bundle whose index.html records the directory the build ran in.
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

// Expect exactly `--emptyOutDir --outDir <output>`.
assert.deepEqual(process.argv.slice(2, 4), ["--emptyOutDir", "--outDir"]);
assert.equal(process.argv.length, 5);

const output = process.argv[4];

// Write the bundle: the entry point and one asset.
fs.mkdirSync(path.join(output, "assets"), { recursive: true });
fs.writeFileSync(path.join(output, "index.html"), process.cwd());
fs.writeFileSync(path.join(output, "assets/app.js"), 'console.log("hello");');
