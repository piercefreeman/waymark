'use strict';

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const DEFAULT_BOOT_BINARY = 'waymark-boot-singleton';
const BOOT_TIMEOUT_MS = 10_000;
const BUILD_TIMEOUT_MS = 300_000;

let cachedGrpcPort = null;
let bootPromise = null;

function repoRoots(env = process.env) {
  const roots = [];
  const seen = new Set();

  function addAncestors(startPath) {
    if (!startPath) {
      return;
    }

    let current = path.resolve(startPath);
    while (true) {
      if (!seen.has(current)) {
        seen.add(current);
        roots.push(current);
      }

      const parent = path.dirname(current);
      if (parent === current) {
        break;
      }
      current = parent;
    }
  }

  if (env.WAYMARK_REPO_ROOT) {
    addAncestors(env.WAYMARK_REPO_ROOT);
  }

  addAncestors(process.cwd());
  addAncestors(__dirname);

  return roots;
}

function repoRoot(env = process.env) {
  for (const candidate of repoRoots(env)) {
    if (fs.existsSync(path.join(candidate, 'Cargo.toml'))) {
      return candidate;
    }
  }

  return repoRoots(env)[0] || process.cwd();
}

function envGrpcPortOverride(env = process.env) {
  if (!env.WAYMARK_BRIDGE_GRPC_PORT) {
    return null;
  }

  const port = Number.parseInt(env.WAYMARK_BRIDGE_GRPC_PORT, 10);
  if (Number.isNaN(port)) {
    throw new Error(
      `invalid WAYMARK_BRIDGE_GRPC_PORT value: ${env.WAYMARK_BRIDGE_GRPC_PORT}`
    );
  }

  return port;
}

function explicitBridgeAddress(env = process.env) {
  if (!env.WAYMARK_BRIDGE_GRPC_ADDR) {
    return null;
  }

  const address = env.WAYMARK_BRIDGE_GRPC_ADDR.trim();
  return address ? address : null;
}

function resolveBootBinary(binary, env = process.env) {
  if (path.isAbsolute(binary)) {
    return binary;
  }

  if (binary.includes(path.sep)) {
    return path.resolve(binary);
  }

  const inPath = findExecutableInPath(binary, env);
  if (inPath) {
    return inPath;
  }

  for (const candidate of targetBinaryCandidates(binary, env)) {
    if (fs.existsSync(candidate)) {
      return candidate;
    }
  }

  return binary;
}

function findExecutableInPath(binary, env = process.env) {
  const pathEntries = (env.PATH || '').split(path.delimiter).filter(Boolean);
  for (const entry of pathEntries) {
    const candidate = path.join(entry, binary);
    if (fs.existsSync(candidate)) {
      return candidate;
    }
  }

  return null;
}

function targetBinaryCandidates(binary, env = process.env) {
  const candidates = [];
  const targetDirectories = [];

  if (env.CARGO_TARGET_DIR) {
    targetDirectories.push(env.CARGO_TARGET_DIR);
  }

  for (const root of repoRoots(env)) {
    targetDirectories.push(path.join(root, 'target', 'llvm-cov-target'));
    targetDirectories.push(path.join(root, 'target'));
  }

  for (const directory of targetDirectories) {
    candidates.push(path.join(directory, 'debug', binary));
    candidates.push(path.join(directory, 'release', binary));
  }

  return candidates;
}

function ensureBootBinary(binary, env = process.env) {
  const resolved = resolveBootBinary(binary, env);
  if (fs.existsSync(resolved)) {
    return resolved;
  }

  const root = repoRoot(env);
  const cargoToml = path.join(root, 'Cargo.toml');
  if (!fs.existsSync(cargoToml)) {
    return resolved;
  }

  const build = spawnSync(
    'cargo',
    ['build', '--bin', 'waymark-boot-singleton', '--bin', 'waymark-bridge'],
    {
      cwd: root,
      encoding: 'utf8',
      env,
      timeout: BUILD_TIMEOUT_MS
    }
  );

  if (build.error) {
    throw new Error(`failed to build Waymark bridge binaries: ${build.error.message}`);
  }

  if (build.status !== 0) {
    throw new Error(
      `failed to build Waymark bridge binaries: ${formatSpawnOutput(build.stderr)}`
    );
  }

  const rebuilt = resolveBootBinary(binary, env);
  if (fs.existsSync(rebuilt)) {
    return rebuilt;
  }

  return resolved;
}

function bootCommand(env = process.env) {
  if (env.WAYMARK_BOOT_COMMAND) {
    return {
      command: env.WAYMARK_BOOT_COMMAND,
      shell: env.SHELL || '/bin/sh',
      usesCommandString: true
    };
  }

  return {
    command: ensureBootBinary(env.WAYMARK_BOOT_BINARY || DEFAULT_BOOT_BINARY, env),
    shell: false,
    usesCommandString: false
  };
}

function bootSingletonBlocking(env = process.env) {
  if (explicitBridgeAddress(env)) {
    return null;
  }

  const overridePort = envGrpcPortOverride(env);
  if (overridePort !== null) {
    cachedGrpcPort = overridePort;
    return overridePort;
  }

  const tempDirectory = fs.mkdtempSync(path.join(os.tmpdir(), 'waymark-nextjs-bridge-'));
  const outputFile = path.join(tempDirectory, 'grpc-port.txt');

  try {
    const command = bootCommand(env);
    const child = command.usesCommandString
      ? spawnSync(
        command.shell,
        ['-lc', `${command.command} --output-file ${shellQuote(outputFile)}`],
        {
          encoding: 'utf8',
          env,
          stdio: 'pipe',
          timeout: BOOT_TIMEOUT_MS
        }
      )
      : spawnSync(command.command, ['--output-file', outputFile], {
        encoding: 'utf8',
        env,
        stdio: 'pipe',
        timeout: BOOT_TIMEOUT_MS
      });

    if (child.error) {
      throw new Error(`unable to boot waymark server: ${child.error.message}`);
    }

    if (child.status !== 0) {
      throw new Error(`unable to boot waymark server: ${formatSpawnOutput(child.stderr)}`);
    }

    const portText = fs.readFileSync(outputFile, 'utf8').trim();
    const grpcPort = Number.parseInt(portText, 10);
    if (Number.isNaN(grpcPort)) {
      throw new Error(`unable to read port from output file: ${portText}`);
    }

    cachedGrpcPort = grpcPort;
    return grpcPort;
  } finally {
    fs.rmSync(tempDirectory, { force: true, recursive: true });
  }
}

async function ensureSingleton(env = process.env) {
  if (explicitBridgeAddress(env)) {
    return null;
  }

  const overridePort = envGrpcPortOverride(env);
  if (overridePort !== null) {
    cachedGrpcPort = overridePort;
    return overridePort;
  }

  if (cachedGrpcPort !== null) {
    return cachedGrpcPort;
  }

  if (!bootPromise) {
    bootPromise = Promise.resolve().then(() => bootSingletonBlocking(env));
    bootPromise.catch(() => {
      cachedGrpcPort = null;
    }).finally(() => {
      bootPromise = null;
    });
  }

  return await bootPromise;
}

function shellQuote(value) {
  return `'${String(value).replace(/'/g, `'\\''`)}'`;
}

function formatSpawnOutput(output) {
  const text = String(output || '').trim();
  if (!text) {
    return 'process exited without stderr';
  }
  return text;
}

function resetSingletonState() {
  cachedGrpcPort = null;
  bootPromise = null;
}

module.exports = {
  bootSingletonBlocking,
  ensureSingleton,
  explicitBridgeAddress,
  envGrpcPortOverride,
  resetSingletonState,
  resolveBootBinary
};
