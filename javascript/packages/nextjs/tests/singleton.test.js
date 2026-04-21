'use strict';

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const { ensureSingleton, resetSingletonState } = require('../src/runtime/singleton.js');

function makeTempDirectory() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'waymark-nextjs-singleton-'));
}

function writeExecutableScript(filePath, source) {
  fs.writeFileSync(filePath, source, 'utf8');
  fs.chmodSync(filePath, 0o755);
}

describe('bridge singleton boot', () => {
  const originalEnv = {
    WAYMARK_BRIDGE_GRPC_ADDR: process.env.WAYMARK_BRIDGE_GRPC_ADDR,
    WAYMARK_BOOT_BINARY: process.env.WAYMARK_BOOT_BINARY,
    WAYMARK_BOOT_COMMAND: process.env.WAYMARK_BOOT_COMMAND,
    WAYMARK_BRIDGE_GRPC_PORT: process.env.WAYMARK_BRIDGE_GRPC_PORT,
    WAYMARK_TEST_MARKER_FILE: process.env.WAYMARK_TEST_MARKER_FILE
  };

  beforeEach(() => {
    resetSingletonState();
    delete process.env.WAYMARK_BRIDGE_GRPC_ADDR;
    delete process.env.WAYMARK_BOOT_BINARY;
    delete process.env.WAYMARK_BOOT_COMMAND;
    delete process.env.WAYMARK_BRIDGE_GRPC_PORT;
    delete process.env.WAYMARK_TEST_MARKER_FILE;
  });

  afterEach(() => {
    resetSingletonState();
    restoreEnvValue('WAYMARK_BRIDGE_GRPC_ADDR', originalEnv.WAYMARK_BRIDGE_GRPC_ADDR);
    restoreEnvValue('WAYMARK_BOOT_BINARY', originalEnv.WAYMARK_BOOT_BINARY);
    restoreEnvValue('WAYMARK_BOOT_COMMAND', originalEnv.WAYMARK_BOOT_COMMAND);
    restoreEnvValue('WAYMARK_BRIDGE_GRPC_PORT', originalEnv.WAYMARK_BRIDGE_GRPC_PORT);
    restoreEnvValue('WAYMARK_TEST_MARKER_FILE', originalEnv.WAYMARK_TEST_MARKER_FILE);
  });

  test('boots the singleton once and caches the discovered port', async () => {
    const tempDirectory = makeTempDirectory();
    const markerFile = path.join(tempDirectory, 'marker.txt');
    const bootScript = path.join(tempDirectory, 'boot-singleton.js');

    writeExecutableScript(
      bootScript,
      [
        '#!/usr/bin/env node',
        "const fs = require('node:fs');",
        "const outputIndex = process.argv.indexOf('--output-file');",
        'const outputPath = process.argv[outputIndex + 1];',
        "fs.appendFileSync(process.env.WAYMARK_TEST_MARKER_FILE, 'boot\\n');",
        "fs.writeFileSync(outputPath, '24567\\n', 'utf8');",
        ''
      ].join('\n')
    );

    process.env.WAYMARK_BOOT_BINARY = bootScript;
    process.env.WAYMARK_TEST_MARKER_FILE = markerFile;

    try {
      await expect(ensureSingleton()).resolves.toBe(24567);
      await expect(ensureSingleton()).resolves.toBe(24567);
      expect(fs.readFileSync(markerFile, 'utf8')).toBe('boot\n');
    } finally {
      fs.rmSync(tempDirectory, { force: true, recursive: true });
    }
  });

  test('prefers an explicit gRPC port override without booting a bridge', async () => {
    const tempDirectory = makeTempDirectory();
    const markerFile = path.join(tempDirectory, 'marker.txt');

    process.env.WAYMARK_BOOT_BINARY = path.join(tempDirectory, 'missing-boot-singleton');
    process.env.WAYMARK_BRIDGE_GRPC_PORT = '25123';
    process.env.WAYMARK_TEST_MARKER_FILE = markerFile;

    try {
      await expect(ensureSingleton()).resolves.toBe(25123);
      expect(fs.existsSync(markerFile)).toBe(false);
    } finally {
      fs.rmSync(tempDirectory, { force: true, recursive: true });
    }
  });

  test('skips auto-boot when an explicit bridge address is configured', async () => {
    const tempDirectory = makeTempDirectory();
    const markerFile = path.join(tempDirectory, 'marker.txt');

    process.env.WAYMARK_BOOT_BINARY = path.join(tempDirectory, 'missing-boot-singleton');
    process.env.WAYMARK_BRIDGE_GRPC_ADDR = 'bridge.internal:24117';
    process.env.WAYMARK_TEST_MARKER_FILE = markerFile;

    try {
      await expect(ensureSingleton()).resolves.toBeNull();
      expect(fs.existsSync(markerFile)).toBe(false);
    } finally {
      fs.rmSync(tempDirectory, { force: true, recursive: true });
    }
  });
});

function restoreEnvValue(name, value) {
  if (value === undefined) {
    delete process.env[name];
    return;
  }

  process.env[name] = value;
}
