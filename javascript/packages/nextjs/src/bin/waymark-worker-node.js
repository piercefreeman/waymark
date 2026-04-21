#!/usr/bin/env node
'use strict';

const process = require('node:process');

const { main } = require('../runtime/worker.js');

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  process.stderr.write(`[waymark-worker-node] ${message}\n`);
  process.exitCode = 1;
});
