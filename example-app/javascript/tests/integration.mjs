import assert from 'node:assert/strict';

const APP_BASE_URL = process.env.APP_BASE_URL || 'http://webapp:3000';
const DASHBOARD_BASE_URL = process.env.WAYMARK_DASHBOARD_URL || 'http://daemons:24119';
const READY_TIMEOUT_MS = Number.parseInt(process.env.READY_TIMEOUT_MS || '120000', 10);
const REQUEST_TIMEOUT_MS = Number.parseInt(process.env.REQUEST_TIMEOUT_MS || '15000', 10);

async function main() {
  const home = await waitForJsonOrText(`${APP_BASE_URL}/`);
  assert.equal(home.response.status, 200, 'webapp homepage should respond with 200');
  assert.match(home.text, /Waymark JavaScript Example/, 'homepage should render the example app');

  const dashboard = await waitForJsonOrText(`${DASHBOARD_BASE_URL}/`);
  assert.equal(dashboard.response.status, 200, 'dashboard should respond with 200');
  assert.ok(dashboard.text.length > 0, 'dashboard response should not be empty');

  const runResponse = await waitForJsonOrText(`${APP_BASE_URL}/api/run`, {
    body: JSON.stringify({ number: 7 }),
    headers: {
      'content-type': 'application/json'
    },
    method: 'POST'
  });

  assert.equal(runResponse.response.status, 200, 'workflow run should succeed');
  const payload = JSON.parse(runResponse.text);
  assert.deepEqual(payload, {
    result: {
      doubled: 14,
      square: 49,
      summary: '7 doubled is 14 and squared is 49.',
      value: 7
    }
  });

  process.stdout.write(
    `Verified webapp at ${APP_BASE_URL}, dashboard at ${DASHBOARD_BASE_URL}, and workflow execution.\n`
  );
}

async function waitForJsonOrText(url, options = {}) {
  const deadline = Date.now() + READY_TIMEOUT_MS;
  let lastError = null;

  while (Date.now() < deadline) {
    try {
      const response = await fetchWithTimeout(url, options);
      const text = await response.text();

      if (response.ok) {
        return {
          response,
          text
        };
      }

      lastError = new Error(`HTTP ${response.status} from ${url}: ${text}`);
    } catch (error) {
      lastError = error;
    }

    await sleep(1000);
  }

  throw new Error(
    `Timed out waiting for ${url}: ${lastError instanceof Error ? lastError.message : String(lastError)}`
  );
}

async function fetchWithTimeout(url, options) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);

  try {
    return await fetch(url, {
      ...options,
      signal: controller.signal
    });
  } finally {
    clearTimeout(timeout);
  }
}

function sleep(durationMs) {
  return new Promise((resolve) => setTimeout(resolve, durationMs));
}

main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.stack || error.message : String(error)}\n`);
  process.exitCode = 1;
});
