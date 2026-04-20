'use client';

import { useState } from 'react';

type DemoResult = {
  result?: unknown;
};

export function RunDemo() {
  const [number, setNumber] = useState('5');
  const [error, setError] = useState('');
  const [isRunning, setIsRunning] = useState(false);
  const [response, setResponse] = useState<DemoResult | null>(null);

  async function onSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError('');
    setIsRunning(true);

    try {
      const runResponse = await fetch('/api/run', {
        body: JSON.stringify({ number: Number(number) }),
        headers: {
          'content-type': 'application/json'
        },
        method: 'POST'
      });
      const payload = (await runResponse.json()) as DemoResult & { error?: string };
      if (!runResponse.ok) {
        throw new Error(payload.error || `Request failed with status ${runResponse.status}`);
      }
      setResponse(payload);
    } catch (submitError) {
      setResponse(null);
      setError(submitError instanceof Error ? submitError.message : String(submitError));
    } finally {
      setIsRunning(false);
    }
  }

  return (
    <section
      style={{
        border: '1px solid #cbd5e1',
        borderRadius: '0.75rem',
        background: '#ffffff',
        padding: '1.25rem',
        display: 'grid',
        gap: '1rem'
      }}
    >
      <div>
        <h2 style={{ marginTop: 0, marginBottom: '0.5rem' }}>Run it from the page</h2>
        <p style={{ margin: 0, color: '#475569', lineHeight: 1.6 }}>
          This calls <code>/api/run</code> and renders the workflow result directly in the page.
        </p>
      </div>

      <form onSubmit={onSubmit} style={{ display: 'flex', gap: '0.75rem', flexWrap: 'wrap' }}>
        <label
          style={{
            display: 'grid',
            gap: '0.35rem',
            fontSize: '0.95rem',
            color: '#334155'
          }}
        >
          Number
          <input
            value={number}
            onChange={(event) => setNumber(event.target.value)}
            type="number"
            min={1}
            max={50}
            style={{
              minWidth: '10rem',
              border: '1px solid #94a3b8',
              borderRadius: '0.6rem',
              padding: '0.65rem 0.8rem',
              fontSize: '1rem'
            }}
          />
        </label>

        <button
          type="submit"
          disabled={isRunning}
          style={{
            alignSelf: 'end',
            border: 'none',
            borderRadius: '0.6rem',
            background: isRunning ? '#93c5fd' : '#2563eb',
            color: '#ffffff',
            cursor: isRunning ? 'default' : 'pointer',
            fontSize: '0.95rem',
            fontWeight: 700,
            padding: '0.75rem 1rem'
          }}
        >
          {isRunning ? 'Running…' : 'Run workflow'}
        </button>
      </form>

      {error ? (
        <div
          style={{
            border: '1px solid #fca5a5',
            borderRadius: '0.75rem',
            background: '#fef2f2',
            color: '#991b1b',
            padding: '0.9rem 1rem',
            whiteSpace: 'pre-wrap'
          }}
        >
          {error}
        </div>
      ) : null}

      {response ? (
        <pre
          style={{
            margin: 0,
            overflowX: 'auto',
            padding: '1rem',
            borderRadius: '0.75rem',
            background: '#0f172a',
            color: '#e2e8f0'
          }}
        >
          {JSON.stringify(response, null, 2)}
        </pre>
      ) : null}
    </section>
  );
}
