"use client";

import { type FormEvent, useState } from "react";

interface MathResult {
  factorial: number;
  fibonacci: number;
  number: number;
  summary: string;
}

export default function Home() {
  const [number, setNumber] = useState(5);
  const [result, setResult] = useState<MathResult>();
  const [error, setError] = useState<string>();
  const [running, setRunning] = useState(false);

  async function runWorkflow(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setRunning(true);
    setError(undefined);
    try {
      const response = await fetch("/api/math", {
        body: JSON.stringify({ number }),
        headers: { "content-type": "application/json" },
        method: "POST",
      });
      const body = (await response.json()) as MathResult | { error: string };
      if (!response.ok) {
        throw new Error("error" in body ? body.error : "workflow failed");
      }
      setResult(body as MathResult);
    } catch (caught) {
      setResult(undefined);
      setError(caught instanceof Error ? caught.message : "workflow failed");
    } finally {
      setRunning(false);
    }
  }

  return (
    <main>
      <section className="intro">
        <p className="eyebrow">WAYMARK · JAVASCRIPT VM</p>
        <h1>Parallel math, durable control flow.</h1>
        <p className="lede">
          This Next.js route compiles a typed <code>Workflow.run()</code>,
          executes factorial and Fibonacci actions in parallel, and returns the
          VM result.
        </p>
      </section>

      <section className="runner" aria-labelledby="runner-title">
        <div>
          <p className="section-label">ExampleMathWorkflow</p>
          <h2 id="runner-title">Run the workflow</h2>
        </div>

        <form onSubmit={runWorkflow}>
          <label htmlFor="number">Integer from 1 to 10</label>
          <div className="controls">
            <input
              id="number"
              max={10}
              min={1}
              onChange={(event) => setNumber(event.currentTarget.valueAsNumber)}
              type="number"
              value={number}
            />
            <button disabled={running} type="submit">
              {running ? "Running…" : "Run workflow"}
            </button>
          </div>
        </form>

        <div className="output" aria-live="polite">
          {error !== undefined ? <p className="error">{error}</p> : null}
          {result !== undefined ? (
            <>
              <div className="metrics">
                <span>
                  <small>Factorial</small>
                  <strong>{result.factorial}</strong>
                </span>
                <span>
                  <small>Fibonacci</small>
                  <strong>{result.fibonacci}</strong>
                </span>
              </div>
              <p>{result.summary}</p>
            </>
          ) : (
            <p className="placeholder">
              Submit a number to execute the compiled workflow.
            </p>
          )}
        </div>
      </section>
    </main>
  );
}
