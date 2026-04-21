import { RunDemo } from './run-demo';

export default function HomePage() {
  return (
    <main
      style={{
        minHeight: '100vh',
        padding: '3rem 1.5rem',
        background:
          'linear-gradient(180deg, rgba(248,250,252,1) 0%, rgba(255,255,255,1) 55%, rgba(239,246,255,0.6) 100%)',
        color: '#0f172a',
        fontFamily:
          'ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif'
      }}
    >
      <div
        style={{
          maxWidth: '54rem',
          margin: '0 auto',
          display: 'grid',
          gap: '1.5rem'
        }}
      >
        <div style={{ display: 'grid', gap: '0.75rem' }}>
          <p
            style={{
              margin: 0,
              fontSize: '0.875rem',
              fontWeight: 700,
              letterSpacing: '0.12em',
              textTransform: 'uppercase',
              color: '#2563eb'
            }}
          >
            Waymark JavaScript Example
          </p>
          <h1 style={{ margin: 0, fontSize: '2.5rem', lineHeight: 1.05 }}>
            Next.js authoring example for JavaScript workflows
          </h1>
          <p style={{ margin: 0, fontSize: '1.05rem', lineHeight: 1.6, color: '#475569' }}>
            This app shows the intended JavaScript API surface: exported async actions marked with
            <code
              style={{
                marginLeft: '0.4rem',
                marginRight: '0.4rem',
                padding: '0.15rem 0.4rem',
                borderRadius: '0.4rem',
                background: '#e2e8f0'
              }}
            >
              // use action
            </code>
            and workflow classes that extend
            <code
              style={{
                marginLeft: '0.4rem',
                padding: '0.15rem 0.4rem',
                borderRadius: '0.4rem',
                background: '#e2e8f0'
              }}
            >
              Workflow
            </code>
            .
          </p>
        </div>

        <section
          style={{
            border: '1px solid #cbd5e1',
            borderRadius: '0.75rem',
            background: '#ffffff',
            padding: '1.25rem'
          }}
        >
          <h2 style={{ marginTop: 0 }}>Runtime path</h2>
          <p style={{ marginBottom: 0, lineHeight: 1.6, color: '#475569' }}>
            <code>run()</code> is rewritten into IR submission instead of executing as normal
            JavaScript. When <code>WAYMARK_DATABASE_URL</code> is set, the request goes through a
            Postgres-backed Waymark deployment with remote JavaScript workers and the dashboard on
            <code style={{ marginLeft: '0.35rem' }}>http://localhost:24119/</code>. Without stack
            config, the route falls back to the in-memory bridge path for local iteration.
          </p>
        </section>

        <section
          style={{
            border: '1px solid #cbd5e1',
            borderRadius: '0.75rem',
            background: '#ffffff',
            padding: '1.25rem'
          }}
        >
          <h2 style={{ marginTop: 0 }}>Full-stack commands</h2>
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
            {`cd example-app/javascript
make up

# app: http://localhost:3000/
# dashboard: http://localhost:24119/`}
          </pre>
        </section>

        <section
          style={{
            border: '1px solid #cbd5e1',
            borderRadius: '0.75rem',
            background: '#ffffff',
            padding: '1.25rem'
          }}
        >
          <h2 style={{ marginTop: 0 }}>Example request</h2>
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
            {`curl -X POST http://localhost:3000/api/run \\
  -H "content-type: application/json" \\
  -d '{"number": 5}'`}
          </pre>
        </section>

        <RunDemo />
      </div>
    </main>
  );
}
