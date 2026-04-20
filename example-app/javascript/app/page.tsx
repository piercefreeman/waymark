export default function HomePage() {
  return (
    <main
      style={{
        minHeight: '100vh',
        padding: '3rem 1.5rem',
        background:
          'linear-gradient(180deg, rgba(241,245,249,1) 0%, rgba(255,255,255,1) 100%)',
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
          <h2 style={{ marginTop: 0 }}>Important limitation</h2>
          <p style={{ marginBottom: 0, lineHeight: 1.6, color: '#475569' }}>
            The compiler and bridge submission path exist, but JavaScript worker execution does not
            yet. Treat this app as an authoring and integration example, not a production-ready
            end-to-end demo.
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
      </div>
    </main>
  );
}
