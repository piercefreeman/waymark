//! Bringup for the webapp server.

use std::sync::Arc;

use anyhow::Context as _;
use tokio::net::TcpListener;
use waymark_webapp_config::WebappConfig;

/// Start the webapp server.
///
/// Returns None if the webapp is disabled via configuration.
pub async fn start<WebappBackend>(
    config: WebappConfig,
    database: Arc<WebappBackend>,
    shutdown_signal: tokio_util::sync::WaitForCancellationFutureOwned,
) -> Result<Option<tokio::task::JoinHandle<()>>, anyhow::Error>
where
    WebappBackend: ?Sized,
    WebappBackend: waymark_webapp_backend::WebappBackend,
    WebappBackend: Send + Sync + 'static,
{
    if !config.enabled {
        // TODO: ideally we'd want to avoid this kind of abstraction leak, i.e.
        // `WAYMARK_WEBAPP_ENABLED` is an implementation detail of the config,
        // and here we shouldn't know how exactly the configuration is loaded.
        // Thankfully there is an elegant solution for this - but we'll address
        // it in later refactors when we'll be improming the configuration
        // layer.
        tracing::info!("webapp disabled (set WAYMARK_WEBAPP_ENABLED=true to enable)");
        return Ok(None);
    }

    let bind_addr = config.bind_addr();
    let listener = TcpListener::bind(&bind_addr)
        .await
        .with_context(|| format!("failed to bind webapp listener on {bind_addr}"))?;

    let actual_addr = listener.local_addr()?;

    // Initialize templates
    let templates = waymark_webapp_routes::init_templates()?;

    let state = waymark_webapp_routes::WebappState {
        database,
        templates: Arc::new(templates),
    };

    let app = waymark_webapp_routes::build_router(state);

    // Spawn the server task
    let task = tokio::spawn(async move {
        let result = axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal)
            .await;
        if let Err(error) = result {
            tracing::error!(?error, "webapp server failed");
        }
    });

    tracing::info!(addr = %actual_addr, "webapp server started");

    Ok(Some(task))
}
