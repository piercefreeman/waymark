pub mod instructions;

#[tokio::main]
async fn main() -> Result<(), waymark_fn_main_common::Error> {
    waymark_fn_main_common::init()?;

    tracing::info!("shutdown complete");
    Ok(())
}
