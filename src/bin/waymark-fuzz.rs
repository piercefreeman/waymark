use anyhow::Result;
use clap::Parser;
use waymark::fuzzer::{FuzzArgs, run};

#[tokio::main]
async fn main() -> Result<()> {
    let args = FuzzArgs::parse();
    run(args).await
}
