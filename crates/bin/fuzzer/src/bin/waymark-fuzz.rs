use anyhow::Result;
use clap::Parser;
use waymark_fuzzer::{FuzzArgs, run};

#[tokio::main]
async fn main() -> Result<()> {
    waymark_fn_main_common::init()?;

    let args = FuzzArgs::parse();
    run(args).await
}
