use clap::Parser as _;

mod integration;
mod run;

mod sample {
    pub mod bytecode;
}

use self::run::run;

#[derive(Debug, clap::Parser)]
enum Command {
    BytecodeSample,
}

#[tokio::main]
async fn main() -> Result<(), waymark_fn_main_common::Error> {
    waymark_fn_main_common::init()?;

    let command = Command::try_parse()?;

    match command {
        Command::BytecodeSample => bytecode_sample().await,
    }
}

async fn bytecode_sample() -> Result<(), waymark_fn_main_common::Error> {
    let executable = sample::bytecode::executable();

    let value = run(executable).await;

    let expected = (42 + 5) * 2;

    tracing::info!(?value, ?expected, "program complete");
    Ok(())
}
