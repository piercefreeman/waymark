use clap::Parser as _;

mod run;

mod sample {
    pub mod ast_old;
    pub mod bytecode;
}

use self::run::run;

#[derive(Debug, clap::Parser)]
enum Command {
    BytecodeSample,
    AstOldSample,
}

#[tokio::main]
async fn main() -> Result<(), waymark_fn_main_common::Error> {
    waymark_fn_main_common::init()?;

    let command = Command::try_parse()?;

    match command {
        Command::BytecodeSample => bytecode_sample().await,
        Command::AstOldSample => ast_old_sample().await,
    }
}

async fn bytecode_sample() -> Result<(), waymark_fn_main_common::Error> {
    let executable = sample::bytecode::executable();

    let interpreter = waymark_system_vm::Interpreter::default();
    let runtime = waymark_system_vm::Runtime::with_conventional_entrypoint(
        interpreter,
        std::sync::Arc::new(executable),
    )?;

    let workflow_outcome = run(runtime).await?;

    let expected = (21 * 2 + 5) * 2;
    tracing::info!(?workflow_outcome, ?expected, "program complete");
    Ok(())
}

async fn ast_old_sample() -> Result<(), waymark_fn_main_common::Error> {
    let program = sample::ast_old::program();

    let runtime = waymark_transient_execution_bringup::setup_runtime(
        &program,
        std::collections::HashMap::new(),
    )?;

    let workflow_outcome = run(runtime).await?;

    let expected = (2 + 3) * 2;
    tracing::info!(?workflow_outcome, ?expected, "program complete");
    Ok(())
}
