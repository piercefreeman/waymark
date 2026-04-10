pub use waymark_vcr_file::instance::LogItem;

pub type Sender = tokio::sync::mpsc::Sender<LogItem>;
pub type Receiver = tokio::sync::mpsc::Receiver<LogItem>;

pub struct Params<Backend> {
    pub execution_correlator_prep: crate::execution_correlator::PrepHandle,
    pub backend: Backend,
    pub player_rx: Receiver,
}

pub async fn run<Backend>(params: Params<Backend>) {
    drop(params);

    // let Params {
    //     execution_correlator_prep,
    //     backend,
    //     player_rx,
    // } = params;

    // loop {
    //     // let backend =

    //     // execution_correlator_prep.prepare_correlation(
    //     //     executor_id,
    //     //     execution_id,
    //     //     attempt_number,
    //     //     log_item,
    //     // );
    // }
}
