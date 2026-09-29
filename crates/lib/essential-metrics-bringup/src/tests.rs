use super::*;

/// The batcher's name is what the sampler's recorder filters by: the two
/// copies must agree, or the drops never reach the sample.
#[test]
fn batcher_name_matches_the_sampler_binding() {
    assert_eq!(
        BATCHER_NAME,
        waymark_essential_metrics_sampler::bindings::BATCHER_NAME_ESSENTIAL_METRICS
    );
}
