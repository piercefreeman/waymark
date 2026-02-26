//! Retry/timeout policy helpers shared by runner components.

use waymark_proto::ast as ir;

#[derive(Clone, Debug)]
pub(crate) struct RetryDecision {
    pub(crate) should_retry: bool,
}

pub(crate) struct RetryPolicyEvaluator<'a> {
    policies: &'a [ir::PolicyBracket],
    exception_name: Option<&'a str>,
}

fn is_synthetic_runtime_exception(exception_name: Option<&str>) -> bool {
    matches!(exception_name, Some("ExecutorResume" | "ActionTimeout"))
}

impl<'a> RetryPolicyEvaluator<'a> {
    pub(crate) fn new(policies: &'a [ir::PolicyBracket], exception_name: Option<&'a str>) -> Self {
        Self {
            policies,
            exception_name,
        }
    }

    pub(crate) fn decision(&self, attempt: i32) -> RetryDecision {
        let mut max_retries: i32 = 0;
        let mut matched_policy = false;

        for policy in self.policies {
            let Some(ir::policy_bracket::Kind::Retry(retry)) = policy.kind.as_ref() else {
                continue;
            };
            let matches_exception = if retry.exception_types.is_empty() {
                // Synthetic runtime exceptions (resume/timeout) can represent in-flight
                // work that may still be running out-of-band. Require explicit opt-in
                // exception filters before retrying these cases.
                !is_synthetic_runtime_exception(self.exception_name)
            } else if let Some(name) = self.exception_name {
                retry.exception_types.iter().any(|value| value == name)
            } else {
                false
            };
            if !matches_exception {
                continue;
            }
            matched_policy = true;
            max_retries = max_retries.max(retry.max_retries as i32);
        }

        let should_retry = matched_policy && attempt - 1 < max_retries;

        RetryDecision { should_retry }
    }
}

pub(crate) fn timeout_seconds_from_policies(policies: &[ir::PolicyBracket]) -> Option<u32> {
    let mut timeout_seconds: Option<u64> = None;
    for policy in policies {
        let Some(ir::policy_bracket::Kind::Timeout(timeout)) = policy.kind.as_ref() else {
            continue;
        };
        let seconds = timeout
            .timeout
            .as_ref()
            .map(|duration| duration.seconds)
            .unwrap_or(0);
        if seconds == 0 {
            continue;
        }
        timeout_seconds = Some(match timeout_seconds {
            Some(existing) => existing.min(seconds),
            None => seconds,
        });
    }
    timeout_seconds.map(|seconds| seconds.min(u64::from(u32::MAX)) as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn retry_policy(max_retries: u32, exception_types: Vec<&str>) -> ir::PolicyBracket {
        ir::PolicyBracket {
            kind: Some(ir::policy_bracket::Kind::Retry(ir::RetryPolicy {
                exception_types: exception_types
                    .into_iter()
                    .map(ToString::to_string)
                    .collect(),
                max_retries,
                backoff: None,
            })),
        }
    }

    fn timeout_policy(seconds: u64) -> ir::PolicyBracket {
        ir::PolicyBracket {
            kind: Some(ir::policy_bracket::Kind::Timeout(ir::TimeoutPolicy {
                timeout: Some(ir::Duration { seconds }),
            })),
        }
    }

    #[test]
    fn retry_policy_evaluator_happy_path() {
        let policies = vec![
            retry_policy(1, vec!["ValueError"]),
            retry_policy(3, Vec::new()),
        ];
        let decision = RetryPolicyEvaluator::new(&policies, Some("ValueError")).decision(2);
        assert!(decision.should_retry);

        let exhausted = RetryPolicyEvaluator::new(&policies, Some("ValueError")).decision(4);
        assert!(!exhausted.should_retry);
    }

    #[test]
    fn retry_policy_evaluator_wildcard_does_not_retry_synthetic_timeout() {
        let policies = vec![retry_policy(3, Vec::new())];
        let decision = RetryPolicyEvaluator::new(&policies, Some("ActionTimeout")).decision(1);
        assert!(!decision.should_retry);
    }

    #[test]
    fn retry_policy_evaluator_explicit_timeout_retry_happy_path() {
        let policies = vec![retry_policy(2, vec!["ActionTimeout"])];
        let decision = RetryPolicyEvaluator::new(&policies, Some("ActionTimeout")).decision(1);
        assert!(decision.should_retry);
    }

    #[test]
    fn timeout_seconds_from_policies_happy_path() {
        let policies = vec![timeout_policy(30), timeout_policy(10), timeout_policy(0)];
        assert_eq!(timeout_seconds_from_policies(&policies), Some(10));
    }
}
