use crate::messages::proto;

/// Default exponential multiplier when not specified
pub const DEFAULT_EXPONENTIAL_MULTIPLIER: f64 = 2.0;

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum BackoffConfig {
    /// No delay between retries (immediate retry)
    #[default]
    None,
    /// Linear backoff: delay = base_delay_ms * attempt_number
    Linear { base_delay_ms: i32 },
    /// Exponential backoff: delay = base_delay_ms * multiplier^(attempt_number - 1)
    Exponential { base_delay_ms: i32, multiplier: f64 },
}

impl BackoffConfig {
    pub fn from_proto(policy: Option<&proto::BackoffPolicy>) -> Self {
        let Some(policy) = policy else {
            return Self::default();
        };
        match &policy.policy {
            Some(proto::backoff_policy::Policy::Linear(linear)) => Self::Linear {
                base_delay_ms: linear.base_delay_ms as i32,
            },
            Some(proto::backoff_policy::Policy::Exponential(exp)) => Self::Exponential {
                base_delay_ms: exp.base_delay_ms as i32,
                multiplier: if exp.multiplier > 0.0 {
                    exp.multiplier
                } else {
                    DEFAULT_EXPONENTIAL_MULTIPLIER
                },
            },
            None => Self::default(),
        }
    }

    pub fn kind_str(&self) -> &'static str {
        match self {
            BackoffConfig::None => "none",
            BackoffConfig::Linear { .. } => "linear",
            BackoffConfig::Exponential { .. } => "exponential",
        }
    }

    pub fn base_delay_ms(&self) -> i32 {
        match self {
            BackoffConfig::None => 0,
            BackoffConfig::Linear { base_delay_ms } => *base_delay_ms,
            BackoffConfig::Exponential { base_delay_ms, .. } => *base_delay_ms,
        }
    }

    pub fn multiplier(&self) -> f64 {
        match self {
            BackoffConfig::Exponential { multiplier, .. } => *multiplier,
            _ => DEFAULT_EXPONENTIAL_MULTIPLIER,
        }
    }

    pub fn calculate_delay_ms(&self, attempt_number: i32) -> i64 {
        if attempt_number <= 0 {
            return 0;
        }
        match self {
            BackoffConfig::None => 0,
            BackoffConfig::Linear { base_delay_ms } => {
                if *base_delay_ms <= 0 {
                    return 0;
                }
                (*base_delay_ms as i64) * (attempt_number as i64)
            }
            BackoffConfig::Exponential {
                base_delay_ms,
                multiplier,
            } => {
                if *base_delay_ms <= 0 {
                    return 0;
                }
                // delay = base_delay * multiplier^(attempt - 1)
                let exp = (attempt_number - 1) as f64;
                let factor = multiplier.powf(exp);
                ((*base_delay_ms as f64) * factor) as i64
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_config_from_proto_handles_none() {
        let config = BackoffConfig::from_proto(None);
        assert_eq!(config, BackoffConfig::None);
        assert_eq!(config.kind_str(), "none");
        assert_eq!(config.base_delay_ms(), 0);
    }

    #[test]
    fn backoff_config_from_proto_handles_linear() {
        let policy = proto::BackoffPolicy {
            policy: Some(proto::backoff_policy::Policy::Linear(
                proto::LinearBackoff {
                    base_delay_ms: 1000,
                },
            )),
        };
        let config = BackoffConfig::from_proto(Some(&policy));
        assert!(matches!(
            config,
            BackoffConfig::Linear {
                base_delay_ms: 1000
            }
        ));
        assert_eq!(config.kind_str(), "linear");
        assert_eq!(config.base_delay_ms(), 1000);
    }

    #[test]
    fn backoff_config_from_proto_handles_exponential() {
        let policy = proto::BackoffPolicy {
            policy: Some(proto::backoff_policy::Policy::Exponential(
                proto::ExponentialBackoff {
                    base_delay_ms: 500,
                    multiplier: 3.0,
                },
            )),
        };
        let config = BackoffConfig::from_proto(Some(&policy));
        assert!(
            matches!(config, BackoffConfig::Exponential { base_delay_ms: 500, multiplier } if (multiplier - 3.0).abs() < f64::EPSILON)
        );
        assert_eq!(config.kind_str(), "exponential");
        assert_eq!(config.base_delay_ms(), 500);
        assert!((config.multiplier() - 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn backoff_config_from_proto_uses_default_multiplier() {
        let policy = proto::BackoffPolicy {
            policy: Some(proto::backoff_policy::Policy::Exponential(
                proto::ExponentialBackoff {
                    base_delay_ms: 500,
                    multiplier: 0.0, // zero means use default
                },
            )),
        };
        let config = BackoffConfig::from_proto(Some(&policy));
        assert!((config.multiplier() - DEFAULT_EXPONENTIAL_MULTIPLIER).abs() < f64::EPSILON);
    }

    #[test]
    fn backoff_config_calculate_delay_uses_kind() {
        let linear = BackoffConfig::Linear {
            base_delay_ms: 1000,
        };
        assert_eq!(linear.calculate_delay_ms(3), 3000);

        let exponential = BackoffConfig::Exponential {
            base_delay_ms: 1000,
            multiplier: 2.0,
        };
        assert_eq!(exponential.calculate_delay_ms(3), 4000); // 1000 * 2^2
    }

    #[test]
    fn backoff_kind_str_returns_correct_values() {
        assert_eq!(BackoffConfig::None.kind_str(), "none");
        assert_eq!(
            BackoffConfig::Linear { base_delay_ms: 100 }.kind_str(),
            "linear"
        );
        assert_eq!(
            BackoffConfig::Exponential {
                base_delay_ms: 100,
                multiplier: 2.0
            }
            .kind_str(),
            "exponential"
        );
    }

    #[test]
    fn backoff_none_always_returns_zero_delay() {
        let config = BackoffConfig::None;
        assert_eq!(config.calculate_delay_ms(0), 0);
        assert_eq!(config.calculate_delay_ms(1), 0);
        assert_eq!(config.calculate_delay_ms(10), 0);
    }

    #[test]
    fn backoff_linear_calculates_correctly() {
        // delay = base_delay * attempt_number
        let config = BackoffConfig::Linear {
            base_delay_ms: 1000,
        };
        assert_eq!(config.calculate_delay_ms(0), 0);
        assert_eq!(config.calculate_delay_ms(1), 1000);
        assert_eq!(config.calculate_delay_ms(2), 2000);
        assert_eq!(config.calculate_delay_ms(5), 5000);

        let config2 = BackoffConfig::Linear { base_delay_ms: 500 };
        assert_eq!(config2.calculate_delay_ms(3), 1500);
    }

    #[test]
    fn backoff_exponential_calculates_correctly() {
        // delay = base_delay * multiplier^(attempt - 1)
        let config = BackoffConfig::Exponential {
            base_delay_ms: 1000,
            multiplier: 2.0,
        };
        assert_eq!(config.calculate_delay_ms(0), 0);
        assert_eq!(config.calculate_delay_ms(1), 1000); // 1000 * 2^0
        assert_eq!(config.calculate_delay_ms(2), 2000); // 1000 * 2^1
        assert_eq!(config.calculate_delay_ms(3), 4000); // 1000 * 2^2
        assert_eq!(config.calculate_delay_ms(4), 8000); // 1000 * 2^3

        let config2 = BackoffConfig::Exponential {
            base_delay_ms: 500,
            multiplier: 2.0,
        };
        assert_eq!(config2.calculate_delay_ms(5), 8000); // 500 * 2^4
    }

    #[test]
    fn backoff_exponential_with_custom_multiplier() {
        // delay = base_delay * multiplier^(attempt - 1)
        let config = BackoffConfig::Exponential {
            base_delay_ms: 100,
            multiplier: 3.0,
        };
        assert_eq!(config.calculate_delay_ms(1), 100); // 100 * 3^0 = 100
        assert_eq!(config.calculate_delay_ms(2), 300); // 100 * 3^1 = 300
        assert_eq!(config.calculate_delay_ms(3), 900); // 100 * 3^2 = 900
        assert_eq!(config.calculate_delay_ms(4), 2700); // 100 * 3^3 = 2700
    }

    #[test]
    fn backoff_handles_zero_base_delay() {
        let linear = BackoffConfig::Linear { base_delay_ms: 0 };
        assert_eq!(linear.calculate_delay_ms(5), 0);

        let exponential = BackoffConfig::Exponential {
            base_delay_ms: 0,
            multiplier: 2.0,
        };
        assert_eq!(exponential.calculate_delay_ms(5), 0);
    }

    #[test]
    fn backoff_exponential_handles_large_attempts() {
        // Test with high attempt numbers to ensure no overflow
        let config = BackoffConfig::Exponential {
            base_delay_ms: 1,
            multiplier: 2.0,
        };
        let delay_30 = config.calculate_delay_ms(31); // 2^30
        let delay_31 = config.calculate_delay_ms(32); // 2^31
        // Should handle large exponents gracefully
        assert!(delay_30 > 0);
        assert!(delay_31 > delay_30);
    }
}
