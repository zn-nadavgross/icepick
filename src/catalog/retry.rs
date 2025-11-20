//! Retry configuration and backoff strategies for catalog operations

use std::time::Duration;

/// Retry configuration for catalog operations
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    max_retries: u32,
    /// Backoff strategy to use between retries
    backoff: BackoffStrategy,
    /// Maximum total time to spend retrying
    max_elapsed_time: Option<Duration>,
}

impl RetryConfig {
    /// Create a new retry configuration
    pub fn new(max_retries: u32, backoff: BackoffStrategy) -> Self {
        Self {
            max_retries,
            backoff,
            max_elapsed_time: None,
        }
    }

    /// Set the maximum elapsed time for retries
    pub fn with_max_elapsed_time(mut self, duration: Duration) -> Self {
        self.max_elapsed_time = Some(duration);
        self
    }

    /// Get the maximum number of retries
    pub fn max_retries(&self) -> u32 {
        self.max_retries
    }

    /// Get the backoff strategy
    pub fn backoff(&self) -> &BackoffStrategy {
        &self.backoff
    }

    /// Get the maximum elapsed time
    pub fn max_elapsed_time(&self) -> Option<Duration> {
        self.max_elapsed_time
    }

    /// Calculate the delay before the next retry attempt
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        self.backoff.delay_for_attempt(attempt)
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            backoff: BackoffStrategy::Exponential {
                initial_delay: Duration::from_millis(100),
                max_delay: Duration::from_secs(30),
                multiplier: 2.0,
            },
            max_elapsed_time: Some(Duration::from_secs(60)),
        }
    }
}

/// Backoff strategy for retries
#[derive(Debug, Clone)]
pub enum BackoffStrategy {
    /// Fixed delay between retries
    Fixed { delay: Duration },
    /// Exponential backoff with jitter
    Exponential {
        initial_delay: Duration,
        max_delay: Duration,
        multiplier: f64,
    },
    /// Linear backoff
    Linear {
        initial_delay: Duration,
        increment: Duration,
        max_delay: Duration,
    },
}

impl BackoffStrategy {
    /// Calculate the delay for a given retry attempt
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        match self {
            BackoffStrategy::Fixed { delay } => *delay,
            BackoffStrategy::Exponential {
                initial_delay,
                max_delay,
                multiplier,
            } => {
                let delay_ms = initial_delay.as_millis() as f64 * multiplier.powi(attempt as i32);
                let delay = Duration::from_millis(delay_ms as u64);
                delay.min(*max_delay)
            }
            BackoffStrategy::Linear {
                initial_delay,
                increment,
                max_delay,
            } => {
                let delay = *initial_delay + *increment * attempt;
                delay.min(*max_delay)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fixed_backoff() {
        let strategy = BackoffStrategy::Fixed {
            delay: Duration::from_millis(100),
        };

        assert_eq!(strategy.delay_for_attempt(0), Duration::from_millis(100));
        assert_eq!(strategy.delay_for_attempt(1), Duration::from_millis(100));
        assert_eq!(strategy.delay_for_attempt(5), Duration::from_millis(100));
    }

    #[test]
    fn test_exponential_backoff() {
        let strategy = BackoffStrategy::Exponential {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            multiplier: 2.0,
        };

        assert_eq!(strategy.delay_for_attempt(0), Duration::from_millis(100));
        assert_eq!(strategy.delay_for_attempt(1), Duration::from_millis(200));
        assert_eq!(strategy.delay_for_attempt(2), Duration::from_millis(400));
        // Should cap at max_delay
        assert_eq!(strategy.delay_for_attempt(20), Duration::from_secs(10));
    }

    #[test]
    fn test_linear_backoff() {
        let strategy = BackoffStrategy::Linear {
            initial_delay: Duration::from_millis(100),
            increment: Duration::from_millis(50),
            max_delay: Duration::from_secs(5),
        };

        assert_eq!(strategy.delay_for_attempt(0), Duration::from_millis(100));
        assert_eq!(strategy.delay_for_attempt(1), Duration::from_millis(150));
        assert_eq!(strategy.delay_for_attempt(2), Duration::from_millis(200));
        // Should cap at max_delay
        assert_eq!(strategy.delay_for_attempt(1000), Duration::from_secs(5));
    }

    #[test]
    fn test_default_retry_config() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries(), 3);
        assert!(config.max_elapsed_time().is_some());
    }

    #[test]
    fn test_retry_config_with_custom_settings() {
        let config = RetryConfig::new(
            5,
            BackoffStrategy::Exponential {
                initial_delay: Duration::from_millis(50),
                max_delay: Duration::from_secs(60),
                multiplier: 3.0,
            },
        )
        .with_max_elapsed_time(Duration::from_secs(300));

        assert_eq!(config.max_retries(), 5);
        assert_eq!(config.max_elapsed_time(), Some(Duration::from_secs(300)));
        assert_eq!(config.delay_for_attempt(0), Duration::from_millis(50));
        assert_eq!(config.delay_for_attempt(1), Duration::from_millis(150));
    }
}
