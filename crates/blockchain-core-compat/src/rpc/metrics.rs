/// Trait for tracking blockchain operations
pub trait BlockchainMetrics: Send + Sync {
    fn track_operation(&self, operation: &str, duration_ms: f64, success: bool);
    fn track_rpc_error(&self, operation: &str, code: i64);
    fn track_transaction_retry(&self, attempt: u32, success: bool);
}

/// Prometheus implementation of metrics
pub struct PrometheusMetrics;

impl BlockchainMetrics for PrometheusMetrics {
    fn track_operation(&self, operation: &str, duration_ms: f64, success: bool) {
        metrics::counter!("blockchain_operations_total", "operation" => operation.to_string(), "success" => success.to_string()).increment(1);
        metrics::histogram!("blockchain_operation_duration_ms", "operation" => operation.to_string()).record(duration_ms);
    }

    fn track_rpc_error(&self, operation: &str, code: i64) {
        metrics::counter!("blockchain_rpc_errors_total", "operation" => operation.to_string(), "code" => code.to_string()).increment(1);
    }

    fn track_transaction_retry(&self, attempt: u32, success: bool) {
        metrics::counter!("blockchain_transaction_retries_total", "attempt" => attempt.to_string(), "success" => success.to_string()).increment(1);
    }
}

/// No-op implementation of metrics
pub struct NoopMetrics;

impl BlockchainMetrics for NoopMetrics {
    fn track_operation(&self, _operation: &str, _duration_ms: f64, _success: bool) {}
    fn track_rpc_error(&self, _operation: &str, _code: i64) {}
    fn track_transaction_retry(&self, _attempt: u32, _success: bool) {}
}
