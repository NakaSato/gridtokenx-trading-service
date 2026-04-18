use super::{AuditEvent, AuditLogger};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration, Instant};
use tracing::{error, info, debug};

/// Background worker that consumes audit events from a channel
/// and writes them to the database in high-performance batches.
pub struct AuditWorker {
    pub receiver: mpsc::Receiver<AuditEvent>,
    pub logger: AuditLogger,
}

const BATCH_SIZE: usize = 50;
const BATCH_TIMEOUT: Duration = Duration::from_millis(100);

impl AuditWorker {
    pub fn new(logger: AuditLogger, receiver: mpsc::Receiver<AuditEvent>) -> Self {
        Self { receiver, logger }
    }

    /// Starts the background event processing loop
    pub async fn run(mut self) {
        info!("⏳ Starting Batch-Optimized AuditWorker background loop for Trading Service...");
        
        let mut buffer = Vec::with_capacity(BATCH_SIZE);
        let sleep_timer = sleep(BATCH_TIMEOUT);
        tokio::pin!(sleep_timer);

        loop {
            tokio::select! {
                // Receive event from channel
                maybe_event = self.receiver.recv() => {
                    match maybe_event {
                        Some(event) => {
                            buffer.push(event);
                            
                            // Flush if batch size reached
                            if buffer.len() >= BATCH_SIZE {
                                debug!("AuditWorker: Batch size reached ({}), flushing...", BATCH_SIZE);
                                if let Err(e) = self.logger.log_batch(&buffer).await {
                                    error!("❌ AuditWorker failed to write batch: {}", e);
                                }
                                buffer.clear();
                                sleep_timer.as_mut().reset(Instant::now() + BATCH_TIMEOUT);
                            }
                        }
                        None => {
                            // Channel closed, flush remaining and exit
                            if !buffer.is_empty() {
                                info!("AuditWorker: Channel closed, flushing final {} events...", buffer.len());
                                if let Err(e) = self.logger.log_batch(&buffer).await {
                                    error!("❌ AuditWorker failed to write final batch: {}", e);
                                }
                            }
                            break;
                        }
                    }
                }
                
                // Timeout reached, flush if buffer not empty
                _ = &mut sleep_timer => {
                    if !buffer.is_empty() {
                        debug!("AuditWorker: Timeout reached, flushing {} events...", buffer.len());
                        if let Err(e) = self.logger.log_batch(&buffer).await {
                            error!("❌ AuditWorker failed to write timeout batch: {}", e);
                        }
                        buffer.clear();
                    }
                    sleep_timer.as_mut().reset(Instant::now() + BATCH_TIMEOUT);
                }
            }
        }
        
        info!("🛑 AuditWorker shutting down (channel closed)");
    }
}
