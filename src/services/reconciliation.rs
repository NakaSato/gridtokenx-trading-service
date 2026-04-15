//! Financial Reconciliation Service
//!
//! Provides tools for auditing system integrity:
//! - User balance verification against transaction history
//! - Settlement integrity (buyer_paid == seller_earned + fees)
//! - Zero-sum energy and currency validation across the platform

use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde::Serialize;
use sqlx::{PgPool, Row};
use tracing::info;
use utoipa::ToSchema;
use uuid::Uuid;
use metrics::{gauge, histogram};
use std::sync::Arc;

use crate::services::settlement::ImbalanceSettlementService;

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ReconciliationReport {
    pub total_users_checked: i64,
    pub users_with_discrepancies: Vec<UserDiscrepancy>,
    pub total_settlements_checked: i64,
    pub settlement_integrity_failures: Vec<SettlementFailure>,
    pub energy_volume_audit: Vec<EnergyImbalance>,
    pub platform_revenue_summary: RevenueSummary,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct EnergyImbalance {
    pub user_id: Uuid,
    pub metered_generation: Decimal,
    pub metered_consumption: Decimal,
    pub settled_sales_qty: Decimal,
    pub settled_purchases_qty: Decimal,
    pub generation_gap: Decimal,
    pub consumption_gap: Decimal,
    pub is_flagged: bool,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct UserDiscrepancy {
    pub user_id: Uuid,
    pub current_balance: Decimal,
    pub expected_balance: Decimal,
    pub difference: Decimal,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SettlementFailure {
    pub settlement_id: Uuid,
    pub discrepancy_amount: Decimal,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct RevenueSummary {
    pub total_fees: Decimal,
    pub total_wheeling: Decimal,
    pub total_loss_cost: Decimal,
}

#[derive(Clone, Debug)]
pub struct ReconciliationService {
    pool: PgPool,
    imbalance_settlement: Option<Arc<ImbalanceSettlementService>>,
}

impl ReconciliationService {
    pub fn new(pool: PgPool) -> Self {
        Self { pool, imbalance_settlement: None }
    }

    pub fn with_imbalance_settlement(mut self, service: Arc<ImbalanceSettlementService>) -> Self {
        self.imbalance_settlement = Some(service);
        self
    }

    /// Audit all active users to ensure their current balance matches their transactional history.
    /// Expected Balance = Credits (Refunds/Sales) - Debits (Escrows/Fees)
    pub async fn audit_user_balances(&self) -> Result<Vec<UserDiscrepancy>> {
        info!("Running user balance audit...");

        // This is a simplified version. In a production system, we'd use a dedicated ledger table.
        // Here we sum:
        // + Initial Balance (from user registration)
        // + Net matched value from sales
        // - Net matched value from purchases
        // + Escrow refunds from cancelled/expired orders
        // - Active escrow locks

        let start = std::time::Instant::now();
        let rows = sqlx::query(
            r#"
            SELECT 
                u.id, 
                u.balance as current_balance,
                (
                    COALESCE((SELECT SUM(net_amount) FROM settlements WHERE seller_id = u.id AND status = 'confirmed'), 0) -
                    COALESCE((SELECT SUM(total_amount) FROM settlements WHERE buyer_id = u.id AND status = 'confirmed'), 0) +
                    COALESCE((SELECT SUM(amount) FROM escrow_records WHERE user_id = u.id AND status = 'released'), 0) -
                    COALESCE((SELECT SUM(amount) FROM escrow_records WHERE user_id = u.id AND status = 'locked'), 0)
                ) as calculated_balance
            FROM users u
            "#
        )
        .fetch_all(&self.pool)
        .await
        .context("Failed to fetch user balance data for audit")?;

        let duration = start.elapsed();
        histogram!("reconciliation_audit_duration_seconds", "type" => "user_balance").record(duration.as_secs_f64());

        let mut discrepancies = Vec::new();
        for row in rows {
            let user_id: Uuid = row.get("id");
            let current: Decimal = row.get("current_balance");
            // Note: We don't have a reliable 'initial_balance' field in the DB yet,
            // so we're comparing current state vs transactional delta.
            // In the village test, we know prosumers started with 5000.
            let calculated: Decimal = row.get("calculated_balance");

            // For the purpose of this demo/phase, we check if they are "wildly" off.
            // A more robust check would involve the full ledger.
            let diff = current - calculated;
            if diff.abs() > Decimal::from_parts(1, 0, 0, false, 2) {
                // > 0.01 discrepancy
                discrepancies.push(UserDiscrepancy {
                    user_id,
                    current_balance: current,
                    expected_balance: calculated,
                    difference: diff,
                });
            }
        }

        gauge!("reconciliation_discrepancy_count", "type" => "user_balance").set(discrepancies.len() as f64);

        Ok(discrepancies)
    }

    /// Verify that every settlement satisfies the conservation of value:
    /// Buyer_Paid == Seller_Earned + Platform_Fees + Wheeling + Loss_Costs
    pub async fn verify_settlement_integrity(&self) -> Result<Vec<SettlementFailure>> {
        info!("Verifying settlement integrity...");

        let rows = sqlx::query(
            r#"
            SELECT 
                id, total_amount, net_amount, fee_amount, 
                COALESCE(wheeling_charge, 0) as wheeling_charge, 
                COALESCE(loss_cost, 0) as loss_cost
            FROM settlements
            WHERE status != 'failed'
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .context("Failed to fetch settlement data for integrity audit")?;

        let mut failures = Vec::new();
        for row in rows {
            let id: Uuid = row.get("id");
            let total: Decimal = row.get("total_amount"); // What buyer paid
            let net: Decimal = row.get("net_amount"); // What seller got
            let fee: Decimal = row.get("fee_amount");
            let wheeling: Decimal = row.get("wheeling_charge");
            let loss: Decimal = row.get("loss_cost");

            let calculated_input = net + fee + wheeling + loss;
            let diff = total - calculated_input;

            if diff.abs() > Decimal::from_parts(1, 0, 0, false, 4) {
                // > 0.0001
                failures.push(SettlementFailure {
                    settlement_id: id,
                    discrepancy_amount: diff,
                    message: format!("Buyer paid {}, but expected {}", total, calculated_input),
                });
            }
        }

        Ok(failures)
    }

    pub async fn get_revenue_summary(&self) -> Result<RevenueSummary> {
        let row = sqlx::query(
            r#"
            SELECT 
                COALESCE(SUM(amount) FILTER (WHERE revenue_type = 'platform_fee'), 0) as platform_fees,
                COALESCE(SUM(amount) FILTER (WHERE revenue_type = 'wheeling_charge'), 0) as wheeling_charges,
                COALESCE(SUM(amount) FILTER (WHERE revenue_type = 'loss_cost'), 0) as loss_costs
            FROM platform_revenue
            "#
        )
        .fetch_one(&self.pool)
        .await
        .context("Failed to generate revenue summary for report")?;

        Ok(RevenueSummary {
            total_fees: row.get("platform_fees"),
            total_wheeling: row.get("wheeling_charges"),
            total_loss_cost: row.get("loss_costs"),
        })
    }

    /// Verify physical energy volume vs financial settled energy volume.
    pub async fn verify_energy_integrity(&self, hours: i64) -> Result<Vec<EnergyImbalance>> {
        info!("Running energy volume audit (last {} hours)...", hours);

        // 1. Get metered data per user
        let metered_rows = sqlx::query(
            r#"
            SELECT 
                user_id,
                COALESCE(SUM(energy_generated), 0) as total_gen,
                COALESCE(SUM(energy_consumed), 0) as total_con
            FROM meter_readings
            WHERE reading_timestamp >= NOW() - ($1 || ' hours')::INTERVAL
            GROUP BY user_id
            "#
        )
        .bind(hours)
        .fetch_all(&self.pool)
        .await
        .context(format!("Failed to fetch meter readings for energy audit window: {} hours", hours))?;

        // 2. Get settled data per user
        let settled_rows = sqlx::query(
            r#"
            SELECT 
                u.id as user_id,
                COALESCE((SELECT SUM(energy_amount) FROM settlements WHERE seller_id = u.id AND status = 'confirmed' AND created_at >= NOW() - ($1 || ' hours')::INTERVAL), 0) as settled_sales,
                COALESCE((SELECT SUM(energy_amount) FROM settlements WHERE buyer_id = u.id AND status = 'confirmed' AND created_at >= NOW() - ($2 || ' hours')::INTERVAL), 0) as settled_purchases
            FROM users u
            "#
        )
        .bind(hours)
        .bind(hours)
        .fetch_all(&self.pool)
        .await
        .context(format!("Failed to fetch settled volumes for energy audit window: {} hours", hours))?;

        // Map for indexing
        let mut imbalances_map: std::collections::HashMap<Uuid, EnergyImbalance> = std::collections::HashMap::new();

        for row in metered_rows {
            let uid: Uuid = row.get("user_id");
            let gen: Decimal = row.get("total_gen");
            let con: Decimal = row.get("total_con");
            imbalances_map.insert(uid, EnergyImbalance {
                user_id: uid,
                metered_generation: gen,
                metered_consumption: con,
                settled_sales_qty: Decimal::ZERO,
                settled_purchases_qty: Decimal::ZERO,
                generation_gap: Decimal::ZERO,
                consumption_gap: Decimal::ZERO,
                is_flagged: false,
            });
        }

        for row in settled_rows {
            let uid: Uuid = row.get("user_id");
            let sales: Decimal = row.get("settled_sales");
            let purchases: Decimal = row.get("settled_purchases");
            
            let imb = imbalances_map.entry(uid).or_insert(EnergyImbalance {
                user_id: uid,
                metered_generation: Decimal::ZERO,
                metered_consumption: Decimal::ZERO,
                settled_sales_qty: Decimal::ZERO,
                settled_purchases_qty: Decimal::ZERO,
                generation_gap: Decimal::ZERO,
                consumption_gap: Decimal::ZERO,
                is_flagged: false,
            });
            imb.settled_sales_qty = sales;
            imb.settled_purchases_qty = purchases;
        }

        let mut results = Vec::new();
        let tolerance = Decimal::from_parts(5, 0, 0, false, 2); // 5%

        for (_, mut imb) in imbalances_map {
            // Gap calculation: Actual Metered - Financial Settled
            imb.generation_gap = imb.metered_generation - imb.settled_sales_qty;
            imb.consumption_gap = imb.metered_consumption - imb.settled_purchases_qty;

            // Flag if disagreement > 5% of largest value
            let mut flagged = false;
            
            if imb.metered_generation > Decimal::ZERO || imb.settled_sales_qty > Decimal::ZERO {
                let max_gen = imb.metered_generation.max(imb.settled_sales_qty);
                if imb.generation_gap.abs() > max_gen * tolerance {
                    flagged = true;
                }
            }
            
            if imb.metered_consumption > Decimal::ZERO || imb.settled_purchases_qty > Decimal::ZERO {
                let max_con = imb.metered_consumption.max(imb.settled_purchases_qty);
                if imb.consumption_gap.abs() > max_con * tolerance {
                    flagged = true;
                }
            }

            imb.is_flagged = flagged;

            // Only report if there is any activity or flag
            if flagged || imb.metered_generation > Decimal::ZERO || imb.metered_consumption > Decimal::ZERO || 
               imb.settled_sales_qty > Decimal::ZERO || imb.settled_purchases_qty > Decimal::ZERO {
                results.push(imb);
            }
        }

        Ok(results)
    }

    /// Generate full reconciliation report
    pub async fn generate_report(&self) -> Result<ReconciliationReport> {
        let user_discrepancies = self.audit_user_balances().await?;
        let settlement_failures = self.verify_settlement_integrity().await?;
        let energy_audit = self.verify_energy_integrity(24).await?; // 24h audit window
        let revenue_summary = self.get_revenue_summary().await?;

        let total_users: i64 = sqlx::query("SELECT COUNT(*) FROM users")
            .fetch_one(&self.pool)
            .await
            .context("Failed to count total users for report")?
            .get(0);

        let total_settlements: i64 = sqlx::query("SELECT COUNT(*) FROM settlements")
            .fetch_one(&self.pool)
            .await
            .context("Failed to count total settlements for report")?
            .get(0);

        Ok(ReconciliationReport {
            total_users_checked: total_users,
            users_with_discrepancies: user_discrepancies,
            total_settlements_checked: total_settlements,
            settlement_integrity_failures: settlement_failures,
            energy_volume_audit: energy_audit,
            platform_revenue_summary: revenue_summary,
            timestamp: chrono::Utc::now(),
        })
    }

    /// Perform financial settlement of detected imbalances for a given epoch.
    pub async fn perform_imbalance_settlement(&self, epoch_id: Uuid) -> Result<usize> {
        let scribe: &Arc<ImbalanceSettlementService> = match &self.imbalance_settlement {
            Some(s) => s,
            None => anyhow::bail!("ImbalanceSettlementService not configured for this reconciliation instance"),
        };

        info!("Starting automated imbalance settlement for epoch {}...", epoch_id);
        
        // 1. Run the audit for the last 24h (or appropriate window for the epoch)
        let imbalances = self.verify_energy_integrity(24).await?;
        
        // 2. Filter for significant imbalances that need settlement
        let actionable_imbalances: Vec<EnergyImbalance> = imbalances
            .into_iter()
            .filter(|i| i.is_flagged || !i.generation_gap.is_zero() || !i.consumption_gap.is_zero())
            .collect();

        if actionable_imbalances.is_empty() {
            info!("No actionable energy imbalances found for epoch {}", epoch_id);
            return Ok(0);
        }

        // 3. Trigger the settlement service
        let count = scribe.settle_imbalances(actionable_imbalances, epoch_id).await?;
        
        info!("Successfully created {} correction settlements for epoch {}", count, epoch_id);
        Ok(count)
    }
}
