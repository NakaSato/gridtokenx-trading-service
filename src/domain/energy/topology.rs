use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

/// Cached P2P configuration values for calculations
#[derive(Clone, Debug, Default)]
pub struct GridPricingConfig {
    pub wheeling_same_zone: Decimal,
    pub wheeling_adjacent_zone: Decimal,
    pub wheeling_base_charge: Decimal,
    pub wheeling_distance_rate: Decimal,
    pub wheeling_fallback: Decimal,
    pub loss_same_zone: Decimal,
    pub loss_adjacent_zone: Decimal,
    pub loss_base: Decimal,
    pub loss_distance_rate: Decimal,
    pub loss_max: Decimal,
    /// Distance-keyed wheeling rates (distance -> rate)
    pub zone_wheeling: HashMap<i32, Decimal>,
    /// Distance-keyed loss rates (distance -> rate)
    pub zone_loss: HashMap<i32, Decimal>,
    /// Zone-pair specific wheeling overrides: (from_zone, to_zone) -> rate
    /// Checked first before distance-based fallback
    pub zone_pair_wheeling: HashMap<(i32, i32), Decimal>,
    /// Zone-pair specific loss overrides: (from_zone, to_zone) -> rate
    /// Checked first before distance-based fallback
    pub zone_pair_loss: HashMap<(i32, i32), Decimal>,
}

impl GridPricingConfig {
    pub fn with_defaults() -> Self {
        let mut zone_wheeling = HashMap::new();
        zone_wheeling.insert(0, Decimal::from_f64(0.50).unwrap_or_default());
        zone_wheeling.insert(1, Decimal::from_f64(1.00).unwrap_or_default());
        
        let mut zone_loss = HashMap::new();
        zone_loss.insert(0, Decimal::from_f64(0.01).unwrap_or_default());
        zone_loss.insert(1, Decimal::from_f64(0.03).unwrap_or_default());

        Self {
            wheeling_same_zone: Decimal::from_f64(0.50).unwrap_or_default(),
            wheeling_adjacent_zone: Decimal::from_f64(1.00).unwrap_or_default(),
            wheeling_base_charge: Decimal::from_f64(1.50).unwrap_or_default(),
            wheeling_distance_rate: Decimal::from_f64(0.10).unwrap_or_default(),
            wheeling_fallback: Decimal::from_f64(2.00).unwrap_or_default(),
            loss_same_zone: Decimal::from_f64(0.01).unwrap_or_default(),
            loss_adjacent_zone: Decimal::from_f64(0.03).unwrap_or_default(),
            loss_base: Decimal::from_f64(0.03).unwrap_or_default(),
            loss_distance_rate: Decimal::from_f64(0.01).unwrap_or_default(),
            loss_max: Decimal::from_f64(0.15).unwrap_or_default(),
            zone_wheeling,
            zone_loss,
            zone_pair_wheeling: HashMap::new(),
            zone_pair_loss: HashMap::new(),
        }
    }
}

/// Represents a physical transmission branch between zones
#[derive(Clone, Debug)]
pub struct BranchSegment {
    pub from_zone: i32,
    pub to_zone: i32,
    pub capacity_kwh: Decimal,
    pub current_flow_kwh: Decimal,
}

/// Service to manage grid topology and calculate transmission costs
#[derive(Clone, Debug)]
pub struct GridTopologyService {
    /// Maps "zone_a-zone_b" to branch capacity and usage
    branches: Arc<RwLock<HashMap<String, BranchSegment>>>,
    /// Cached pricing configuration (loaded from DB)
    pricing_config: Arc<RwLock<GridPricingConfig>>,
}

impl GridTopologyService {
    pub fn new() -> Self {
        let mut branches = HashMap::new();
        
        // Initialize default constraints (Zone 1 <-> Zone 2 <-> Zone 3 ...)
        // This simulates a linear feeder.
        for i in 1..10 {
            let key = format!("{}-{}", i, i + 1);
            branches.insert(key, BranchSegment {
                from_zone: i,
                to_zone: i + 1,
                capacity_kwh: Decimal::from(1000), // Default 1MWh capacity
                current_flow_kwh: Decimal::ZERO,
            });
        }

        Self {
            branches: Arc::new(RwLock::new(branches)),
            pricing_config: Arc::new(RwLock::new(GridPricingConfig::with_defaults())),
        }
    }

    /// Update pricing configuration from P2PConfigService
    pub async fn update_pricing_config(&self, config: GridPricingConfig) {
        let mut pricing = self.pricing_config.write().await;
        *pricing = config;
    }

    /// Load zone-pair specific overrides from P2P config key-value pairs
    /// Parses keys like `wheeling.zone_1_3` and `loss.zone_2_5` into zone-pair maps
    pub async fn load_zone_pair_overrides(&self, config_values: &HashMap<String, Decimal>) {
        let mut pricing = self.pricing_config.write().await;
        
        for (key, value) in config_values {
            // Parse wheeling.zone_X_Y
            if let Some(zones) = key.strip_prefix("wheeling.zone_") {
                if let Some((from_str, to_str)) = zones.split_once('_') {
                    if let (Ok(from), Ok(to)) = (from_str.parse::<i32>(), to_str.parse::<i32>()) {
                        debug!("Loaded zone-pair wheeling override ({},{}) = {}", from, to, value);
                        pricing.zone_pair_wheeling.insert((from, to), *value);
                    }
                }
            }
            // Parse loss.zone_X_Y
            if let Some(zones) = key.strip_prefix("loss.zone_") {
                if let Some((from_str, to_str)) = zones.split_once('_') {
                    if let (Ok(from), Ok(to)) = (from_str.parse::<i32>(), to_str.parse::<i32>()) {
                        debug!("Loaded zone-pair loss override ({},{}) = {}", from, to, value);
                        pricing.zone_pair_loss.insert((from, to), *value);
                    }
                }
            }
        }
    }

    /// Reset all branch flows (usually at start of matching cycle)
    pub async fn reset_flows(&self) {
        let mut branches = self.branches.write().await;
        for segment in branches.values_mut() {
            segment.current_flow_kwh = Decimal::ZERO;
        }
    }

    /// Check if a trade of `amount` from `src` to `dst` is physically possible
    pub async fn can_accommodate_flow(&self, from_zone: Option<i32>, to_zone: Option<i32>, amount: Decimal) -> bool {
        match (from_zone, to_zone) {
            (Some(sz), Some(dz)) if sz != dz => {
                let branches = self.branches.read().await;
                let path = self.get_path(sz, dz);
                
                for key in path {
                    if let Some(segment) = branches.get(&key) {
                        if segment.current_flow_kwh + amount > segment.capacity_kwh {
                            return false;
                        }
                    } else {
                        // Unknown segment, assume high risk/limited capacity
                        return false; 
                    }
                }
                true
            }
            _ => true, // Local trade or unknown zones (default to true for backward compatibility)
        }
    }

    /// Record a successful flow between zones
    pub async fn record_flow(&self, from_zone: Option<i32>, to_zone: Option<i32>, amount: Decimal) {
        match (from_zone, to_zone) {
            (Some(sz), Some(dz)) if sz != dz => {
                let mut branches = self.branches.write().await;
                let path = self.get_path(sz, dz);
                
                for key in path {
                    if let Some(segment) = branches.get_mut(&key) {
                        segment.current_flow_kwh += amount;
                    }
                }
            }
            _ => {}
        }
    }

    /// Simple path discovery for linear/radial grid
    fn get_path(&self, sz: i32, dz: i32) -> Vec<String> {
        let mut path = Vec::new();
        let (start, end) = if sz < dz { (sz, dz) } else { (dz, sz) };
        
        for i in start..end {
            path.push(format!("{}-{}", i, i + 1));
        }
        path
    }

    /// Calculate wheeling charge (transmission fee) in THB per kWh
    /// Priority: zone-pair override → distance-based → defaults
    pub async fn calculate_wheeling_charge(&self, from_zone: Option<i32>, to_zone: Option<i32>) -> Decimal {
        let config = self.pricing_config.read().await;
        
        match (from_zone, to_zone) {
            (Some(mz), Some(bz)) => {
                // 1. Check zone-pair specific override (both directions)
                if let Some(&charge) = config.zone_pair_wheeling.get(&(mz, bz))
                    .or_else(|| config.zone_pair_wheeling.get(&(bz, mz)))
                {
                    debug!("Zone-pair wheeling override ({},{}): {}", mz, bz, charge);
                    return charge;
                }

                // 2. Distance-based lookup
                if mz == bz {
                    config.wheeling_same_zone
                } else {
                    let distance = (mz - bz).abs();
                    if let Some(&charge) = config.zone_wheeling.get(&distance) {
                        charge
                    } else if distance == 1 {
                        config.wheeling_adjacent_zone
                    } else {
                        let distance_dec = Decimal::from(distance);
                        config.wheeling_base_charge + (config.wheeling_distance_rate * distance_dec)
                    }
                }
            }
            _ => config.wheeling_fallback
        }
    }

    /// Legacy sync version for backward compatibility (uses defaults)
    pub fn calculate_wheeling_charge_sync(&self, from_zone: Option<i32>, to_zone: Option<i32>) -> Decimal {
        let config = GridPricingConfig::with_defaults();
        
        match (from_zone, to_zone) {
            (Some(mz), Some(bz)) => {
                if mz == bz {
                    config.wheeling_same_zone
                } else {
                    let distance = (mz - bz).abs();
                    if let Some(&charge) = config.zone_wheeling.get(&distance) {
                        charge
                    } else if distance == 1 {
                        config.wheeling_adjacent_zone
                    } else {
                        let distance_dec = Decimal::from(distance);
                        config.wheeling_base_charge + (config.wheeling_distance_rate * distance_dec)
                    }
                }
            }
            _ => config.wheeling_fallback
        }
    }

    /// Calculate technical loss (%)
    /// Priority: zone-pair override → distance-based → defaults
    pub async fn calculate_loss_factor(&self, from_zone: Option<i32>, to_zone: Option<i32>) -> Decimal {
        let config = self.pricing_config.read().await;
        
        match (from_zone, to_zone) {
            (Some(mz), Some(bz)) => {
                // 1. Check zone-pair specific override (both directions)
                if let Some(&loss) = config.zone_pair_loss.get(&(mz, bz))
                    .or_else(|| config.zone_pair_loss.get(&(bz, mz)))
                {
                    debug!("Zone-pair loss override ({},{}): {}", mz, bz, loss);
                    return loss.min(config.loss_max);
                }

                // 2. Distance-based lookup
                if mz == bz {
                    config.loss_same_zone
                } else {
                    let distance = (mz - bz).abs();
                    if let Some(&loss) = config.zone_loss.get(&distance) {
                        loss.min(config.loss_max)
                    } else if distance == 1 {
                        config.loss_adjacent_zone
                    } else {
                        let distance_dec = Decimal::from(distance);
                        let loss = config.loss_base + (config.loss_distance_rate * distance_dec);
                        loss.min(config.loss_max)
                    }
                }
            }
            _ => Decimal::from_f64(0.05).unwrap_or_default()
        }
    }

    /// Legacy sync version for backward compatibility (uses defaults)
    pub fn calculate_loss_factor_sync(&self, from_zone: Option<i32>, to_zone: Option<i32>) -> Decimal {
        let config = GridPricingConfig::with_defaults();
        
        match (from_zone, to_zone) {
            (Some(mz), Some(bz)) => {
                if mz == bz {
                    config.loss_same_zone
                } else {
                    let distance = (mz - bz).abs();
                    if let Some(&loss) = config.zone_loss.get(&distance) {
                        loss.min(config.loss_max)
                    } else if distance == 1 {
                        config.loss_adjacent_zone
                    } else {
                        let distance_dec = Decimal::from(distance);
                        let loss = config.loss_base + (config.loss_distance_rate * distance_dec);
                        loss.min(config.loss_max)
                    }
                }
            }
            _ => Decimal::from_f64(0.05).unwrap_or_default()
        }
    }

    /// Calculate actual cost of losses
    pub fn calculate_loss_cost(&self, energy_amount: Decimal, price: Decimal, loss_factor: Decimal) -> Decimal {
        energy_amount * price * loss_factor
    }
}
