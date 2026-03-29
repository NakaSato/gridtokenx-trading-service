use chrono::{DateTime, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::Serialize;
// gy_hdl::types::ReadingData;
pub trait ReadingData {
    fn voltage(&self) -> Option<Decimal>;
    fn frequency(&self) -> Option<Decimal>;
    fn battery_level(&self) -> Option<Decimal>;
    fn power_factor(&self) -> Option<Decimal>;
    fn thd_voltage(&self) -> Option<Decimal>;
    fn thd_current(&self) -> Option<Decimal>;
    fn kwh_value(&self) -> f64;
    fn timestamp(&self) -> DateTime<Utc>;
}
use std::fmt;

/// Alert severity levels
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
}

impl fmt::Display for AlertSeverity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlertSeverity::Info => write!(f, "info"),
            AlertSeverity::Warning => write!(f, "warning"),
            AlertSeverity::Critical => write!(f, "critical"),
        }
    }
}

/// Meter alert for abnormal readings
#[derive(Debug, Clone, Serialize)]
pub struct MeterAlert {
    pub meter_id: String,
    pub alert_type: String,
    pub value: Decimal,
    pub threshold: Decimal,
    pub severity: AlertSeverity,
    pub message: String,
    pub timestamp: DateTime<Utc>,
}

/// Check for abnormal readings and generate alerts
pub fn check_alerts<T: ReadingData>(meter_id: &str, data: &T) -> Vec<MeterAlert> {
    let mut alerts = Vec::new();
    let now = Utc::now();

    // Voltage alerts
    if let Some(voltage) = data.voltage() {
        if voltage < Decimal::from(200) {
            alerts.push(MeterAlert {
                meter_id: meter_id.to_string(),
                alert_type: "low_voltage".to_string(),
                value: voltage,
                threshold: Decimal::from(200),
                severity: AlertSeverity::Critical,
                message: format!("Low voltage detected: {:.1}V (threshold: 200V)", voltage),
                timestamp: now,
            });
        } else if voltage > Decimal::from(260) {
            alerts.push(MeterAlert {
                meter_id: meter_id.to_string(),
                alert_type: "high_voltage".to_string(),
                value: voltage,
                threshold: Decimal::from(260),
                severity: AlertSeverity::Critical,
                message: format!("High voltage detected: {:.1}V (threshold: 260V)", voltage),
                timestamp: now,
            });
        }
    }

    // Frequency alerts
    if let Some(frequency) = data.frequency() {
        let low = Decimal::from_parts(495, 0, 0, false, 1); // 49.5
        let high = Decimal::from_parts(505, 0, 0, false, 1); // 50.5
        if frequency < low || frequency > high {
            alerts.push(MeterAlert {
                meter_id: meter_id.to_string(),
                alert_type: "frequency_deviation".to_string(),
                value: frequency,
                threshold: if frequency < low { low } else { high },
                severity: AlertSeverity::Warning,
                message: format!(
                    "Frequency deviation: {:.2}Hz (normal: 49.5-50.5Hz)",
                    frequency
                ),
                timestamp: now,
            });
        }
    }

    // Battery alerts
    if let Some(battery) = data.battery_level() {
        if battery < Decimal::from(20) {
            alerts.push(MeterAlert {
                meter_id: meter_id.to_string(),
                alert_type: "low_battery".to_string(),
                value: battery,
                threshold: Decimal::from(20),
                severity: if battery < Decimal::from(10) {
                    AlertSeverity::Critical
                } else {
                    AlertSeverity::Warning
                },
                message: format!("Low battery: {:.0}%", battery),
                timestamp: now,
            });
        }
    }

    // Power factor alerts
    if let Some(pf) = data.power_factor() {
        let threshold = Decimal::from_parts(8, 0, 0, false, 1); // 0.8
        if pf < threshold {
            alerts.push(MeterAlert {
                meter_id: meter_id.to_string(),
                alert_type: "poor_power_factor".to_string(),
                value: pf,
                threshold,
                severity: AlertSeverity::Warning,
                message: format!("Poor power factor: {:.2} (threshold: 0.8)", pf),
                timestamp: now,
            });
        }
    }

    // THD alerts
    if let Some(thd_v) = data.thd_voltage() {
        let threshold = Decimal::from(5);
        if thd_v > threshold {
            alerts.push(MeterAlert {
                meter_id: meter_id.to_string(),
                alert_type: "high_thd_voltage".to_string(),
                value: thd_v,
                threshold,
                severity: AlertSeverity::Warning,
                message: format!("High THD voltage: {:.1}% (threshold: 5%)", thd_v),
                timestamp: now,
            });
        }
    }

    if let Some(thd_i) = data.thd_current() {
        let threshold = Decimal::from(8);
        if thd_i > threshold {
            alerts.push(MeterAlert {
                meter_id: meter_id.to_string(),
                alert_type: "high_thd_current".to_string(),
                value: thd_i,
                threshold,
                severity: AlertSeverity::Warning,
                message: format!("High THD current: {:.1}% (threshold: 8%)", thd_i),
                timestamp: now,
            });
        }
    }

    alerts
}

/// Calculate health score (0-100) based on electrical parameters
pub fn calculate_health_score<T: ReadingData>(data: &T) -> f64 {
    let mut total_weight = 0.0;
    let mut weighted_score = 0.0;

    // Voltage score (30% weight) - optimal range 220-240V
    if let Some(voltage) = data.voltage() {
        let v_220 = Decimal::from(220);
        let v_240 = Decimal::from(240);
        let v_200 = Decimal::from(200);
        let v_260 = Decimal::from(260);

        let voltage_score = if voltage >= v_220 && voltage <= v_240 {
            100.0
        } else if voltage >= v_200 && voltage <= v_260 {
            let deviation = if voltage < v_220 {
                v_220 - voltage
            } else {
                voltage - v_240
            };
            100.0 - (deviation.to_f64().unwrap_or(0.0) * 5.0).min(50.0)
        } else {
            25.0 // Very poor
        };
        weighted_score += voltage_score * 0.3;
        total_weight += 0.3;
    }

    // Power factor score (30% weight)
    if let Some(pf) = data.power_factor() {
        let pf_score = (pf.to_f64().unwrap_or(0.0) * 100.0).min(100.0);
        weighted_score += pf_score * 0.3;
        total_weight += 0.3;
    }

    // THD score (20% weight) - lower is better
    let thd_total =
        data.thd_voltage().unwrap_or(Decimal::ZERO) + data.thd_current().unwrap_or(Decimal::ZERO);
    if data.thd_voltage().is_some() || data.thd_current().is_some() {
        let thd_score = (100.0 - thd_total.to_f64().unwrap_or(0.0) * 5.0).max(0.0);
        weighted_score += thd_score * 0.2;
        total_weight += 0.2;
    }

    // Battery score (20% weight)
    if let Some(battery) = data.battery_level() {
        weighted_score += battery.to_f64().unwrap_or(0.0) * 0.2;
        total_weight += 0.2;
    }

    // Normalize if not all components available
    if total_weight > 0.0 {
        (weighted_score / total_weight).min(100.0).max(0.0)
    } else {
        50.0 // Default neutral score
    }
}
