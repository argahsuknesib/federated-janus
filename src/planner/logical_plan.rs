/// The fixed logical anomaly query shared by every physical strategy.
#[derive(Debug, Clone, PartialEq)]
pub struct LogicalPlan {
    pub anomaly_multiplier: f64,
}
impl Default for LogicalPlan {
    fn default() -> Self {
        Self {
            anomaly_multiplier: 1.3,
        }
    }
}
