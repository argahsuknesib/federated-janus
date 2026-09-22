use clap::{Parser, ValueEnum};
use federated_janus::{
    execute, ExecutionStrategy, InMemoryHistoricalSource, InMemoryLiveSource, LiveSource,
    LogicalPlan, Observation,
};
use std::{fs, path::PathBuf};
#[derive(Clone, ValueEnum)]
enum StrategyArg {
    FetchAll,
    AggregatePushdown,
    BindJoin,
}
impl From<StrategyArg> for ExecutionStrategy {
    fn from(v: StrategyArg) -> Self {
        match v {
            StrategyArg::FetchAll => Self::FetchAll,
            StrategyArg::AggregatePushdown => Self::AggregatePushdown,
            StrategyArg::BindJoin => Self::BindJoin,
        }
    }
}
#[derive(Parser)]
struct Args {
    #[arg(long, value_enum, default_value = "fetch-all")]
    strategy: StrategyArg,
    #[arg(long, default_value_t = 100)]
    live_sensors: usize,
    #[arg(long, default_value_t = 1000)]
    historical_sensors: usize,
    /// Optional total cap; otherwise all sensors get `historical_observations_per_sensor` values.
    #[arg(long)]
    number_of_historical_observations: Option<usize>,
    #[arg(long, default_value_t = 10)]
    historical_observations_per_sensor: usize,
    #[arg(long, default_value_t = 1)]
    live_observations_per_sensor: usize,
    #[arg(long, default_value_t = 0.1)]
    anomaly_fraction: f64,
    #[arg(long, default_value_t = 7)]
    random_seed: u64,
    #[arg(long)]
    csv: Option<PathBuf>,
}
fn data(a: &Args) -> (InMemoryLiveSource, InMemoryHistoricalSource) {
    let mut h = Vec::new();
    for s in 0..a.historical_sensors {
        for i in 0..a.historical_observations_per_sensor {
            h.push(Observation::new(
                i as u64,
                format!("https://example.org/sensor{s}"),
                100.0 + (i % 5) as f64,
            ));
            if a.number_of_historical_observations
                .is_some_and(|cap| h.len() >= cap)
            {
                break;
            }
        }
        if a.number_of_historical_observations
            .is_some_and(|cap| h.len() >= cap)
        {
            break;
        }
    }
    let mut l = Vec::new();
    for s in 0..a.live_sensors.min(a.historical_sensors) {
        for i in 0..a.live_observations_per_sensor {
            let anomaly = (s as f64 / a.live_sensors.max(1) as f64) < a.anomaly_fraction;
            l.push(Observation::new(
                1_000_000 + i as u64,
                format!("https://example.org/sensor{s}"),
                if anomaly { 200.0 } else { 100.0 },
            ));
        }
    }
    (InMemoryLiveSource::new(l), InMemoryHistoricalSource::new(h))
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let a = Args::parse();
    let (l, h) = data(&a);
    let out = execute(a.strategy.into(), &LogicalPlan::default(), &l, &h)?;
    println!(
        "strategy={} results={} bytes={}",
        out.metrics.strategy.as_str(),
        out.results.len(),
        out.metrics.bytes_transferred
    );
    if let Some(p) = a.csv {
        fs::write(
            p,
            format!(
                "{}\n{}\n",
                federated_janus::metrics::ExecutionMetrics::csv_header(),
                out.metrics.csv_row(l.materialize_live_window().len())
            ),
        )?;
    }
    Ok(())
}
