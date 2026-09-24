use clap::Parser;
use federated_janus::remote::{profile, serve};
#[derive(Parser)]
struct Args {
    #[arg(long)]
    archive: String,
    #[arg(long)]
    quads: usize,
    #[arg(long, default_value_t = 100_000)]
    segment_quads: usize,
    #[arg(long, default_value = "127.0.0.1:48123")]
    address: String,
    #[arg(long, default_value = "native-localhost")]
    profile: String,
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let a = Args::parse();
    serve(
        &a.address,
        &a.archive,
        a.quads,
        a.segment_quads,
        profile(&a.profile).ok_or("unknown profile")?,
    )
    .map_err(Into::into)
}
