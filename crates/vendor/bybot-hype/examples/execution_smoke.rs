use bybot_hype::execution_smoke::{run_execution_smoke, SmokeTestConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let confirm_live = std::env::args().any(|argument| argument == "--confirm-live");
    run_execution_smoke(SmokeTestConfig {
        confirm_live,
        ..SmokeTestConfig::default()
    })
    .await
}
