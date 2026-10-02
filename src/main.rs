#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args().nth(1).as_deref() == Some("mcp") {
        let source = zepp_health_rs::api::ZeppApiClient::from_env()?;
        let path =
            std::env::var("ZEPP_DB_PATH").unwrap_or_else(|_| "zepp-health.sqlite3".to_owned());
        let store = zepp_health_rs::store::Store::open(path)?;
        let health = zepp_health_rs::health::HealthClient::new(store, source);
        zepp_health_rs::mcp::run_stdio(zepp_health_rs::mcp::McpServer::new(health)).await?;
    } else {
        println!(
            "zepp-health-rs provides a library client; run with `mcp` to start the MCP server."
        );
    }
    Ok(())
}
