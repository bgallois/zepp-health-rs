#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args().nth(1).as_deref() == Some("mcp") {
        let source = match zepp_health_rs::api::ZeppApiClient::from_env() {
            Ok(source) => source,
            Err(zepp_health_rs::api::ApiError::MissingToken) => {
                eprintln!(
                    "ZEPP_TOKEN is not configured; MCP handshake will work, but data requests will fail until credentials are configured."
                );
                zepp_health_rs::api::ZeppApiClient::unconfigured_from_env()?
            }
            Err(error) => return Err(error.into()),
        };
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
