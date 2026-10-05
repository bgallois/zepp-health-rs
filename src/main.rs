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
        #[cfg(feature = "intervals")]
        let intervals_for_health =
            zepp_health_rs::intervals::IntervalsHealthClient::from_env(&path)?;
        #[cfg(feature = "intervals")]
        let intervals_for_mcp = zepp_health_rs::intervals::IntervalsHealthClient::from_env(&path)?;
        let store = zepp_health_rs::store::Store::open(&path)?;
        let health = zepp_health_rs::health::HealthClient::new(store, source);
        #[cfg(feature = "intervals")]
        let health = match intervals_for_health {
            Some(client) => health.with_external_wellness(std::rc::Rc::new(client)),
            None => health,
        };
        #[cfg(feature = "intervals")]
        zepp_health_rs::mcp::run_stdio(zepp_health_rs::mcp::McpServer::new_with_intervals(
            health,
            intervals_for_mcp,
        ))
        .await?;
        #[cfg(not(feature = "intervals"))]
        zepp_health_rs::mcp::run_stdio(zepp_health_rs::mcp::McpServer::new(health)).await?;
    } else {
        println!(
            "zepp-health-rs provides a library client; run with `mcp` to start the MCP server."
        );
    }
    Ok(())
}
