use rt_daemon::{
    manager::{DaemonConfig, DaemonManager},
    server::IpcServer,
};
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let socket_path = std::env::var("RT_DAEMON_SOCKET")
        .unwrap_or_else(|_| "/var/run/robotunnel/rt.sock".to_string());

    let listen_port: u16 = std::env::var("RT_DAEMON_LISTEN_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(11411);

    let insecure = std::env::var("RT_DAEMON_INSECURE")
        .map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(true);

    let config = DaemonConfig {
        listen_port,
        socket_path: socket_path.clone(),
        insecure_allow_any_client: insecure,
        auth_seed: [0u8; 32],
    };

    let manager = Arc::new(DaemonManager::new(config));
    let server = IpcServer::new(socket_path.clone().into(), manager);

    tracing::info!("robotunneld listening on socket={} tunnel_port={}", socket_path, listen_port);

    if let Err(e) = server.run().await {
        tracing::error!("daemon fatal error: {}", e);
        std::process::exit(1);
    }
}
