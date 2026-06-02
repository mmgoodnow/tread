use std::{path::PathBuf, time::Duration};

use clap::{Args, Parser, Subcommand};
use tokio::task::JoinSet;
use tread::{
    Settings, api,
    clients::{
        overseerr::OverseerrClient, prometheus_rtorrent::PrometheusRtorrentClient,
        tautulli::TautulliClient,
    },
    db,
};

#[derive(Debug, Parser)]
#[command(version, about = "Media request lifecycle observability service")]
struct Cli {
    #[arg(long, env = "TREAD_CONFIG")]
    config: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    Serve,
    Configure(ConfigureArgs),
}

#[derive(Debug, Args)]
struct ConfigureArgs {
    #[arg(long, default_value = ".env.local")]
    output: PathBuf,
    #[arg(long)]
    overseerr_url: Option<String>,
    #[arg(long)]
    overseerr_api_key: Option<String>,
    #[arg(long)]
    tautulli_url: Option<String>,
    #[arg(long)]
    tautulli_api_key: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "tread=info,tower_http=info".to_string()),
        )
        .init();

    let cli = Cli::parse();
    match cli.command.unwrap_or(Command::Serve) {
        Command::Serve => serve(Settings::load(cli.config)?).await,
        Command::Configure(args) => configure(args),
    }
}

async fn serve(settings: Settings) -> anyhow::Result<()> {
    ensure_sqlite_parent(&settings.database_url)?;
    let pool = db::connect(&settings.database_url).await?;

    let mut tasks = JoinSet::new();
    if settings.overseerr.enabled {
        if let Some(client) = OverseerrClient::from_settings(&settings.overseerr) {
            let pool = pool.clone();
            let interval = settings.poll_interval();
            tasks.spawn(async move { poll_overseerr(client, pool, interval).await });
        }
    }
    if settings.tautulli.enabled {
        if let Some(client) = TautulliClient::from_settings(&settings.tautulli) {
            let pool = pool.clone();
            let interval = settings.poll_interval();
            tasks.spawn(async move { poll_tautulli(client, pool, interval).await });
        }
    }
    if settings.prometheus.enabled && settings.prometheus.rtorrent_enabled {
        if let Some(client) = PrometheusRtorrentClient::from_settings(&settings.prometheus) {
            let pool = pool.clone();
            let interval = settings.poll_interval();
            tasks.spawn(async move { poll_rtorrent_from_prometheus(client, pool, interval).await });
        }
    }

    let listener = tokio::net::TcpListener::bind(settings.bind_addr).await?;
    tracing::info!(addr = %settings.bind_addr, "listening");
    axum::serve(listener, api::router(pool))
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    tasks.abort_all();
    Ok(())
}

async fn poll_overseerr(client: OverseerrClient, pool: sqlx::SqlitePool, interval: Duration) {
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;
        match client.poll_requests(&pool).await {
            Ok(count) => tracing::debug!(count, "polled overseerr requests"),
            Err(_) => tracing::warn!("overseerr poll failed"),
        }
    }
}

async fn poll_tautulli(client: TautulliClient, pool: sqlx::SqlitePool, interval: Duration) {
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;
        match client.poll_recently_added(&pool).await {
            Ok(count) => tracing::debug!(count, "polled tautulli recently added"),
            Err(_) => tracing::warn!("tautulli poll failed"),
        }
    }
}

async fn poll_rtorrent_from_prometheus(
    client: PrometheusRtorrentClient,
    pool: sqlx::SqlitePool,
    interval: Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;
        match client.poll_torrents(&pool).await {
            Ok(count) => tracing::debug!(count, "polled rtorrent metrics from prometheus"),
            Err(_) => tracing::warn!("rtorrent prometheus poll failed"),
        }
    }
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

fn configure(args: ConfigureArgs) -> anyhow::Result<()> {
    let mut lines = vec![
        "TREAD_BIND_ADDR=0.0.0.0:80".to_string(),
        "TREAD_DATABASE_URL=sqlite:///config/tread.db?mode=rwc".to_string(),
        "TREAD_POLL_INTERVAL_SECONDS=60".to_string(),
    ];

    if let Some(value) = args.overseerr_url {
        lines.push(format!("TREAD_OVERSEERR__BASE_URL={value}"));
        lines.push("TREAD_OVERSEERR__ENABLED=true".to_string());
    }
    if let Some(value) = args.overseerr_api_key {
        lines.push(format!("TREAD_OVERSEERR__API_KEY={value}"));
    }
    if let Some(value) = args.tautulli_url {
        lines.push(format!("TREAD_TAUTULLI__BASE_URL={value}"));
        lines.push("TREAD_TAUTULLI__ENABLED=true".to_string());
    }
    if let Some(value) = args.tautulli_api_key {
        lines.push(format!("TREAD_TAUTULLI__API_KEY={value}"));
    }
    std::fs::write(&args.output, format!("{}\n", lines.join("\n")))?;
    println!("wrote {}", args.output.display());
    Ok(())
}

fn ensure_sqlite_parent(database_url: &str) -> anyhow::Result<()> {
    let Some(path) = database_url.strip_prefix("sqlite://") else {
        return Ok(());
    };
    let path = path.split('?').next().unwrap_or(path);
    if let Some(parent) = std::path::Path::new(path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}
