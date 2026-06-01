use tracing_subscriber::{fmt, prelude::*, EnvFilter};

pub fn init_logging() {
    let rust_log = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    let filter_str = if rust_log.to_lowercase().contains("debug") || rust_log.to_lowercase().contains("trace") {
        rust_log
    } else {
        format!("stric_flow=info,stric_core=warn,{}", rust_log.replace("info", "off"))
    };
    let _ = tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::new(filter_str))
        .try_init();
}
