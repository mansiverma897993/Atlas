//! # telemetry
//!
//! One-call observability bootstrap for every service (ADR-0012). Sets up the three pillars:
//!
//! * **Logs** — structured JSON (or pretty locally) via `tracing-subscriber`, with an
//!   `EnvFilter` level. Every span/line carries the fields the service attaches
//!   (`correlation_id`, `request_id`, `trace_id`, …).
//! * **Traces** — when built with the `otel` feature and enabled in config, a
//!   `tracing-opentelemetry` layer exports spans over OTLP to the collector.
//! * **Metrics** — a Prometheus recorder ([`metrics_exporter_prometheus`]) whose handle
//!   renders the `/metrics` exposition the service serves on its metrics port.
//!
//! Call [`init`] once at the top of `main`, and [`shutdown`] on graceful exit to flush
//! exporters.

use config::AppConfig;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use thiserror::Error;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

/// Errors from initializing telemetry.
#[derive(Debug, Error)]
pub enum TelemetryError {
    /// The tracing subscriber was already set.
    #[error("tracing subscriber already initialized: {0}")]
    Subscriber(String),
    /// The metrics recorder could not be installed.
    #[error("failed to install metrics recorder: {0}")]
    Metrics(String),
    /// The OTLP exporter pipeline failed to build.
    #[error("failed to build OTLP pipeline: {0}")]
    Otlp(String),
}

/// Handle returned by [`init`]; keep it alive for the process lifetime.
pub struct Telemetry {
    /// Renders Prometheus text exposition for the `/metrics` endpoint.
    pub prometheus: PrometheusHandle,
}

/// Initialize logging, tracing, and metrics from config. Call once in `main`.
pub fn init(config: &AppConfig) -> Result<Telemetry, TelemetryError> {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.log.level));

    // ---- metrics recorder (Prometheus) ----
    let prometheus = PrometheusBuilder::new()
        .install_recorder()
        .map_err(|e| TelemetryError::Metrics(e.to_string()))?;

    // ---- logging + tracing layers ----
    let registry = tracing_subscriber::registry().with(env_filter);

    // fmt layer: json in prod, pretty locally
    let fmt_layer = if config.log.format == "json" {
        tracing_subscriber::fmt::layer()
            .json()
            .with_current_span(true)
            .with_span_list(false)
            .boxed()
    } else {
        tracing_subscriber::fmt::layer().pretty().boxed()
    };

    #[cfg(feature = "otel")]
    {
        if config.otel.enabled {
            let otel_layer = otel::build_tracer_layer(config)?;
            registry.with(fmt_layer).with(otel_layer).init();
        } else {
            registry.with(fmt_layer).init();
        }
    }
    #[cfg(not(feature = "otel"))]
    {
        registry.with(fmt_layer).init();
    }

    tracing::info!(
        service = %config.otel.service_name,
        otel_enabled = config.otel.enabled,
        "telemetry initialized"
    );

    Ok(Telemetry { prometheus })
}

/// Flush and shut down exporters. Call on graceful shutdown.
pub fn shutdown() {
    #[cfg(feature = "otel")]
    {
        opentelemetry::global::shutdown_tracer_provider();
    }
}

use tracing_subscriber::Layer;

#[cfg(feature = "otel")]
mod otel {
    use super::{AppConfig, TelemetryError};
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry::KeyValue;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::{trace, Resource};
    use tracing_subscriber::Layer;

    /// Build the OpenTelemetry → OTLP tracing layer for the registry.
    pub fn build_tracer_layer<S>(
        config: &AppConfig,
    ) -> Result<Box<dyn Layer<S> + Send + Sync>, TelemetryError>
    where
        S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a> + Send + Sync,
    {
        let exporter = opentelemetry_otlp::new_exporter()
            .tonic()
            .with_endpoint(config.otel.endpoint.clone());

        let resource = Resource::new(vec![KeyValue::new(
            opentelemetry_semantic_conventions::resource::SERVICE_NAME,
            config.otel.service_name.clone(),
        )]);

        let provider = opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(exporter)
            .with_trace_config(
                trace::Config::default()
                    .with_sampler(trace::Sampler::TraceIdRatioBased(config.otel.sample_ratio))
                    .with_resource(resource),
            )
            .install_batch(opentelemetry_sdk::runtime::Tokio)
            .map_err(|e| TelemetryError::Otlp(e.to_string()))?;

        // Acquire a named tracer from the provider, then register the provider globally so
        // context propagation (gRPC/Kafka headers) resolves against the same pipeline.
        let tracer = provider.tracer(config.otel.service_name.clone());
        opentelemetry::global::set_tracer_provider(provider);

        Ok(tracing_opentelemetry::layer().with_tracer(tracer).boxed())
    }
}
