//! # config
//!
//! Layered, strongly-typed configuration with **fail-fast** semantics. Precedence, low to
//! high (later overrides earlier):
//!
//! 1. Built-in [`Defaults`] baked into the binary.
//! 2. `config/{RUN_ENV}.toml` (e.g. `config/production.toml`), if present.
//! 3. Environment variables prefixed `APP`, nested with `__`
//!    (e.g. `APP__DATABASE__URL`).
//!
//! The merged result is parsed into a typed [`AppConfig`] and **validated at startup**; a
//! malformed or incomplete configuration aborts boot rather than failing at first request
//! (twelve-factor, ADR-aligned). Each service picks the sub-structs it needs.

use std::time::Duration;

use figment::providers::{Env, Format, Serialized, Toml};
use figment::Figment;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Errors from loading or validating configuration.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// The layered sources could not be merged/parsed.
    #[error("failed to load configuration: {0}")]
    Load(#[from] figment::Error),

    /// A semantic validation rule failed (value present but invalid).
    #[error("invalid configuration: {0}")]
    Invalid(String),
}

/// Top-level configuration. Services deserialize the whole thing and use the parts relevant
/// to them (e.g. `worker` ignores `server.grpc_addr`).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AppConfig {
    /// Network binding.
    #[serde(default)]
    pub server: ServerConfig,
    /// PostgreSQL connection.
    #[serde(default)]
    pub database: DatabaseConfig,
    /// Redis connection.
    #[serde(default)]
    pub redis: RedisConfig,
    /// Kafka/Redpanda connection.
    #[serde(default)]
    pub kafka: KafkaConfig,
    /// OpenTelemetry export.
    #[serde(default)]
    pub otel: OtelConfig,
    /// JWT / token settings.
    #[serde(default)]
    pub jwt: JwtConfig,
    /// Logging.
    #[serde(default)]
    pub log: LogConfig,
}

/// Network binding for a service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Public/admin HTTP bind address (`0.0.0.0:8080`).
    pub http_addr: String,
    /// gRPC bind address (services that expose gRPC).
    pub grpc_addr: String,
    /// Prometheus metrics bind address.
    pub metrics_addr: String,
    /// Deadline for draining in-flight work on shutdown.
    pub shutdown_grace_seconds: u64,
}

/// PostgreSQL pool configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// Connection URL (`postgres://user:pass@host:5432/db`).
    pub url: String,
    /// Maximum pool size.
    pub max_connections: u32,
    /// Minimum idle connections kept warm.
    pub min_connections: u32,
    /// Per-acquire timeout.
    pub acquire_timeout_seconds: u64,
}

/// Redis configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisConfig {
    /// Connection URL (`redis://host:6379`).
    pub url: String,
    /// Default TTL applied to caches that don't set one explicitly.
    pub default_ttl_seconds: u64,
}

/// Kafka/Redpanda configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConfig {
    /// Comma-separated broker list (`redpanda:9092`).
    pub brokers: String,
    /// Consumer group id for this service.
    pub consumer_group: String,
    /// Max delivery attempts before a message is routed to the DLQ.
    pub max_delivery_attempts: u32,
}

/// OpenTelemetry configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtelConfig {
    /// OTLP collector endpoint (`http://otel-collector:4317`).
    pub endpoint: String,
    /// Logical service name reported in traces/metrics.
    pub service_name: String,
    /// Trace sampling ratio in `[0.0, 1.0]`.
    pub sample_ratio: f64,
    /// Whether OTLP export is enabled (off => local-only tracing).
    pub enabled: bool,
}

/// JWT / token configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwtConfig {
    /// Token issuer (`iss` claim).
    pub issuer: String,
    /// Expected audience (`aud` claim).
    pub audience: String,
    /// Access-token lifetime.
    pub access_ttl_seconds: u64,
    /// Refresh-token lifetime.
    pub refresh_ttl_seconds: u64,
    /// JWKS endpoint used by verifiers to fetch public keys.
    pub jwks_url: String,
}

/// Logging configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogConfig {
    /// Level filter (`info`, `debug`, or an `env_filter` directive).
    pub level: String,
    /// `json` (production) or `pretty` (local).
    pub format: String,
}

impl DatabaseConfig {
    /// Pool acquire timeout as a [`Duration`].
    #[must_use]
    pub fn acquire_timeout(&self) -> Duration {
        Duration::from_secs(self.acquire_timeout_seconds)
    }
}

impl ServerConfig {
    /// Shutdown grace period as a [`Duration`].
    #[must_use]
    pub fn shutdown_grace(&self) -> Duration {
        Duration::from_secs(self.shutdown_grace_seconds)
    }
}

// ---- Defaults (layer 1) ----

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            http_addr: "0.0.0.0:8080".into(),
            grpc_addr: "0.0.0.0:50051".into(),
            metrics_addr: "0.0.0.0:9100".into(),
            shutdown_grace_seconds: 30,
        }
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: "postgres://app:app@localhost:5432/postgres".into(),
            max_connections: 20,
            min_connections: 2,
            acquire_timeout_seconds: 10,
        }
    }
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            url: "redis://localhost:6379".into(),
            default_ttl_seconds: 300,
        }
    }
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            brokers: "localhost:9092".into(),
            consumer_group: "default".into(),
            max_delivery_attempts: 5,
        }
    }
}

impl Default for OtelConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:4317".into(),
            service_name: "service".into(),
            sample_ratio: 1.0,
            enabled: false,
        }
    }
}

impl Default for JwtConfig {
    fn default() -> Self {
        Self {
            issuer: "https://identity.local".into(),
            audience: "ledger-platform".into(),
            access_ttl_seconds: 900,
            refresh_ttl_seconds: 2_592_000,
            jwks_url: "http://identity:8081/.well-known/jwks.json".into(),
        }
    }
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: "info".into(),
            format: "json".into(),
        }
    }
}

impl AppConfig {
    /// Load and validate the configuration from all layers.
    ///
    /// `RUN_ENV` (default `local`) selects the optional `config/{RUN_ENV}.toml` file.
    pub fn load() -> Result<Self, ConfigError> {
        let run_env = std::env::var("RUN_ENV").unwrap_or_else(|_| "local".into());
        Self::load_from(&format!("config/{run_env}.toml"))
    }

    /// Load with an explicit config-file path (used in tests).
    pub fn load_from(file: &str) -> Result<Self, ConfigError> {
        let cfg: AppConfig = Figment::new()
            // layer 1: struct defaults
            .merge(Serialized::defaults(AppConfig::default()))
            // layer 2: file (optional)
            .merge(Toml::file(file))
            // layer 3: environment (APP__SECTION__KEY)
            .merge(Env::prefixed("APP__").split("__"))
            .extract()?;
        let run_env = std::env::var("RUN_ENV").unwrap_or_else(|_| "local".into());
        cfg.validate(&run_env)?;
        Ok(cfg)
    }

    /// Semantic validation beyond type-checking. `run_env` gates the stricter production rules.
    fn validate(&self, run_env: &str) -> Result<(), ConfigError> {
        if self.database.max_connections < self.database.min_connections {
            return Err(ConfigError::Invalid(
                "database.max_connections must be >= min_connections".into(),
            ));
        }
        if !(0.0..=1.0).contains(&self.otel.sample_ratio) {
            return Err(ConfigError::Invalid(
                "otel.sample_ratio must be within [0.0, 1.0]".into(),
            ));
        }
        if self.jwt.access_ttl_seconds == 0 {
            return Err(ConfigError::Invalid(
                "jwt.access_ttl_seconds must be > 0".into(),
            ));
        }
        if !matches!(self.log.format.as_str(), "json" | "pretty") {
            return Err(ConfigError::Invalid(
                "log.format must be 'json' or 'pretty'".into(),
            ));
        }
        // In production, refuse to boot on the built-in local placeholders. Without this a
        // misconfigured deploy silently targets a localhost database with the weak `app:app`
        // credentials (the struct defaults) instead of failing fast.
        if run_env == "production" {
            self.validate_production()?;
        }
        Ok(())
    }

    /// Extra validation applied only when `RUN_ENV=production`: the insecure local defaults must
    /// have been overridden by real secrets/config.
    fn validate_production(&self) -> Result<(), ConfigError> {
        let url = &self.database.url;
        if url.contains("localhost") || url.contains("127.0.0.1") {
            return Err(ConfigError::Invalid(
                "database.url points at localhost in production — set APP__DATABASE__URL to the managed instance".into(),
            ));
        }
        if url.contains("app:app@") {
            return Err(ConfigError::Invalid(
                "database.url uses the default 'app:app' credentials in production — supply real credentials".into(),
            ));
        }
        if self.jwt.issuer == "https://identity.local" {
            return Err(ConfigError::Invalid(
                "jwt.issuer is still the local default in production — set APP__JWT__ISSUER".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid() {
        let cfg = AppConfig::default();
        assert!(cfg.validate("local").is_ok());
    }

    #[test]
    fn production_rejects_local_defaults() {
        // The struct defaults (localhost + app:app + identity.local) must not survive a
        // production boot.
        let cfg = AppConfig::default();
        assert!(matches!(
            cfg.validate("production"),
            Err(ConfigError::Invalid(_))
        ));
    }

    #[test]
    fn production_accepts_real_config() {
        let mut cfg = AppConfig::default();
        cfg.database.url = "postgres://svc:s3cr3t@db.internal:5432/ledger_db".into();
        cfg.jwt.issuer = "https://identity.example.com".into();
        assert!(cfg.validate("production").is_ok());
    }

    #[test]
    fn env_overrides_defaults() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("APP__DATABASE__MAX_CONNECTIONS", "50");
            jail.set_env("APP__OTEL__SERVICE_NAME", "ledger");
            let cfg = AppConfig::load_from("does-not-exist.toml").unwrap();
            assert_eq!(cfg.database.max_connections, 50);
            assert_eq!(cfg.otel.service_name, "ledger");
            Ok(())
        });
    }

    #[test]
    fn invalid_sample_ratio_is_rejected() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("APP__OTEL__SAMPLE_RATIO", "2.0");
            let err = AppConfig::load_from("none.toml").unwrap_err();
            assert!(matches!(err, ConfigError::Invalid(_)));
            Ok(())
        });
    }

    #[test]
    fn file_layer_is_read() {
        figment::Jail::expect_with(|jail| {
            jail.create_file("test.toml", "[log]\nformat = \"pretty\"\n")?;
            let cfg = AppConfig::load_from("test.toml").unwrap();
            assert_eq!(cfg.log.format, "pretty");
            Ok(())
        });
    }
}
