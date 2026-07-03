//! OpenAPI specification derived from the handler DTOs (`utoipa`), served as Swagger UI at
//! `/swagger` and as raw JSON at `/api-docs/openapi.json` (ADR-0011: the gateway generates
//! OpenAPI from its own handler types).

use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::{Modify, OpenApi};

use crate::dto;

/// The generated OpenAPI document.
#[derive(OpenApi)]
#[openapi(
    info(
        title = "Ledger Platform Gateway",
        version = "0.1.0",
        description = "Public REST edge: authentication and ledger operations."
    ),
    paths(
        crate::handlers::register,
        crate::handlers::login,
        crate::handlers::refresh,
        crate::handlers::logout,
        crate::handlers::open_account,
        crate::handlers::get_account,
        crate::handlers::get_balance,
        crate::handlers::list_transactions,
        crate::handlers::create_transfer,
        crate::handlers::get_transfer,
    ),
    components(schemas(
        dto::RegisterRequest,
        dto::RegisterResponse,
        dto::LoginRequest,
        dto::RefreshRequest,
        dto::LogoutRequest,
        dto::TokenPair,
        dto::LogoutResponse,
        dto::Money,
        dto::OpenAccountRequest,
        dto::OpenAccountResponse,
        dto::AccountView,
        dto::BalanceView,
        dto::TransferRequest,
        dto::TransferAccepted,
        dto::TransferView,
        dto::TransactionEntry,
        dto::TransactionPage,
    )),
    modifiers(&SecurityAddon),
    tags(
        (name = "auth", description = "Authentication and token lifecycle"),
        (name = "ledger", description = "Accounts, balances, and transfers"),
    )
)]
pub struct ApiDoc;

/// Registers the `bearer` (JWT) security scheme on the components.
struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "bearer",
                SecurityScheme::Http(
                    HttpBuilder::new()
                        .scheme(HttpAuthScheme::Bearer)
                        .bearer_format("JWT")
                        .build(),
                ),
            );
        }
    }
}
