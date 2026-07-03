//! The tonic gRPC server for `identity.v1.AuthService`. Thin: it translates protobuf ⇄
//! application values, delegates to [`AuthHandlers`], and maps errors to gRPC [`Status`]. No
//! business logic lives here.

use tonic::{Request, Response, Status};

use crate::application::error::AuthError;
use crate::application::handlers::{AuthHandlers, IssuedTokens};
use crate::application::ports::PortError;
use crate::domain::DomainError;

use proto::identity::auth_service_server::AuthService;
use proto::identity::{
    AuthorizeRequest, AuthorizeResponse, Claims, LoginRequest, LogoutRequest, LogoutResponse,
    RefreshRequest, RegisterRequest, RegisterResponse, TokenPair, ValidateRequest,
};

/// gRPC adapter over the Identity application layer.
pub struct GrpcAuth {
    handlers: AuthHandlers,
}

impl GrpcAuth {
    /// Wire the adapter with the application handlers.
    #[must_use]
    pub fn new(handlers: AuthHandlers) -> Self {
        Self { handlers }
    }
}

/// Translate an application error into a gRPC status code.
fn map_error(e: AuthError) -> Status {
    match e {
        AuthError::Domain(d) => match d {
            DomainError::WeakPassword(_) | DomainError::InvalidEmail => {
                Status::invalid_argument(d.to_string())
            }
            DomainError::InvalidCredentials => Status::unauthenticated(d.to_string()),
            DomainError::InvalidRefreshToken => Status::unauthenticated(d.to_string()),
            DomainError::TokenReuseDetected => Status::permission_denied(d.to_string()),
            DomainError::Forbidden => Status::permission_denied(d.to_string()),
        },
        AuthError::EmailExists => Status::already_exists(e.to_string()),
        AuthError::Token(_) => Status::unauthenticated(e.to_string()),
        AuthError::InvalidSubject => Status::invalid_argument(e.to_string()),
        AuthError::Port(PortError::UniqueViolation) => Status::already_exists("already exists"),
        AuthError::Port(p) => Status::internal(p.to_string()),
    }
}

/// Convert issued tokens to the wire `TokenPair`.
fn to_token_pair(t: IssuedTokens) -> TokenPair {
    TokenPair {
        access_token: t.access_token,
        refresh_token: t.refresh_token,
        expires_in: t.expires_in,
        token_type: "Bearer".to_string(),
    }
}

#[tonic::async_trait]
impl AuthService for GrpcAuth {
    async fn register(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>, Status> {
        let req = request.into_inner();
        let id = self
            .handlers
            .register(&req.email, &req.password, &req.display_name)
            .await
            .map_err(map_error)?;
        Ok(Response::new(RegisterResponse {
            user_id: id.to_string(),
        }))
    }

    async fn login(&self, request: Request<LoginRequest>) -> Result<Response<TokenPair>, Status> {
        let req = request.into_inner();
        let tokens = self
            .handlers
            .login(&req.email, &req.password)
            .await
            .map_err(map_error)?;
        Ok(Response::new(to_token_pair(tokens)))
    }

    async fn refresh_token(
        &self,
        request: Request<RefreshRequest>,
    ) -> Result<Response<TokenPair>, Status> {
        let req = request.into_inner();
        let tokens = self
            .handlers
            .refresh(&req.refresh_token)
            .await
            .map_err(map_error)?;
        Ok(Response::new(to_token_pair(tokens)))
    }

    async fn logout(
        &self,
        request: Request<LogoutRequest>,
    ) -> Result<Response<LogoutResponse>, Status> {
        let req = request.into_inner();
        let revoked = self
            .handlers
            .logout(&req.refresh_token)
            .await
            .map_err(map_error)?;
        Ok(Response::new(LogoutResponse { revoked }))
    }

    async fn validate_token(
        &self,
        request: Request<ValidateRequest>,
    ) -> Result<Response<Claims>, Status> {
        let req = request.into_inner();
        let claims = self
            .handlers
            .validate_token(&req.access_token)
            .map_err(map_error)?;
        Ok(Response::new(Claims {
            subject: claims.sub,
            roles: claims.roles,
            permissions: claims.permissions,
            expires_at: claims.exp,
            issuer: claims.iss,
        }))
    }

    async fn authorize(
        &self,
        request: Request<AuthorizeRequest>,
    ) -> Result<Response<AuthorizeResponse>, Status> {
        let req = request.into_inner();
        let allowed = self
            .handlers
            .authorize(&req.subject, &req.permission)
            .await
            .map_err(map_error)?;
        Ok(Response::new(AuthorizeResponse { allowed }))
    }
}
