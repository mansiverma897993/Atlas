//! The tonic gRPC server for `ledger.v1`. Thin: it translates protobuf ⇄ domain types,
//! delegates to the application handlers, and maps errors to gRPC [`Status`]. No business
//! logic lives here.

use kernel::{AccountId, Currency, Money, OwnerId, TransferId};
use tonic::{Request, Response, Status};

use crate::application::commands::{CommandError, CommandHandlers};
use crate::application::ports::PortError;
use crate::application::queries::QueryHandlers;
use crate::domain::account::AccountCommand;
use crate::domain::error::DomainError;
use proto::ledger::ledger_service_server::LedgerService;
use proto::ledger::{
    AccountView, BalanceView, CommandAck, FreezeAccountRequest, GetAccountRequest,
    GetBalanceRequest, GetTransferRequest, InitiateTransferRequest, InitiateTransferResponse,
    ListTransactionsRequest, Money as ProtoMoney, OpenAccountRequest, OpenAccountResponse,
    TransactionEntry, TransactionPage, TransferView,
};

/// gRPC adapter over the ledger application layer.
pub struct GrpcLedger {
    commands: CommandHandlers,
    queries: QueryHandlers,
}

impl GrpcLedger {
    /// Wire the adapter with the application handlers.
    pub fn new(commands: CommandHandlers, queries: QueryHandlers) -> Self {
        Self { commands, queries }
    }
}

/// Map a client-supplied currency code to the domain [`Currency`].
fn parse_currency(code: &str) -> Result<Currency, Status> {
    Currency::from_code(code)
        .ok_or_else(|| Status::invalid_argument(format!("unknown currency '{code}'")))
}

fn parse_account(id: &str) -> Result<AccountId, Status> {
    AccountId::parse(id).map_err(|_| Status::invalid_argument("invalid account id"))
}

fn to_proto_money(m: Money) -> ProtoMoney {
    ProtoMoney {
        minor_units: m.minor_units() as i64,
        currency: m.currency().code().to_string(),
    }
}

/// Translate application errors into gRPC status codes.
fn map_command_error(e: CommandError) -> Status {
    match e {
        CommandError::Domain(d) => match d {
            DomainError::AccountNotFound => Status::not_found(d.to_string()),
            DomainError::AlreadyOpened => Status::already_exists(d.to_string()),
            DomainError::InsufficientFunds { .. } => Status::failed_precondition(d.to_string()),
            _ => Status::invalid_argument(d.to_string()),
        },
        CommandError::Port(PortError::Conflict { .. }) => Status::aborted("concurrency conflict"),
        CommandError::Port(e) => Status::internal(e.to_string()),
    }
}

#[tonic::async_trait]
impl LedgerService for GrpcLedger {
    async fn open_account(
        &self,
        request: Request<OpenAccountRequest>,
    ) -> Result<Response<OpenAccountResponse>, Status> {
        let req = request.into_inner();
        let owner = OwnerId::parse(&req.owner_id)
            .map_err(|_| Status::invalid_argument("invalid owner id"))?;
        let currency = parse_currency(&req.currency)?;
        let id = self
            .commands
            .open_account(owner, currency, "grpc")
            .await
            .map_err(map_command_error)?;
        Ok(Response::new(OpenAccountResponse {
            account_id: id.to_string(),
        }))
    }

    async fn initiate_transfer(
        &self,
        request: Request<InitiateTransferRequest>,
    ) -> Result<Response<InitiateTransferResponse>, Status> {
        let req = request.into_inner();
        let source = parse_account(&req.source_account_id)?;
        let destination = parse_account(&req.destination_account_id)?;
        let amount_proto = req
            .amount
            .ok_or_else(|| Status::invalid_argument("amount required"))?;
        let currency = parse_currency(&amount_proto.currency)?;
        if amount_proto.minor_units <= 0 {
            return Err(Status::invalid_argument("amount must be positive"));
        }
        let amount = Money::from_minor(i128::from(amount_proto.minor_units), currency);
        let key = if req.idempotency_key.is_empty() {
            TransferId::new().to_string()
        } else {
            req.idempotency_key
        };

        let transfer_id = self
            .commands
            .initiate_transfer(&key, source, destination, amount, "grpc")
            .await
            .map_err(map_command_error)?;
        Ok(Response::new(InitiateTransferResponse {
            transfer_id: transfer_id.to_string(),
            status: "REQUESTED".into(),
        }))
    }

    async fn freeze_account(
        &self,
        request: Request<FreezeAccountRequest>,
    ) -> Result<Response<CommandAck>, Status> {
        let req = request.into_inner();
        let account = parse_account(&req.account_id)?;
        self.commands
            .execute(
                account,
                AccountCommand::Freeze { reason: req.reason },
                "grpc",
            )
            .await
            .map_err(map_command_error)?;
        Ok(Response::new(CommandAck { accepted: true }))
    }

    async fn get_account(
        &self,
        request: Request<GetAccountRequest>,
    ) -> Result<Response<AccountView>, Status> {
        let account = parse_account(&request.into_inner().account_id)?;
        let view = self
            .queries
            .account(account)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("account not found"))?;
        Ok(Response::new(AccountView {
            account_id: view.account_id.to_string(),
            owner_id: view.owner_id,
            currency: view.currency,
            status: view.status,
            posted_balance: Some(to_proto_money(view.posted)),
            reserved: Some(to_proto_money(view.reserved)),
            available: Some(to_proto_money(view.available)),
            version: view.version,
        }))
    }

    async fn get_balance(
        &self,
        request: Request<GetBalanceRequest>,
    ) -> Result<Response<BalanceView>, Status> {
        let account = parse_account(&request.into_inner().account_id)?;
        let view = self
            .queries
            .account(account)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("account not found"))?;
        Ok(Response::new(BalanceView {
            account_id: view.account_id.to_string(),
            posted: Some(to_proto_money(view.posted)),
            reserved: Some(to_proto_money(view.reserved)),
            available: Some(to_proto_money(view.available)),
        }))
    }

    async fn get_transfer(
        &self,
        request: Request<GetTransferRequest>,
    ) -> Result<Response<TransferView>, Status> {
        let id = TransferId::parse(&request.into_inner().transfer_id)
            .map_err(|_| Status::invalid_argument("invalid transfer id"))?;
        let view = self
            .queries
            .transfer(id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("transfer not found"))?;
        Ok(Response::new(TransferView {
            transfer_id: view.transfer_id.to_string(),
            source_account_id: view.source.to_string(),
            destination_account_id: view.destination.to_string(),
            amount: Some(to_proto_money(view.amount)),
            status: view.status,
            failure_reason: view.failure_reason.unwrap_or_default(),
            created_at: 0,
            updated_at: 0,
        }))
    }

    async fn list_transactions(
        &self,
        request: Request<ListTransactionsRequest>,
    ) -> Result<Response<TransactionPage>, Status> {
        let req = request.into_inner();
        let account = parse_account(&req.account_id)?;
        let cursor = if req.cursor.is_empty() {
            None
        } else {
            Some(req.cursor)
        };
        let (entries, next) = self
            .queries
            .transactions(account, req.limit.max(1), cursor)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(TransactionPage {
            entries: entries
                .into_iter()
                .map(|e| TransactionEntry {
                    transfer_id: e.transfer_id.to_string(),
                    direction: e.direction,
                    amount: Some(to_proto_money(e.amount)),
                    occurred_at: e.occurred_at,
                })
                .collect(),
            next_cursor: next.unwrap_or_default(),
        }))
    }
}
