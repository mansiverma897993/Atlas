-- =============================================================================
-- Postgres bootstrap for the Ledger Platform (local docker-compose only).
--
-- The official postgres image runs every *.sql in /docker-entrypoint-initdb.d
-- exactly once, on first cluster init (empty data volume), as the superuser.
--
-- We use ONE Postgres server hosting THREE logical databases — one per bounded
-- context (docs/ARCHITECTURE.md §7, ADR-0008):
--     identity_db   → identity service
--     ledger_db     → ledger service (event store + projections)
--     worker_db     → worker service (saga state + read models)
-- notification is stateless (Redis only) and gets no database.
--
-- In real prod these are separate managed instances; co-locating them locally
-- keeps the laptop stack small. Migrations (schema) are applied SEPARATELY by
-- the `migrate` job — this file only creates databases and the app role.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Least-privilege application role.
-- The app connects as `app`, never as the postgres superuser (docs §6.3).
-- Password is fine to hardcode for LOCAL dev; prod injects it via Secret.
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'app') THEN
        CREATE ROLE app WITH LOGIN PASSWORD 'app'
            NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
    END IF;
END
$$;

-- -----------------------------------------------------------------------------
-- One database per context, each owned by `app`.
-- (CREATE DATABASE cannot run inside the DO/transaction block above.)
-- -----------------------------------------------------------------------------
CREATE DATABASE identity_db OWNER app;
CREATE DATABASE ledger_db   OWNER app;
CREATE DATABASE worker_db   OWNER app;

-- -----------------------------------------------------------------------------
-- Harden default privileges: revoke the implicit PUBLIC access and grant only
-- what `app` needs. Applied per-database via \connect.
-- -----------------------------------------------------------------------------
\connect identity_db
REVOKE ALL ON DATABASE identity_db FROM PUBLIC;
GRANT CONNECT ON DATABASE identity_db TO app;
-- app owns the schema so migrations can create/alter objects.
ALTER SCHEMA public OWNER TO app;
GRANT ALL ON SCHEMA public TO app;

\connect ledger_db
REVOKE ALL ON DATABASE ledger_db FROM PUBLIC;
GRANT CONNECT ON DATABASE ledger_db TO app;
ALTER SCHEMA public OWNER TO app;
GRANT ALL ON SCHEMA public TO app;

\connect worker_db
REVOKE ALL ON DATABASE worker_db FROM PUBLIC;
GRANT CONNECT ON DATABASE worker_db TO app;
ALTER SCHEMA public OWNER TO app;
GRANT ALL ON SCHEMA public TO app;
