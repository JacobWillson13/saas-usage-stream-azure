from __future__ import annotations

import argparse
import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from psycopg import sql
from psycopg_pool import ConnectionPool

ROOT = Path(__file__).resolve().parents[2]


SCHEMA_SQL = ROOT / "sql" / "ddl" / "schema.sql"
VIEWS_SQL = ROOT / "sql" / "views" / "dash.sql"


def env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise RuntimeError(f"{name} is required")
    return value


def azure_user_candidates(user: str, host: str) -> list[str]:
    server = host.split(".", 1)[0]
    candidates = [user]
    if "@" not in user and server:
        candidates.append(f"{user}@{server}")
    return candidates


def connect_with_candidates(
    user: str,
    password: str,
    *,
    autocommit: bool = False,
    dbname: str | None = None,
) -> psycopg.Connection:
    host = env("PGHOST")
    last_error: Exception | None = None
    for candidate in azure_user_candidates(user, host):
        try:
            return psycopg.connect(
                host=host,
                port=env("PGPORT", "5432"),
                dbname=dbname or env("PGDATABASE"),
                user=candidate,
                password=password,
                sslmode="require",
                autocommit=autocommit,
                options="-c timezone=UTC",
            )
        except psycopg.OperationalError as error:
            last_error = error
    if last_error is not None:
        raise last_error
    raise RuntimeError("no database user candidates")


def admin_connection(dbname: str | None = None) -> psycopg.Connection:
    load_dotenv(ROOT / ".env")
    return connect_with_candidates(
        env("PGADMIN_USER"),
        env("PGADMIN_PASSWORD"),
        autocommit=True,
        dbname=dbname,
    )


def stream_pool(max_size: int = 2) -> ConnectionPool:
    """Pooled stream_rw connections for the local apps (at most 5 per app)."""
    load_dotenv(ROOT / ".env")
    conninfo = psycopg.conninfo.make_conninfo(
        host=env("PGHOST"),
        port=env("PGPORT", "5432"),
        dbname=env("PGDATABASE"),
        user=os.getenv("PGSTREAM_USER", "stream_rw"),
        password=env("STREAM_RW_PASSWORD"),
        sslmode="require",
        options="-c timezone=UTC",
        connect_timeout=15,
    )
    return ConnectionPool(
        conninfo,
        min_size=1,
        max_size=min(max_size, 5),
        check=ConnectionPool.check_connection,
        open=True,
    )


def ensure_database() -> None:
    database = env("PGDATABASE")
    with admin_connection("postgres") as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database,))
        if cur.fetchone() is None:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
            print(f"created database {database}")


def create_or_update_role(cur: psycopg.Cursor, role: str, password: str) -> None:
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
    if cur.fetchone() is None:
        cur.execute(sql.SQL("CREATE ROLE {} LOGIN").format(sql.Identifier(role)))
    # Hash client-side so the plaintext never appears in SQL text, logs, or errors.
    verifier = cur.connection.pgconn.encrypt_password(
        password.encode(), role.encode(), b"scram-sha-256"
    )
    cur.execute(
        sql.SQL("ALTER ROLE {} WITH LOGIN PASSWORD {}").format(
            sql.Identifier(role), sql.Literal(verifier.decode())
        )
    )


def init_db() -> None:
    load_dotenv(ROOT / ".env")
    ensure_database()
    with admin_connection() as conn, conn.cursor() as cur:
        stream_password = env("STREAM_RW_PASSWORD")
        grafana_password = env("GRAFANA_RO_PASSWORD")
        create_or_update_role(cur, "stream_rw", stream_password)
        create_or_update_role(cur, "grafana_ro", grafana_password)

        cur.execute(SCHEMA_SQL.read_text())
        cur.execute(VIEWS_SQL.read_text())

        database = sql.Identifier(env("PGDATABASE"))
        cur.execute(sql.SQL("REVOKE ALL ON DATABASE {} FROM PUBLIC").format(database))
        cur.execute(
            sql.SQL("GRANT CONNECT ON DATABASE {} TO stream_rw, grafana_ro").format(database)
        )
        cur.execute("REVOKE ALL ON SCHEMA public FROM PUBLIC;")
        cur.execute("REVOKE ALL ON SCHEMA app, ops, dash FROM PUBLIC;")
        cur.execute("GRANT USAGE ON SCHEMA app, ops TO stream_rw;")
        cur.execute(
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE "
            "ON ALL TABLES IN SCHEMA app, ops TO stream_rw;"
        )
        cur.execute("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA app, ops TO stream_rw;")
        cur.execute(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA app "
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO stream_rw;"
        )
        cur.execute(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA ops "
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO stream_rw;"
        )
        cur.execute(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA app "
            "GRANT USAGE, SELECT ON SEQUENCES TO stream_rw;"
        )
        cur.execute(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA ops "
            "GRANT USAGE, SELECT ON SEQUENCES TO stream_rw;"
        )

        cur.execute("GRANT USAGE ON SCHEMA dash TO grafana_ro;")
        cur.execute("GRANT SELECT ON ALL TABLES IN SCHEMA dash TO grafana_ro;")
        cur.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA dash GRANT SELECT ON TABLES TO grafana_ro;")
        cur.execute("ALTER ROLE grafana_ro SET default_transaction_read_only = on;")
        cur.execute("ALTER ROLE grafana_ro SET statement_timeout = '10s';")
        cur.execute("ALTER ROLE grafana_ro SET timezone = 'UTC';")

    print("db-init complete")


def reset_db() -> None:
    response = input("Drop and recreate app and ops schemas? Type reset to continue: ")
    if response != "reset":
        print("reset cancelled")
        return
    with admin_connection() as conn, conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS dash CASCADE;")
        cur.execute("DROP SCHEMA IF EXISTS ops CASCADE;")
        cur.execute("DROP SCHEMA IF EXISTS app CASCADE;")
    init_db()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["init", "reset"])
    args = parser.parse_args()
    if args.command == "init":
        init_db()
    else:
        reset_db()


if __name__ == "__main__":
    main()
