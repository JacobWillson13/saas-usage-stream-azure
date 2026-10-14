-- Schemas and tables. Idempotent: run by `make db-init` as the admin role.
-- Every app table has event_ts (display time), source_event_ts (source or take time,
-- used by the scorer and model), ingested_at, ingest_id, is_backfill, is_demo, and
-- is_injected (created or modified by anomaly injection) (SPEC 4.2). Payments also
-- have is_generated (live payments sampled from recent history). The unique constraint on ingest_id provides its btree index.

CREATE SCHEMA IF NOT EXISTS app;
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS dash;

-- One sequence for all app tables, so consumers read every table in insert order.
CREATE SEQUENCE IF NOT EXISTS app.ingest_id_seq;

CREATE TABLE IF NOT EXISTS app.accounts (
    account_id text PRIMARY KEY,
    event_ts timestamptz NOT NULL,
    source_event_ts timestamptz NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    ingest_id bigint NOT NULL DEFAULT nextval('app.ingest_id_seq') UNIQUE,
    is_backfill boolean NOT NULL DEFAULT false,
    is_demo boolean NOT NULL DEFAULT false,
    is_injected boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL,
    account_type text NOT NULL,
    signup_plan text NOT NULL,
    price_version text NOT NULL,
    trial_started_at timestamptz,
    trial_ends_at timestamptz,
    currency text NOT NULL,
    is_nonprofit boolean NOT NULL,
    is_internal boolean NOT NULL
);

CREATE TABLE IF NOT EXISTS app.users (
    user_id text PRIMARY KEY,
    account_id text NOT NULL REFERENCES app.accounts(account_id),
    event_ts timestamptz NOT NULL,
    source_event_ts timestamptz NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    ingest_id bigint NOT NULL DEFAULT nextval('app.ingest_id_seq') UNIQUE,
    is_backfill boolean NOT NULL DEFAULT false,
    is_demo boolean NOT NULL DEFAULT false,
    is_injected boolean NOT NULL DEFAULT false,
    role text NOT NULL,
    invited_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS app.devices (
    device_id text PRIMARY KEY,
    account_id text NOT NULL REFERENCES app.accounts(account_id),
    user_id text REFERENCES app.users(user_id),
    event_ts timestamptz NOT NULL,
    source_event_ts timestamptz NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    ingest_id bigint NOT NULL DEFAULT nextval('app.ingest_id_seq') UNIQUE,
    is_backfill boolean NOT NULL DEFAULT false,
    is_demo boolean NOT NULL DEFAULT false,
    is_injected boolean NOT NULL DEFAULT false,
    device_fingerprint text NOT NULL,
    os text NOT NULL,
    is_tagged boolean NOT NULL,
    registered_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS app.usage_daily (
    account_id text NOT NULL REFERENCES app.accounts(account_id),
    activity_date date NOT NULL,
    event_ts timestamptz NOT NULL,
    source_event_ts timestamptz NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    ingest_id bigint NOT NULL DEFAULT nextval('app.ingest_id_seq') UNIQUE,
    is_backfill boolean NOT NULL DEFAULT false,
    is_demo boolean NOT NULL DEFAULT false,
    is_injected boolean NOT NULL DEFAULT false,
    active_users integer NOT NULL,
    user_devices integer NOT NULL,
    tagged_resources integer NOT NULL,
    ephemeral_minutes integer NOT NULL,
    PRIMARY KEY (account_id, activity_date)
);

CREATE TABLE IF NOT EXISTS app.feature_usage_daily (
    account_id text NOT NULL REFERENCES app.accounts(account_id),
    activity_date date NOT NULL,
    feature text NOT NULL,
    event_ts timestamptz NOT NULL,
    source_event_ts timestamptz NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    ingest_id bigint NOT NULL DEFAULT nextval('app.ingest_id_seq') UNIQUE,
    is_backfill boolean NOT NULL DEFAULT false,
    is_demo boolean NOT NULL DEFAULT false,
    is_injected boolean NOT NULL DEFAULT false,
    attempts integer NOT NULL,
    gated_blocked boolean NOT NULL,
    PRIMARY KEY (account_id, activity_date, feature)
);

CREATE TABLE IF NOT EXISTS app.license_events (
    license_event_id text PRIMARY KEY,
    account_id text NOT NULL REFERENCES app.accounts(account_id),
    user_id text REFERENCES app.users(user_id),
    event_ts timestamptz NOT NULL,
    source_event_ts timestamptz NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    ingest_id bigint NOT NULL DEFAULT nextval('app.ingest_id_seq') UNIQUE,
    is_backfill boolean NOT NULL DEFAULT false,
    is_demo boolean NOT NULL DEFAULT false,
    is_injected boolean NOT NULL DEFAULT false,
    event_type text NOT NULL,
    licenses_held_after integer NOT NULL,
    licenses_used_after integer NOT NULL
);

CREATE TABLE IF NOT EXISTS app.plan_changes (
    plan_change_id text PRIMARY KEY,
    account_id text NOT NULL REFERENCES app.accounts(account_id),
    event_ts timestamptz NOT NULL,
    source_event_ts timestamptz NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    ingest_id bigint NOT NULL DEFAULT nextval('app.ingest_id_seq') UNIQUE,
    is_backfill boolean NOT NULL DEFAULT false,
    is_demo boolean NOT NULL DEFAULT false,
    is_injected boolean NOT NULL DEFAULT false,
    from_plan text NOT NULL,
    to_plan text NOT NULL,
    from_price_version text NOT NULL,
    to_price_version text NOT NULL,
    change_source text NOT NULL
);

CREATE TABLE IF NOT EXISTS app.invoices (
    invoice_id text PRIMARY KEY,
    account_id text NOT NULL REFERENCES app.accounts(account_id),
    event_ts timestamptz NOT NULL,
    source_event_ts timestamptz NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    ingest_id bigint NOT NULL DEFAULT nextval('app.ingest_id_seq') UNIQUE,
    is_backfill boolean NOT NULL DEFAULT false,
    is_demo boolean NOT NULL DEFAULT false,
    is_injected boolean NOT NULL DEFAULT false,
    status text NOT NULL,
    currency text NOT NULL,
    subtotal numeric(12,2) NOT NULL,
    subtotal_usd numeric(12,2) NOT NULL,
    discount_total numeric(12,2) NOT NULL,
    discount_total_usd numeric(12,2) NOT NULL,
    tax numeric(12,2) NOT NULL,
    tax_usd numeric(12,2) NOT NULL,
    total numeric(12,2) NOT NULL,
    total_usd numeric(12,2) NOT NULL,
    invoice_date date NOT NULL,
    service_period_start date NOT NULL,
    service_period_end date NOT NULL,
    due_date date NOT NULL,
    paid_at timestamptz
);

CREATE TABLE IF NOT EXISTS app.payments (
    payment_id text PRIMARY KEY,
    account_id text REFERENCES app.accounts(account_id),
    invoice_id text REFERENCES app.invoices(invoice_id),
    event_ts timestamptz NOT NULL,
    source_event_ts timestamptz NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    ingest_id bigint NOT NULL DEFAULT nextval('app.ingest_id_seq') UNIQUE,
    is_backfill boolean NOT NULL DEFAULT false,
    is_demo boolean NOT NULL DEFAULT false,
    is_injected boolean NOT NULL DEFAULT false,
    status text NOT NULL,
    currency text NOT NULL,
    amount numeric(12,2) NOT NULL,
    amount_usd numeric(12,2) NOT NULL,
    amount_refunded numeric(12,2) NOT NULL,
    amount_refunded_usd numeric(12,2) NOT NULL,
    is_test boolean NOT NULL,
    paid boolean NOT NULL,
    captured boolean NOT NULL,
    refunded boolean NOT NULL,
    is_generated boolean NOT NULL DEFAULT false
);

-- Live-only tables: sessions and feature events generated by `make demo`.
-- History for the same activity stays in usage_daily and feature_usage_daily.

CREATE TABLE IF NOT EXISTS app.sessions (
    session_id text PRIMARY KEY,
    account_id text NOT NULL REFERENCES app.accounts(account_id),
    user_id text REFERENCES app.users(user_id),
    device_id text REFERENCES app.devices(device_id),
    event_ts timestamptz NOT NULL,
    source_event_ts timestamptz NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    ingest_id bigint NOT NULL DEFAULT nextval('app.ingest_id_seq') UNIQUE,
    is_backfill boolean NOT NULL DEFAULT false,
    is_demo boolean NOT NULL DEFAULT false,
    is_injected boolean NOT NULL DEFAULT false,
    started_at timestamptz NOT NULL,
    duration_s integer NOT NULL
);

CREATE TABLE IF NOT EXISTS app.feature_events (
    event_id text PRIMARY KEY,
    account_id text NOT NULL REFERENCES app.accounts(account_id),
    user_id text REFERENCES app.users(user_id),
    event_ts timestamptz NOT NULL,
    source_event_ts timestamptz NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    ingest_id bigint NOT NULL DEFAULT nextval('app.ingest_id_seq') UNIQUE,
    is_backfill boolean NOT NULL DEFAULT false,
    is_demo boolean NOT NULL DEFAULT false,
    is_injected boolean NOT NULL DEFAULT false,
    feature text NOT NULL,
    gated_blocked boolean NOT NULL,
    occurred_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS ops.replay_state (
    singleton boolean PRIMARY KEY DEFAULT true,
    status text NOT NULL DEFAULT 'empty',
    demo_date date,
    source_cutoff date,
    offset_days integer,
    cursor_index integer NOT NULL DEFAULT 0,
    cursor_ts timestamptz,
    demo_batches integer NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (singleton)
);

CREATE TABLE IF NOT EXISTS ops.ingest_log (
    batch_id bigserial PRIMARY KEY,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz NOT NULL DEFAULT now(),
    rows_inserted integer NOT NULL DEFAULT 0,
    min_source_ts timestamptz,
    max_source_ts timestamptz,
    demo_batch integer,
    is_backfill boolean NOT NULL DEFAULT false,
    is_demo boolean NOT NULL DEFAULT false
);

CREATE INDEX IF NOT EXISTS ingest_log_finished_at_idx ON ops.ingest_log (finished_at);

CREATE TABLE IF NOT EXISTS ops.ingest_log_tables (
    batch_id bigint NOT NULL REFERENCES ops.ingest_log(batch_id) ON DELETE CASCADE,
    table_name text NOT NULL,
    rows_inserted integer NOT NULL,
    PRIMARY KEY (batch_id, table_name)
);

CREATE TABLE IF NOT EXISTS ops.consumer_offsets (
    consumer_name text PRIMARY KEY,
    last_ingest_id bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- The ops output tables below are insert-only, so a demo reset can delete
-- everything above the checkpoint's id high-water marks.

CREATE TABLE IF NOT EXISTS ops.injected_anomalies (
    anomaly_id bigserial PRIMARY KEY,
    anomaly_type text NOT NULL,
    table_name text NOT NULL,
    row_key text NOT NULL,
    source_event_ts timestamptz NOT NULL,
    event_ts timestamptz NOT NULL,
    scripted boolean NOT NULL DEFAULT false,
    is_demo boolean NOT NULL DEFAULT false,
    injected_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS injected_anomalies_row_idx
    ON ops.injected_anomalies (table_name, row_key);

CREATE TABLE IF NOT EXISTS ops.outlier_flags (
    flag_id bigserial PRIMARY KEY,
    table_name text NOT NULL,
    row_key text NOT NULL,
    flag_type text NOT NULL,
    method text NOT NULL,
    score double precision,
    reason text NOT NULL,
    source_event_ts timestamptz NOT NULL,
    event_ts timestamptz NOT NULL,
    flagged_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (table_name, row_key, flag_type)
);

CREATE INDEX IF NOT EXISTS outlier_flags_flagged_at_idx ON ops.outlier_flags (flagged_at);

CREATE TABLE IF NOT EXISTS ops.predictions (
    prediction_id bigserial PRIMARY KEY,
    account_id text NOT NULL,
    model_name text NOT NULL,
    source_day date NOT NULL,
    source_event_ts timestamptz NOT NULL,
    event_ts timestamptz NOT NULL,
    probability double precision NOT NULL,
    phase text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (account_id, model_name, source_day)
);

CREATE INDEX IF NOT EXISTS predictions_created_at_idx ON ops.predictions (created_at);

CREATE TABLE IF NOT EXISTS ops.trial_outcomes (
    outcome_id bigserial PRIMARY KEY,
    account_id text NOT NULL,
    model_name text NOT NULL,
    label integer NOT NULL,
    probability double precision NOT NULL,
    source_event_ts timestamptz NOT NULL,
    event_ts timestamptz NOT NULL,
    phase text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (account_id, model_name)
);

CREATE TABLE IF NOT EXISTS ops.model_metrics (
    metric_id bigserial PRIMARY KEY,
    model_name text NOT NULL,
    phase text NOT NULL,
    resolved_trials integer NOT NULL,
    rolling_auc double precision,
    rolling_log_loss double precision,
    cumulative_auc double precision,
    top_decile_lift double precision,
    source_event_ts timestamptz NOT NULL,
    event_ts timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (model_name, resolved_trials)
);

CREATE INDEX IF NOT EXISTS accounts_ingested_at_brin ON app.accounts USING brin (ingested_at);
CREATE INDEX IF NOT EXISTS accounts_event_ts_idx ON app.accounts (event_ts);
CREATE INDEX IF NOT EXISTS users_ingested_at_brin ON app.users USING brin (ingested_at);
CREATE INDEX IF NOT EXISTS users_event_ts_idx ON app.users (event_ts);
CREATE INDEX IF NOT EXISTS devices_ingested_at_brin ON app.devices USING brin (ingested_at);
CREATE INDEX IF NOT EXISTS devices_event_ts_idx ON app.devices (event_ts);
CREATE INDEX IF NOT EXISTS usage_daily_ingested_at_brin ON app.usage_daily USING brin (ingested_at);
CREATE INDEX IF NOT EXISTS usage_daily_event_ts_idx ON app.usage_daily (event_ts);
CREATE INDEX IF NOT EXISTS feature_usage_daily_ingested_at_brin ON app.feature_usage_daily USING brin (ingested_at);
CREATE INDEX IF NOT EXISTS feature_usage_daily_event_ts_idx ON app.feature_usage_daily (event_ts);
CREATE INDEX IF NOT EXISTS license_events_ingested_at_brin ON app.license_events USING brin (ingested_at);
CREATE INDEX IF NOT EXISTS license_events_event_ts_idx ON app.license_events (event_ts);
CREATE INDEX IF NOT EXISTS plan_changes_ingested_at_brin ON app.plan_changes USING brin (ingested_at);
CREATE INDEX IF NOT EXISTS plan_changes_event_ts_idx ON app.plan_changes (event_ts);
CREATE INDEX IF NOT EXISTS invoices_ingested_at_brin ON app.invoices USING brin (ingested_at);
CREATE INDEX IF NOT EXISTS invoices_event_ts_idx ON app.invoices (event_ts);
CREATE INDEX IF NOT EXISTS payments_ingested_at_brin ON app.payments USING brin (ingested_at);
CREATE INDEX IF NOT EXISTS payments_event_ts_idx ON app.payments (event_ts);
CREATE INDEX IF NOT EXISTS accounts_source_event_ts_idx ON app.accounts (source_event_ts);
CREATE INDEX IF NOT EXISTS accounts_demo_idx ON app.accounts (ingest_id) WHERE is_demo;
CREATE INDEX IF NOT EXISTS users_source_event_ts_idx ON app.users (source_event_ts);
CREATE INDEX IF NOT EXISTS users_demo_idx ON app.users (ingest_id) WHERE is_demo;
CREATE INDEX IF NOT EXISTS devices_source_event_ts_idx ON app.devices (source_event_ts);
CREATE INDEX IF NOT EXISTS devices_demo_idx ON app.devices (ingest_id) WHERE is_demo;
CREATE INDEX IF NOT EXISTS usage_daily_source_event_ts_idx ON app.usage_daily (source_event_ts);
CREATE INDEX IF NOT EXISTS usage_daily_demo_idx ON app.usage_daily (ingest_id) WHERE is_demo;
CREATE INDEX IF NOT EXISTS feature_usage_daily_source_event_ts_idx ON app.feature_usage_daily (source_event_ts);
CREATE INDEX IF NOT EXISTS feature_usage_daily_demo_idx ON app.feature_usage_daily (ingest_id) WHERE is_demo;
CREATE INDEX IF NOT EXISTS license_events_source_event_ts_idx ON app.license_events (source_event_ts);
CREATE INDEX IF NOT EXISTS license_events_demo_idx ON app.license_events (ingest_id) WHERE is_demo;
CREATE INDEX IF NOT EXISTS plan_changes_source_event_ts_idx ON app.plan_changes (source_event_ts);
CREATE INDEX IF NOT EXISTS plan_changes_demo_idx ON app.plan_changes (ingest_id) WHERE is_demo;
CREATE INDEX IF NOT EXISTS invoices_source_event_ts_idx ON app.invoices (source_event_ts);
CREATE INDEX IF NOT EXISTS invoices_demo_idx ON app.invoices (ingest_id) WHERE is_demo;
CREATE INDEX IF NOT EXISTS payments_source_event_ts_idx ON app.payments (source_event_ts);
CREATE INDEX IF NOT EXISTS payments_demo_idx ON app.payments (ingest_id) WHERE is_demo;
CREATE INDEX IF NOT EXISTS sessions_ingested_at_brin ON app.sessions USING brin (ingested_at);
CREATE INDEX IF NOT EXISTS sessions_event_ts_idx ON app.sessions (event_ts);
CREATE INDEX IF NOT EXISTS sessions_source_event_ts_idx ON app.sessions (source_event_ts);
CREATE INDEX IF NOT EXISTS sessions_demo_idx ON app.sessions (ingest_id) WHERE is_demo;
CREATE INDEX IF NOT EXISTS feature_events_ingested_at_brin ON app.feature_events USING brin (ingested_at);
CREATE INDEX IF NOT EXISTS feature_events_event_ts_idx ON app.feature_events (event_ts);
CREATE INDEX IF NOT EXISTS feature_events_source_event_ts_idx ON app.feature_events (source_event_ts);
CREATE INDEX IF NOT EXISTS feature_events_demo_idx ON app.feature_events (ingest_id) WHERE is_demo;
