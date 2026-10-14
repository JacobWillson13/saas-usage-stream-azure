-- Dashboard views (SPEC 5). Grafana reads only these, as grafana_ro.
-- Business KPI views (usage, billing, live business panels) exclude is_injected rows;
-- stream health and outlier views include them.
-- Recreated on every `make db-init`, which re-grants SELECT afterwards.
-- Live views cover demo events in the last 15 minutes; demo rows get event_ts = now()
-- at insert, so event_ts is the ingest time. Daily trend views combine history (daily
-- rows before today; the baseline is shifted so it ends yesterday) with today's live
-- events rolled up to the day. All scan recent rows by index.

DROP VIEW IF EXISTS
    dash.replay_state,
    dash.demo_state,
    dash.ingest_log,
    dash.table_counts,
    dash.rows_per_minute,
    dash.batches,
    dash.live_events_per_second,
    dash.live_events_per_minute,
    dash.live_sessions_per_minute,
    dash.live_feature_events,
    dash.live_signups,
    dash.live_payments,
    dash.live_flags,
    dash.latest_flags,
    dash.live_summary,
    dash.live_events,
    dash.usage_summary,
    dash.daily_activity,
    dash.daily_features,
    dash.billing_daily,
    dash.billing_summary,
    dash.outlier_recall,
    dash.flag_precision,
    dash.model_metrics,
    dash.model_summary,
    dash.top_open_trials;

-- Demo state and stream health --------------------------------------------------

CREATE VIEW dash.demo_state AS
SELECT
    status,
    demo_date,
    source_cutoff,
    offset_days,
    cursor_index AS events_emitted,
    updated_at,
    extract(epoch FROM now() - updated_at)::integer AS seconds_since_update,
    elapsed_seconds,
    CASE
        WHEN elapsed_seconds IS NULL THEN 'idle'
        ELSE lpad((elapsed_seconds / 60)::text, 2, '0') || ':'
            || lpad((elapsed_seconds % 60)::text, 2, '0')
    END AS elapsed_label
FROM (
    -- Take time: the emitter's cursor on the take clock (source midnight plus seconds
    -- into the take), so it pauses with Ctrl-C and restarts at 0 after a reset.
    SELECT
        *,
        CASE
            WHEN status = 'running' AND updated_at > now() - interval '15 seconds'
                 AND cursor_ts IS NOT NULL
            THEN floor(extract(epoch FROM
                     cursor_ts - (source_cutoff::timestamp AT TIME ZONE 'UTC')))::integer
        END AS elapsed_seconds
    FROM ops.replay_state
) s;

-- Total rows per table from the ingest log, so Grafana never counts app tables.
CREATE VIEW dash.table_counts AS
SELECT t.table_name, coalesce(sum(l.rows_inserted), 0)::bigint AS rows
FROM (
    VALUES ('accounts'), ('users'), ('devices'), ('sessions'), ('usage_daily'),
           ('feature_usage_daily'), ('feature_events'), ('license_events'), ('plan_changes'),
           ('invoices'), ('payments')
) AS t(table_name)
LEFT JOIN ops.ingest_log_tables l USING (table_name)
GROUP BY t.table_name;

CREATE VIEW dash.rows_per_minute AS
SELECT
    date_trunc('minute', b.finished_at) AS time,
    t.table_name,
    sum(t.rows_inserted)::bigint AS rows
FROM ops.ingest_log b
JOIN ops.ingest_log_tables t USING (batch_id)
WHERE b.is_demo
  AND b.finished_at > now() - interval '24 hours'
GROUP BY 1, 2;

CREATE VIEW dash.batches AS
SELECT
    finished_at AS time,
    batch_id,
    rows_inserted,
    extract(epoch FROM finished_at - started_at) * 1000 AS latency_ms,
    max_source_ts AS source_time
FROM ops.ingest_log
WHERE is_demo
  AND finished_at > now() - interval '24 hours';

-- Live: the last 15 minutes ----------------------------------------------------
-- Flags count as live when both the flag and the flagged row are recent, so a fresh
-- baseline's flags (on shifted history rows) stay out of the live panels.

CREATE VIEW dash.live_events AS
SELECT 'accounts' AS table_name, event_ts FROM app.accounts
    WHERE is_demo AND event_ts > now() - interval '15 minutes'
UNION ALL SELECT 'users', event_ts FROM app.users
    WHERE is_demo AND event_ts > now() - interval '15 minutes'
UNION ALL SELECT 'devices', event_ts FROM app.devices
    WHERE is_demo AND event_ts > now() - interval '15 minutes'
UNION ALL SELECT 'sessions', event_ts FROM app.sessions
    WHERE is_demo AND event_ts > now() - interval '15 minutes'
UNION ALL SELECT 'feature_events', event_ts FROM app.feature_events
    WHERE is_demo AND event_ts > now() - interval '15 minutes'
UNION ALL SELECT 'license_events', event_ts FROM app.license_events
    WHERE is_demo AND event_ts > now() - interval '15 minutes'
UNION ALL SELECT 'plan_changes', event_ts FROM app.plan_changes
    WHERE is_demo AND event_ts > now() - interval '15 minutes'
UNION ALL SELECT 'invoices', event_ts FROM app.invoices
    WHERE is_demo AND event_ts > now() - interval '15 minutes'
UNION ALL SELECT 'payments', event_ts FROM app.payments
    WHERE is_demo AND event_ts > now() - interval '15 minutes';

CREATE VIEW dash.live_events_per_second AS
SELECT date_trunc('second', event_ts) AS time, count(*) AS events
FROM dash.live_events
GROUP BY 1;

CREATE VIEW dash.live_events_per_minute AS
SELECT date_trunc('minute', event_ts) AS time, table_name, count(*) AS events
FROM dash.live_events
GROUP BY 1, 2;

CREATE VIEW dash.live_sessions_per_minute AS
SELECT
    date_trunc('minute', event_ts) AS time,
    count(*) AS sessions,
    count(DISTINCT account_id) AS active_accounts,
    count(DISTINCT user_id) AS active_users
FROM app.sessions
WHERE is_demo AND NOT is_injected AND event_ts > now() - interval '15 minutes'
GROUP BY 1;

CREATE VIEW dash.live_feature_events AS
SELECT
    date_trunc('minute', event_ts) AS time,
    feature,
    count(*) AS events,
    count(*) FILTER (WHERE gated_blocked) AS gated_blocked
FROM app.feature_events
WHERE is_demo AND NOT is_injected AND event_ts > now() - interval '15 minutes'
GROUP BY 1, 2;

CREATE VIEW dash.live_signups AS
SELECT
    date_trunc('minute', event_ts) AS time,
    count(*) AS signups,
    count(*) FILTER (WHERE trial_started_at IS NOT NULL) AS trials
FROM app.accounts
WHERE is_demo AND NOT is_injected AND event_ts > now() - interval '15 minutes'
GROUP BY 1;

CREATE VIEW dash.live_payments AS
SELECT
    date_trunc('minute', event_ts) AS time,
    count(*) FILTER (WHERE status = 'succeeded') AS succeeded,
    count(*) FILTER (WHERE status = 'failed') AS failed
FROM app.payments
WHERE is_demo AND NOT is_injected AND event_ts > now() - interval '15 minutes'
GROUP BY 1;

CREATE VIEW dash.live_flags AS
SELECT date_trunc('minute', flagged_at) AS time, flag_type, count(*) AS flags
FROM ops.outlier_flags
WHERE flagged_at > now() - interval '15 minutes'
  AND event_ts > now() - interval '15 minutes'
GROUP BY 1, 2;

CREATE VIEW dash.latest_flags AS
SELECT
    flagged_at AS time, flag_type, table_name, row_key,
    round(score::numeric, 1) AS score, reason
FROM ops.outlier_flags
WHERE flagged_at > now() - interval '15 minutes'
  AND event_ts > now() - interval '15 minutes'
ORDER BY flagged_at DESC, flag_id DESC
LIMIT 20;

CREATE VIEW dash.live_summary AS
SELECT
    (SELECT count(*) FROM dash.live_events WHERE event_ts > now() - interval '1 minute')
        AS events_last_minute,
    (SELECT count(*) FROM app.sessions
        WHERE is_demo AND NOT is_injected AND event_ts > now() - interval '1 minute')
        AS sessions_last_minute,
    (SELECT count(DISTINCT account_id) FROM app.sessions
        WHERE is_demo AND NOT is_injected AND event_ts > now() - interval '15 minutes')
        AS active_accounts_15m,
    (SELECT count(*) FROM app.accounts
        WHERE is_demo AND NOT is_injected AND event_ts > now() - interval '15 minutes')
        AS signups_15m,
    (SELECT count(*) FROM app.payments
        WHERE is_demo AND NOT is_injected AND status = 'failed'
          AND event_ts > now() - interval '15 minutes') AS payments_failed_15m,
    (SELECT count(*) FROM ops.outlier_flags
        WHERE flagged_at > now() - interval '15 minutes'
          AND event_ts > now() - interval '15 minutes') AS flags_15m;

-- Daily trends: history daily rows plus today's live events -------------------

-- Active users = sum over accounts of each account's peak daily active users in
-- the last 7 days (today: distinct users with a live session).
CREATE VIEW dash.usage_summary AS
WITH daily AS (
    SELECT account_id, active_users
    FROM app.usage_daily
    WHERE event_ts > now() - interval '7 days'
      AND event_ts < date_trunc('day', now())
      AND active_users > 0
      AND NOT is_injected
    UNION ALL
    SELECT account_id, count(DISTINCT user_id)
    FROM app.sessions
    WHERE event_ts >= date_trunc('day', now()) AND NOT is_injected
    GROUP BY account_id
),
recent AS (
    SELECT account_id, max(active_users) AS active_users FROM daily GROUP BY account_id
)
SELECT
    count(*) AS active_accounts_7d,
    coalesce(sum(active_users), 0)::bigint AS active_users_7d
FROM recent;

CREATE VIEW dash.daily_activity AS
WITH days AS (
    SELECT generate_series(
        date_trunc('day', now()) - interval '29 days',
        date_trunc('day', now()),
        interval '1 day'
    ) AS day
),
since AS (
    SELECT date_trunc('day', now()) - interval '29 days' AS ts
),
signups AS (
    SELECT
        date_trunc('day', event_ts) AS day,
        count(*) AS signups,
        count(*) FILTER (WHERE trial_started_at IS NOT NULL) AS trials
    FROM app.accounts
    WHERE event_ts >= (SELECT ts FROM since) AND NOT is_injected
    GROUP BY 1
),
devices AS (
    SELECT date_trunc('day', event_ts) AS day, count(*) AS devices_registered
    FROM app.devices
    WHERE event_ts >= (SELECT ts FROM since) AND NOT is_injected
    GROUP BY 1
),
usage AS (
    SELECT
        date_trunc('day', event_ts) AS day,
        count(*) FILTER (WHERE active_users > 0) AS active_accounts,
        coalesce(sum(active_users), 0) AS active_users
    FROM app.usage_daily
    WHERE event_ts >= (SELECT ts FROM since)
      AND event_ts < date_trunc('day', now())
      AND NOT is_injected
    GROUP BY 1
    UNION ALL
    SELECT
        date_trunc('day', now()),
        count(DISTINCT account_id),
        count(DISTINCT user_id)
    FROM app.sessions
    WHERE event_ts >= date_trunc('day', now()) AND NOT is_injected
)
SELECT
    d.day,
    coalesce(s.signups, 0) AS signups,
    coalesce(s.trials, 0) AS trials,
    coalesce(v.devices_registered, 0) AS devices_registered,
    coalesce(u.active_accounts, 0) AS active_accounts,
    coalesce(u.active_users, 0) AS active_users
FROM days d
LEFT JOIN signups s USING (day)
LEFT JOIN devices v USING (day)
LEFT JOIN usage u USING (day);

CREATE VIEW dash.daily_features AS
WITH days AS (
    SELECT generate_series(
        date_trunc('day', now()) - interval '29 days',
        date_trunc('day', now()),
        interval '1 day'
    ) AS day
),
attempts AS (
    SELECT
        date_trunc('day', event_ts) AS day,
        sum(attempts) AS attempts,
        coalesce(sum(attempts) FILTER (WHERE gated_blocked), 0) AS gated_attempts
    FROM app.feature_usage_daily
    WHERE event_ts >= date_trunc('day', now()) - interval '29 days'
      AND event_ts < date_trunc('day', now())
      AND NOT is_injected
    GROUP BY 1
    UNION ALL
    SELECT date_trunc('day', now()), count(*), count(*) FILTER (WHERE gated_blocked)
    FROM app.feature_events
    WHERE event_ts >= date_trunc('day', now()) AND NOT is_injected
)
SELECT
    d.day,
    coalesce(a.attempts, 0) AS attempts,
    coalesce(a.gated_attempts, 0) AS gated_attempts
FROM days d
LEFT JOIN attempts a USING (day);

-- Billing: single rows, so history and live rows share the tables. Injected rows
-- and test-mode payments are data errors, so they are left out here.

CREATE VIEW dash.billing_daily AS
WITH days AS (
    SELECT generate_series(
        date_trunc('day', now()) - interval '29 days',
        date_trunc('day', now()),
        interval '1 day'
    ) AS day
),
since AS (
    SELECT date_trunc('day', now()) - interval '29 days' AS ts
),
invoiced AS (
    SELECT date_trunc('day', event_ts) AS day, sum(total_usd) AS invoiced_usd
    FROM app.invoices
    WHERE event_ts >= (SELECT ts FROM since) AND NOT is_injected
    GROUP BY 1
),
paid AS (
    SELECT
        date_trunc('day', event_ts) AS day,
        count(*) FILTER (WHERE status = 'succeeded') AS payments_succeeded,
        count(*) FILTER (WHERE status = 'failed') AS payments_failed
    FROM app.payments
    WHERE event_ts >= (SELECT ts FROM since)
      AND NOT is_test
      AND NOT is_injected
    GROUP BY 1
)
SELECT
    d.day,
    coalesce(i.invoiced_usd, 0) AS invoiced_usd,
    coalesce(p.payments_succeeded, 0) AS payments_succeeded,
    coalesce(p.payments_failed, 0) AS payments_failed,
    p.payments_failed::numeric / nullif(p.payments_succeeded + p.payments_failed, 0)
        AS failure_rate
FROM days d
LEFT JOIN invoiced i USING (day)
LEFT JOIN paid p USING (day);

CREATE VIEW dash.billing_summary AS
WITH p AS (
    SELECT
        count(*) FILTER (WHERE status = 'succeeded') AS succeeded,
        count(*) FILTER (WHERE status = 'failed') AS failed
    FROM app.payments
    WHERE event_ts > now() - interval '7 days'
      AND event_ts <= now()
      AND NOT is_test
      AND NOT is_injected
)
SELECT
    (
        SELECT coalesce(sum(total_usd), 0)
        FROM app.invoices
        WHERE event_ts > now() - interval '7 days'
          AND event_ts <= now()
          AND NOT is_injected
    ) AS invoiced_usd_7d,
    p.succeeded AS payments_succeeded_7d,
    p.failed AS payments_failed_7d,
    p.failed::numeric / nullif(p.succeeded + p.failed, 0) AS failure_rate_7d
FROM p;

-- Outliers (row 4): recall per injected anomaly type and precision per flag type.
-- These include injected rows by design.

CREATE VIEW dash.outlier_recall AS
WITH anomalies AS (
    SELECT
        a.*,
        CASE WHEN a.is_demo THEN 'live' ELSE 'baseline' END AS scope,
        CASE a.anomaly_type
            WHEN 'activity_spike' THEN 'activity_spike'
            WHEN 'session_spike' THEN 'session_rate'
            WHEN 'negative_invoice' THEN 'invalid_amount'
            WHEN 'duplicate_payment' THEN 'duplicate_row'
            WHEN 'failure_burst' THEN 'failure_rate'
        END AS expected_flag
    FROM ops.injected_anomalies a
)
SELECT
    a.scope,
    a.anomaly_type,
    count(*) AS injected,
    count(f.flag_id) AS detected,
    round(count(f.flag_id)::numeric / count(*), 3) AS recall
FROM anomalies a
LEFT JOIN ops.outlier_flags f
    ON f.table_name = a.table_name AND f.row_key = a.row_key AND f.flag_type = a.expected_flag
GROUP BY 1, 2
ORDER BY 1, 2;

CREATE VIEW dash.flag_precision AS
WITH matched AS (
    SELECT
        f.flag_type,
        EXISTS (
            SELECT 1 FROM ops.injected_anomalies a
            WHERE a.table_name = f.table_name AND a.row_key = f.row_key
        ) AS injected
    FROM ops.outlier_flags f
)
SELECT
    flag_type,
    count(*) AS flags,
    count(*) FILTER (WHERE injected) AS on_injected_rows,
    count(*) FILTER (WHERE NOT injected) AS natural_hits,
    CASE WHEN flag_type IN ('activity_spike', 'session_rate', 'invalid_amount',
                            'duplicate_row', 'failure_rate')
         THEN round(count(*) FILTER (WHERE injected)::numeric / count(*), 3)
    END AS precision
FROM matched
GROUP BY 1
ORDER BY 1;

-- Model (row 5).

CREATE VIEW dash.model_metrics AS
SELECT
    resolved_trials,
    max(rolling_auc) FILTER (WHERE model_name = 'logreg') AS logreg_auc,
    max(rolling_auc) FILTER (WHERE model_name = 'points') AS points_auc,
    max(rolling_log_loss) FILTER (WHERE model_name = 'logreg') AS logreg_log_loss,
    max(rolling_log_loss) FILTER (WHERE model_name = 'points') AS points_log_loss
FROM ops.model_metrics
GROUP BY resolved_trials
ORDER BY resolved_trials;

CREATE VIEW dash.model_summary AS
SELECT
    o.model_name,
    o.phase,
    count(*) AS trials_resolved,
    round(avg(o.label), 3) AS conversion_rate,
    round(avg(o.probability)::numeric, 3) AS mean_prediction,
    (
        SELECT round(m.cumulative_auc::numeric, 3)
        FROM ops.model_metrics m
        WHERE m.model_name = o.model_name
        ORDER BY m.resolved_trials DESC
        LIMIT 1
    ) AS cumulative_auc
FROM ops.trial_outcomes o
GROUP BY 1, 2
ORDER BY 1, 2;

-- Open trials: started, not past trial end + 3 days, and not yet resolved.
CREATE VIEW dash.top_open_trials AS
WITH open AS (
    SELECT account_id, trial_started_at, trial_ends_at
    FROM app.accounts
    WHERE trial_started_at IS NOT NULL
      AND trial_started_at <= now()
      AND trial_ends_at + interval '3 days' >= now()
      AND NOT is_injected
      AND NOT EXISTS (
          SELECT 1 FROM ops.trial_outcomes t WHERE t.account_id = accounts.account_id
      )
),
latest AS (
    SELECT DISTINCT ON (p.account_id, p.model_name)
        p.account_id, p.model_name, p.probability, p.created_at
    FROM ops.predictions p
    JOIN open USING (account_id)
    ORDER BY p.account_id, p.model_name, p.prediction_id DESC
)
SELECT
    l.account_id,
    round(l.probability::numeric, 3) AS probability,
    round(b.probability::numeric, 3) AS points_probability,
    o.trial_started_at,
    o.trial_ends_at,
    l.created_at AS updated_at
FROM latest l
JOIN open o USING (account_id)
LEFT JOIN latest b ON b.account_id = l.account_id AND b.model_name = 'points'
WHERE l.model_name = 'logreg'
ORDER BY l.probability DESC
LIMIT 10;
