# How it works

Five diagrams, from the big picture down to a single take. They render directly on GitHub.

## 1. System overview

Where the data lives and which component writes and reads each part.

```mermaid
flowchart LR
    subgraph local["Laptop / home server"]
        P[("Prepared Parquet<br/>data/prepared")]
        B["make baseline<br/>backfill + time shift"]
        S["Live simulator<br/>make demo"]
        I["Anomaly injector"]
        O["Outlier scorer"]
        M["Trial model<br/>River logistic regression"]
    end

    subgraph azure["Azure, Canada Central"]
        subgraph pg["PostgreSQL Flexible Server (B1ms)"]
            APP[("app schema<br/>11 tables")]
            OPS[("ops schema<br/>flags, predictions,<br/>metrics, offsets")]
            DASH[("dash schema<br/>views")]
        end
        G["Azure Managed Grafana<br/>5 s refresh"]
    end

    P --> B --> APP
    P --> S --> APP
    I --> APP
    APP -->|poll new rows| O --> OPS
    APP -->|poll new rows| M --> OPS
    APP --> DASH
    OPS --> DASH
    DASH -->|read-only role| G
```

## 2. Data preparation and loading

How raw simulation output becomes a stream that reads as "today".

```mermaid
flowchart TD
    A["Original simulation export<br/>(not in repo)"] --> B["Local prepare step<br/>re-key IDs, map names,<br/>dedupe sync history"]
    B --> C[("Prepared Parquet<br/>9 tables, committed")]
    C --> D{"Where does the<br/>row's date fall?"}
    D -->|"before the cutoff"| E["Baseline backfill<br/>shift dates so history<br/>ends yesterday"]
    D -->|"on the cutoff day"| F["Live simulator<br/>split daily totals into<br/>individual events"]
    D -->|"after the cutoff"| X["Not used"]
    E --> G[("app tables<br/>about 1.25M rows")]
    F --> H["Emit about 500 events/min<br/>event_ts = now"]
    H --> G
```

## 3. One live take

What happens during `make demo`, second by second.

```mermaid
sequenceDiagram
    autonumber
    participant Sim as Live simulator
    participant DB as PostgreSQL (app)
    participant Out as Outlier scorer
    participant Mod as Trial model
    participant Ops as PostgreSQL (ops)
    participant Gr as Grafana

    loop every second
        Sim->>DB: insert next events (event_ts = now)
    end
    loop every 5 seconds
        Out->>DB: read rows past my offset
        Out->>Ops: write flags, advance offset
        Mod->>DB: read rows past my offset
        Mod->>Ops: write trial predictions, advance offset
        Gr->>DB: query dash views
    end
    Note over Sim,DB: 1:00 session spike<br/>2:00 duplicate payment<br/>3:00 negative invoice<br/>4:00 failure burst
    Note over Out,Ops: each anomaly flagged about 2.5 s after insert
```

## 4. Demo lifecycle

Why every take starts from the same state.

```mermaid
stateDiagram-v2
    [*] --> Empty
    Empty --> Baseline: make baseline (once per recording day)
    Baseline --> Streaming: make demo
    Streaming --> Stopped: Ctrl-C
    Stopped --> Streaming: make demo (continues the take)
    Stopped --> Baseline: make demo-reset (about 3 s)
    Baseline --> Baseline: make demo-reset (no-op)

    note right of Baseline
        History up to yesterday
        Model checkpoint saved
        Consumer offsets saved
    end note
    note right of Stopped
        Demo rows flagged is_demo
        Reset deletes them and
        restores the checkpoint
    end note
```

## 5. Trial model loop

Predict first, learn later. The model never sees an outcome before it happens.

```mermaid
flowchart TD
    A["New session or feature event<br/>for an open trial"] --> B["Update that trial's features<br/>users, devices, active days,<br/>features tried, prior personal device"]
    B --> C["Predict conversion probability"]
    C --> D[("ops.predictions")]
    E["Trial ends:<br/>plan change arrives"] --> F["Look up the day-7 prediction"]
    D --> F
    F --> G["Score it: log loss, AUC"]
    G --> H[("ops.model_metrics")]
    F --> I["learn_one: update weights"]
    I -.->|next trials| C
```

## Schema at a glance

The core relationships. The generated `docs/img/erd.png` is the exact version with every column and foreign key.

```mermaid
erDiagram
    ACCOUNTS ||--o{ USERS : has
    ACCOUNTS ||--o{ DEVICES : has
    USERS ||--o{ DEVICES : registers
    ACCOUNTS ||--o{ SESSIONS : has
    USERS ||--o{ SESSIONS : starts
    DEVICES ||--o{ SESSIONS : "used in"
    ACCOUNTS ||--o{ FEATURE_EVENTS : has
    USERS ||--o{ FEATURE_EVENTS : triggers
    ACCOUNTS ||--o{ USAGE_DAILY : "summarized in"
    ACCOUNTS ||--o{ FEATURE_USAGE_DAILY : "summarized in"
    ACCOUNTS ||--o{ LICENSE_EVENTS : has
    ACCOUNTS ||--o{ PLAN_CHANGES : has
    ACCOUNTS ||--o{ INVOICES : billed
    INVOICES ||--o{ PAYMENTS : "paid by"
    ACCOUNTS ||--o{ PAYMENTS : makes
```
