CREATE TABLE IF NOT EXISTS test_outbox (
    id           VARCHAR(36)  PRIMARY KEY,
    aggregate_id VARCHAR(100) NOT NULL,
    payload      TEXT         NOT NULL,
    headers_json TEXT,
    status       VARCHAR(20)  NOT NULL DEFAULT 'PENDING',
    processed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS skip_delay_outbox (
    id      VARCHAR(36) PRIMARY KEY,
    payload TEXT        NOT NULL,
    status  VARCHAR(20) NOT NULL DEFAULT 'PENDING'
);

CREATE TABLE IF NOT EXISTS camel_case_outbox (
    id             VARCHAR(36)  PRIMARY KEY,
    order_id       VARCHAR(100) NOT NULL,
    customer_name  VARCHAR(100),
    total_amount   NUMERIC(10,2),
    status         VARCHAR(20)  NOT NULL DEFAULT 'PENDING'
);

CREATE TABLE IF NOT EXISTS custom_mapping_outbox (
    id              VARCHAR(36)  PRIMARY KEY,
    order_id        VARCHAR(100) NOT NULL,
    customer_name   VARCHAR(100),
    customer_email  VARCHAR(200),
    city            VARCHAR(100),
    total_amount    NUMERIC(10,2),
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP,
    order_date      DATE,
    status_code     INTEGER DEFAULT 1,
    status          VARCHAR(20)  NOT NULL DEFAULT 'PENDING'
);

-- Table for custom query integration test
CREATE TABLE IF NOT EXISTS custom_query_outbox (
    id         VARCHAR(36)  PRIMARY KEY,
    payload    TEXT         NOT NULL,
    status     VARCHAR(20)  NOT NULL DEFAULT 'PENDING',
    priority   INTEGER      DEFAULT 0
);

-- Status table used in cross-table custom query test
CREATE TABLE IF NOT EXISTS custom_query_status (
    batch_id   VARCHAR(36)  PRIMARY KEY,
    ready      BOOLEAN      NOT NULL DEFAULT FALSE
);

-- Table for custom acknowledgement integration test
CREATE TABLE IF NOT EXISTS custom_ack_outbox (
    id              VARCHAR(36)  PRIMARY KEY,
    payload         TEXT         NOT NULL,
    ack_flag        INTEGER      NOT NULL DEFAULT 0,
    ack_timestamp   TIMESTAMP
);

-- Table for static fields integration test
CREATE TABLE IF NOT EXISTS static_fields_outbox (
    id              VARCHAR(36)  PRIMARY KEY,
    order_id        VARCHAR(100) NOT NULL,
    customer_name   VARCHAR(100),
    amount          NUMERIC(10,2),
    status          VARCHAR(20)  NOT NULL DEFAULT 'PENDING'
);

-- Tables for RPS_ADR / RPS_ADR_STATUS integration test
-- (replacing RpsAdrRepository / RpsAdrStatusRepository with the adapter)
-- ext_load_id and ext_job_id are VARCHAR so that the adapter's String-typed
-- id bind parameter is compatible with PostgreSQL's strict type system.
-- In Oracle (the production DB), NUMBER columns accept VARCHAR bind parameters
-- via implicit casting, so the production config keeps numeric column types.
CREATE TABLE IF NOT EXISTS rps_adr (
    ext_load_id           VARCHAR(36)   PRIMARY KEY,
    bk_stichtag_dat       DATE          NOT NULL,
    ext_job_id            VARCHAR(36)   NOT NULL,
    bk_adr_nummer         BIGINT        NOT NULL,
    kunde_ausgefallen_knz BOOLEAN       DEFAULT FALSE,
    doppelkunde_knz       BOOLEAN       DEFAULT FALSE,
    umsatz_btr            NUMERIC(15,2),
    kne_obligo_btr        NUMERIC(15,2),
    kne_obligo_ts         TIMESTAMP
);

-- dwh_job_id_batch is VARCHAR for the same reason as above.
CREATE TABLE IF NOT EXISTS rps_adr_status (
    dwh_job_id_batch    VARCHAR(36)   PRIMARY KEY,
    stichtag_dat        DATE          NOT NULL,
    anz_adr             BIGINT,
    daten_abgeholt_jn   INTEGER       NOT NULL DEFAULT 0,
    daten_abholung_dat  TIMESTAMP
);
