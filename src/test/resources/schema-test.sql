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

-- Tables for the cross-join two-table integration test.
-- load_id and batch_ref_id are VARCHAR so that the adapter's String-typed
-- id bind parameter is compatible with PostgreSQL's strict type system.
-- In Oracle (the production DB), NUMBER columns accept VARCHAR bind parameters
-- via implicit casting, so the production config keeps numeric column types.
CREATE TABLE IF NOT EXISTS data_records (
    load_id          VARCHAR(36)   PRIMARY KEY,
    reference_date   DATE          NOT NULL,
    batch_ref_id     VARCHAR(36)   NOT NULL,
    entity_id        BIGINT        NOT NULL,
    default_flag     BOOLEAN       DEFAULT FALSE,
    duplicate_flag   BOOLEAN       DEFAULT FALSE,
    amount           NUMERIC(15,2),
    liability_amount NUMERIC(15,2),
    liability_ts     TIMESTAMP
);

-- batch_id is VARCHAR for the same reason as load_id above.
CREATE TABLE IF NOT EXISTS batch_status (
    batch_id        VARCHAR(36)   PRIMARY KEY,
    reference_date  DATE          NOT NULL,
    record_count    BIGINT,
    fetched_flag    INTEGER       NOT NULL DEFAULT 0,
    fetched_at      TIMESTAMP
);

-- Table for column-to-list mapping integration test
-- Simulates a product audit trail where each row tracks changes to
-- multiple attributes via paired new_*/old_* columns.
CREATE TABLE IF NOT EXISTS product_audit (
    audit_id        VARCHAR(36)   PRIMARY KEY,
    audit_ts        TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    action          CHAR(1)       NOT NULL DEFAULT 'U',
    product_key     VARCHAR(100)  NOT NULL,
    new_price       NUMERIC(10,2),
    old_price       NUMERIC(10,2),
    new_stock       INTEGER,
    old_stock       INTEGER,
    new_label       VARCHAR(200),
    old_label       VARCHAR(200),
    status          VARCHAR(20)   NOT NULL DEFAULT 'PENDING'
);
