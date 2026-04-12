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
