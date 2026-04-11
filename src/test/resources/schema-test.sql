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
