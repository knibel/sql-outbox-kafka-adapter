CREATE TABLE IF NOT EXISTS test_outbox (
    id           VARCHAR(36)  PRIMARY KEY,
    aggregate_id VARCHAR(100) NOT NULL,
    payload      TEXT         NOT NULL,
    headers_json TEXT,
    status       VARCHAR(20)  NOT NULL DEFAULT 'PENDING'
);
