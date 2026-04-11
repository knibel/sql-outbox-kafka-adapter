CREATE TABLE "test_outbox" (
    "id"           VARCHAR2(36)  NOT NULL,
    "aggregate_id" VARCHAR2(100) NOT NULL,
    "payload"      CLOB          NOT NULL,
    "headers_json" CLOB,
    "status"       VARCHAR2(20)  DEFAULT 'PENDING' NOT NULL,
    "processed_at" TIMESTAMP,
    CONSTRAINT test_outbox_pk PRIMARY KEY ("id")
);
