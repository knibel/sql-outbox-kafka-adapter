# sql-outbox-kafka-adapter

A Spring Boot service that polls one or more SQL outbox tables and publishes
each row to Apache Kafka.  It uses `FOR UPDATE SKIP LOCKED` so multiple
instances can run side-by-side without processing the same row twice, and an
idempotent (or optionally transactional) Kafka producer for reliable delivery.

Supported databases: **PostgreSQL 9.5+**, **Oracle 12c+**.

---

## Quick start

Add the following to your `application.yml` (adjust values to match your
environment):

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/mydb
    username: myuser
    password: mypassword

outbox:
  kafka:
    bootstrapServers: localhost:9092
  tables:
    - tableName: orders_outbox
      staticTopic: orders
      pollIntervalMs: 500
```

The matching outbox table (STATUS strategy, the default):

```sql
CREATE TABLE orders_outbox (
    id         VARCHAR(36)   PRIMARY KEY,
    payload    TEXT          NOT NULL,
    status     VARCHAR(16)   NOT NULL DEFAULT 'PENDING'
);
```

---

## Configuration reference

All properties live under the `outbox` prefix.

### Kafka settings (`outbox.kafka`)

| Property | Default | Description |
|---|---|---|
| `bootstrapServers` | `localhost:9092` | Comma-separated list of Kafka broker addresses. |
| `transactionalIdPrefix` | _(empty)_ | When non-empty, the producer runs in transactional (exactly-once) mode. Each table gets a unique transactional ID derived from this prefix and the table name. Requires Kafka 2.5+. Leave empty for idempotent-only mode. |
| `producerProperties` | _(empty map)_ | Any additional Kafka producer properties (e.g. `compression.type`, `linger.ms`) passed verbatim to `KafkaProducer`. |

### Table settings (`outbox.tables[]`)

Each entry in the `tables` list configures one outbox table.

#### Polling

| Property | Default | Description |
|---|---|---|
| `tableName` | _(required)_ | Name of the outbox table. |
| `pollIntervalMs` | `1000` | How often to poll for new rows, in milliseconds. |
| `batchSize` | `100` | Maximum number of rows processed per poll cycle. |

#### Column mapping

| Property | Default | Description |
|---|---|---|
| `idColumn` | `id` | Primary-key column. Also used as the Kafka record key when `keyColumn` is not set. |
| `keyColumn` | _(none)_ | Column whose value becomes the Kafka record key. Falls back to `idColumn` when absent. |
| `payloadColumn` | `payload` | Column containing the Kafka record value (typically a JSON string). |
| `headersColumn` | _(none)_ | Column containing a JSON object whose entries become Kafka record headers (e.g. `{"source":"orders-service"}`). Omit to send no headers. |
| `topicColumn` | _(none)_ | Column used to determine the Kafka topic per row (enables per-row topic routing). When absent, `staticTopic` is used. |
| `staticTopic` | _(none)_ | Kafka topic used for all rows when `topicColumn` is not set. |

#### Acknowledgement strategy

After a row is successfully published, the adapter marks it as done according to
the configured `acknowledgementStrategy`:

| Strategy | Behaviour | Required columns |
|---|---|---|
| `STATUS` _(default)_ | Updates `statusColumn` from `pendingValue` to `doneValue`. | `statusColumn`, `pendingValue`, `doneValue` |
| `DELETE` | Deletes the row from the table. All rows present are treated as pending. | _(none)_ |
| `TIMESTAMP` | Writes the current timestamp into `processedAtColumn`. Rows with `NULL` in that column are treated as pending. | `processedAtColumn` |

##### STATUS strategy properties (defaults shown)

| Property | Default | Description |
|---|---|---|
| `acknowledgementStrategy` | `STATUS` | Strategy to use. |
| `statusColumn` | `status` | Column that tracks processing status. |
| `pendingValue` | `PENDING` | Value that marks a row as not yet processed. |
| `doneValue` | `DONE` | Value that marks a row as successfully processed. |

##### TIMESTAMP strategy properties

| Property | Default | Description |
|---|---|---|
| `acknowledgementStrategy` | _(set to `TIMESTAMP`)_ | Strategy to use. |
| `processedAtColumn` | _(required)_ | Column into which `NOW()` is written after publishing. |

---

## Examples

### STATUS strategy (default)

```yaml
outbox:
  kafka:
    bootstrapServers: localhost:9092
  tables:
    - tableName: orders_outbox
      idColumn: id
      keyColumn: aggregate_id
      payloadColumn: payload
      headersColumn: headers_json
      staticTopic: orders
      pollIntervalMs: 500
      batchSize: 50
      acknowledgementStrategy: STATUS
      statusColumn: status
      pendingValue: PENDING
      doneValue: DONE
```

```sql
CREATE TABLE orders_outbox (
    id            VARCHAR(36)  PRIMARY KEY,
    aggregate_id  VARCHAR(36)  NOT NULL,
    payload       TEXT         NOT NULL,
    headers_json  TEXT,
    status        VARCHAR(16)  NOT NULL DEFAULT 'PENDING'
);
```

### DELETE strategy

```yaml
outbox:
  kafka:
    bootstrapServers: localhost:9092
  tables:
    - tableName: events_outbox
      staticTopic: events
      acknowledgementStrategy: DELETE
```

```sql
CREATE TABLE events_outbox (
    id       VARCHAR(36) PRIMARY KEY,
    payload  TEXT        NOT NULL
);
```

### TIMESTAMP strategy

```yaml
outbox:
  kafka:
    bootstrapServers: localhost:9092
  tables:
    - tableName: notifications_outbox
      staticTopic: notifications
      acknowledgementStrategy: TIMESTAMP
      processedAtColumn: processed_at
```

```sql
CREATE TABLE notifications_outbox (
    id            VARCHAR(36)              PRIMARY KEY,
    payload       TEXT                     NOT NULL,
    processed_at  TIMESTAMP WITH TIME ZONE
);
```

### Per-row topic routing

Set `topicColumn` to route each row to a different Kafka topic:

```yaml
outbox:
  kafka:
    bootstrapServers: localhost:9092
  tables:
    - tableName: domain_events_outbox
      topicColumn: kafka_topic
```

```sql
CREATE TABLE domain_events_outbox (
    id           VARCHAR(36) PRIMARY KEY,
    payload      TEXT        NOT NULL,
    kafka_topic  VARCHAR(255) NOT NULL,
    status       VARCHAR(16) NOT NULL DEFAULT 'PENDING'
);
```

### Multiple tables

```yaml
outbox:
  kafka:
    bootstrapServers: localhost:9092
  tables:
    - tableName: orders_outbox
      staticTopic: orders
    - tableName: payments_outbox
      staticTopic: payments
      acknowledgementStrategy: DELETE
      pollIntervalMs: 2000
```

### Exactly-once delivery (Kafka transactions)

```yaml
outbox:
  kafka:
    bootstrapServers: localhost:9092
    transactionalIdPrefix: my-app-outbox
  tables:
    - tableName: orders_outbox
      staticTopic: orders
```

### Extra producer properties

```yaml
outbox:
  kafka:
    bootstrapServers: localhost:9092
    producerProperties:
      compression.type: lz4
      linger.ms: "5"
  tables:
    - tableName: orders_outbox
      staticTopic: orders
```
