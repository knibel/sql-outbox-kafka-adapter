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

#### Row mapping

Controls how SQL row columns are mapped to the Kafka record value (payload).

By default (when no `mappings` list is configured), the adapter reads a
pre-serialized JSON string from `payloadColumn`.  For customised mapping
behaviour, use the `mappings` list.

Each mapping rule has the following properties:

| Property | Required | Description |
|---|---|---|
| `source` | when `value` is absent | Source SQL column name, a regex wrapped in `/…/` (e.g. `/new_(.*)/`), or the wildcard `*`. Mutually exclusive with `value`. |
| `value` | when `source` is absent | A static string value to inject. Mutually exclusive with `source`. |
| `target` | **yes** | Target JSON field path (dot-separated for nesting). Special values: `_raw` (pass-through from column) and `_camelCase` (auto-convert all remaining columns). |
| `dataType` | no | Target data type for conversion. One of: `STRING`, `INTEGER`, `LONG`, `DOUBLE`, `BOOLEAN`, `DECIMAL`, `DATE`, `DATETIME`. |
| `format` | when `dataType` is `DATE` or `DATETIME` | A `DateTimeFormatter` pattern (e.g. `yyyy-MM-dd`, `yyyy-MM-dd'T'HH:mm:ss`). |
| `valueMappings` | no | A map of raw database values (as strings) to replacement output values. Applied _before_ `dataType` conversion. |
| `group` | no | Enables array-grouping for regex sources. See below. |

##### Group properties (`group`)

When present on a mapping rule with a regex `source`, columns matching the
pattern are collected into a JSON array at the rule's `target` path.

| Property | Required | Description |
|---|---|---|
| `by` | **yes** | Capture-group expression (e.g. `$1`) used to correlate columns into the same array element. |
| `keyProperty` | no | Property name for injecting the captured group value into each array element. |
| `property` | **yes** | Property name within each element where the column value is placed. |

Rules are evaluated top-to-bottom.  Once a column is claimed by a rule, later
rules skip it.

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

### CamelCase row mapping

Converts all column names from `snake_case` to `camelCase` automatically:

```yaml
outbox:
  kafka:
    bootstrapServers: localhost:9092
  tables:
    - tableName: orders_outbox
      staticTopic: orders
      acknowledgementStrategy: STATUS
      statusColumn: status
      mappings:
        - source: "*"
          target: _camelCase
```

```sql
CREATE TABLE orders_outbox (
    id             VARCHAR(36)    PRIMARY KEY,
    order_id       VARCHAR(100)   NOT NULL,
    customer_name  VARCHAR(100),
    total_amount   NUMERIC(10,2),
    status         VARCHAR(16)    NOT NULL DEFAULT 'PENDING'
);
```

A row with `order_id='ORD-001'`, `customer_name='John Doe'`, `total_amount=99.95`
produces:

```json
{"id":"…","orderId":"ORD-001","customerName":"John Doe","totalAmount":99.95,"status":"DONE"}
```

### Explicit column mapping with nested objects

Map specific columns to target JSON paths, including nested objects via
dot-separated paths:

```yaml
outbox:
  kafka:
    bootstrapServers: localhost:9092
  tables:
    - tableName: orders_outbox
      staticTopic: orders
      acknowledgementStrategy: STATUS
      statusColumn: status
      mappings:
        - source: order_id
          target: orderId
        - source: customer_name
          target: customer.name
        - source: customer_email
          target: customer.email
        - source: city
          target: customer.address.city
```

Produces:

```json
{
  "orderId": "ORD-001",
  "customer": {
    "name": "John Doe",
    "email": "john@example.com",
    "address": {
      "city": "Berlin"
    }
  }
}
```

### Data type conversion

Use `dataType` to convert column values to specific types in the JSON output:

```yaml
outbox:
  kafka:
    bootstrapServers: localhost:9092
  tables:
    - tableName: orders_outbox
      staticTopic: orders
      mappings:
        - source: order_id
          target: orderId
          dataType: STRING
        - source: total_amount
          target: totalAmount
          dataType: DOUBLE
        - source: is_active
          target: active
          dataType: BOOLEAN
```

### Date/datetime formatting

Format temporal columns using `DateTimeFormatter` patterns:

```yaml
outbox:
  kafka:
    bootstrapServers: localhost:9092
  tables:
    - tableName: orders_outbox
      staticTopic: orders
      mappings:
        - source: order_id
          target: orderId
        - source: created_at
          target: createdAt
          dataType: DATETIME
          format: "yyyy-MM-dd'T'HH:mm:ss"
        - source: order_date
          target: orderDate
          dataType: DATE
          format: "yyyy-MM-dd"
```

A row with `created_at=2024-01-15 10:30:45` and `order_date=2024-01-15` produces:

```json
{"orderId":"ORD-001","createdAt":"2024-01-15T10:30:45","orderDate":"2024-01-15"}
```

### Value mapping

Translate raw database values (e.g. integer codes) to human-readable strings:

```yaml
outbox:
  kafka:
    bootstrapServers: localhost:9092
  tables:
    - tableName: orders_outbox
      staticTopic: orders
      mappings:
        - source: order_id
          target: orderId
        - source: status_code
          target: status
          valueMappings:
            "1": ACTIVE
            "2": INACTIVE
            "3": DELETED
        - source: priority
          target: priority
          valueMappings:
            "0": LOW
            "1": MEDIUM
            "2": HIGH
```

A row with `status_code=1` and `priority=2` produces:

```json
{"orderId":"ORD-001","status":"ACTIVE","priority":"HIGH"}
```

Values not present in the mapping pass through unchanged.

### Regex patterns (generic prefix mapping)

Use regex sources (wrapped in `/…/`) to map groups of columns generically.
The `target` may contain back-references (`$1`, `$2`, …) that are resolved
against the capturing groups.

```yaml
outbox:
  kafka:
    bootstrapServers: localhost:9092
  tables:
    - tableName: orders_outbox
      staticTopic: orders
      mappings:
        - source: /neu_(.*)/
          target: "neu.$1"
        - source: /alt_(.*)/
          target: "alt.$1"
```

A row with columns `neu_preis=10.0`, `neu_menge=2`, `alt_preis=8.0`, and
`alt_menge=3` produces:

```json
{
  "neu": { "preis": 10.0, "menge": 2 },
  "alt": { "preis": 8.0,  "menge": 3 }
}
```

You can combine regex patterns with explicit column mappings.  Explicit
mappings always take precedence: a column already handled by an earlier rule
is skipped.

```yaml
mappings:
  - source: neu_preis
    target: specialPrice     # explicit mapping wins for this column
    dataType: DOUBLE
  - source: /neu_(.*)/
    target: "neu.$1"
```

### Group mapping (paired columns → JSON array)

Use `group` rules to collect paired columns into a JSON array.
Each unique capturing-group value produces one array element.  This
is ideal for tables with paired prefix columns (e.g. `new_*`/`old_*`):

```yaml
outbox:
  kafka:
    bootstrapServers: localhost:9092
  tables:
    - tableName: product_audit
      staticTopic: product-audit
      mappings:
        - source: audit_ts
          target: timestamp
          dataType: DATETIME
          format: "yyyy-MM-dd'T'HH:mm:ss"
        - source: action
          target: action
          valueMappings:
            "I": INSERT
            "U": UPDATE
            "D": DELETE
        - source: product_key
          target: productKey
        - source: /new_(.*)/
          target: modifications
          group:
            by: $1
            keyProperty: attribute
            property: after
        - source: /old_(.*)/
          target: modifications
          group:
            by: $1
            keyProperty: attribute
            property: before
```

```sql
CREATE TABLE product_audit (
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
```

A row with `action='U'`, `product_key='SKU-42'`, `new_price=29.99`,
`old_price=24.99`, `new_stock=150`, `old_stock=100` produces:

```json
{
  "timestamp": "2024-03-15T10:30:00",
  "action": "UPDATE",
  "productKey": "SKU-42",
  "modifications": [
    {"attribute": "price", "after": 29.99, "before": 24.99},
    {"attribute": "stock", "after": 150,   "before": 100},
    {"attribute": "label", "after": "Widget Pro", "before": "Widget"}
  ]
}
```

You can combine group rules with explicit and regex rules.  Rules are
evaluated top-to-bottom; a column already handled by an earlier rule is
excluded from later rules.
