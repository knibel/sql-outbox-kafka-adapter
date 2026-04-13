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

#### Row mapping strategy

Controls how SQL row columns are mapped to the Kafka record value (payload).

| Strategy | Behaviour |
|---|---|
| `PAYLOAD_COLUMN` _(default)_ | Reads a pre-serialized JSON string from `payloadColumn`. |
| `TO_CAMEL_CASE` | Selects all columns and converts each `snake_case` column name to `camelCase` in the resulting JSON payload. No `payloadColumn` needed. |
| `CUSTOM` | Uses the explicit `fieldMappings` and/or regex-based `columnPatterns` and/or `listMappings` to map source columns to target JSON fields. Supports nested objects via dot-separated paths, data type conversion, date/datetime formatting, value mapping, and collecting column groups into JSON arrays. |

| Property | Default | Description |
|---|---|---|
| `rowMappingStrategy` | `PAYLOAD_COLUMN` | One of `PAYLOAD_COLUMN`, `TO_CAMEL_CASE`, `CUSTOM`. |
| `fieldMappings` | _(empty)_ | Used when `rowMappingStrategy` is `CUSTOM`. A map of source SQL column names to field mapping objects. At least one of `fieldMappings`, `columnPatterns`, or `listMappings` must be non-empty. |
| `columnPatterns` | _(empty)_ | Used when `rowMappingStrategy` is `CUSTOM`. A map of Java regex patterns to field mapping objects. Each pattern is matched against the full column label; the `name` may contain back-references (`$1`, `$2`, …). Explicit `fieldMappings` take precedence over patterns. At least one of `fieldMappings`, `columnPatterns`, or `listMappings` must be non-empty. |
| `listMappings` | _(empty)_ | Used when `rowMappingStrategy` is `CUSTOM`. A map of target JSON paths to list-mapping objects. Collects columns matching regex patterns into JSON arrays, grouped by the first capturing group. See below for details. At least one of `fieldMappings`, `columnPatterns`, or `listMappings` must be non-empty. |

##### Field mapping properties (`fieldMappings.<column>` and `columnPatterns.<pattern>`)

Entries in both `fieldMappings` and `columnPatterns` share the same mapping properties:

| Property | Required | Description |
|---|---|---|
| `name` | **yes** | Target JSON field path. Dot-separated paths produce nested objects (e.g. `customer.address.city`). In `columnPatterns`, back-references (`$1`, `$2`, …) may be used to incorporate captured groups from the pattern. In `listMappings`, this is the property name within each array element. |
| `dataType` | no | Target data type for conversion. One of: `STRING`, `INTEGER`, `LONG`, `DOUBLE`, `BOOLEAN`, `DECIMAL`, `DATE`, `DATETIME`. When omitted, the JDBC driver's default Java mapping is used. |
| `format` | when `dataType` is `DATE` or `DATETIME` | A `DateTimeFormatter` pattern (e.g. `yyyy-MM-dd`, `yyyy-MM-dd'T'HH:mm:ss`). Accepts `java.sql.Date`, `java.sql.Timestamp`, `LocalDate`, `LocalDateTime`, `Instant`, and `java.util.Date`. |
| `valueMappings` | no | A map of raw database values (as strings) to replacement output values. Applied _before_ `dataType` conversion. Useful for translating integer codes to enum strings (e.g. `"1": ACTIVE`). Unmapped values pass through unchanged. |

##### List mapping properties (`listMappings.<targetPath>`)

Each entry in `listMappings` collects columns matching regex patterns into a
JSON array at the specified target path.  Columns sharing the same first
capturing-group value are merged into one array element.

| Property | Required | Description |
|---|---|---|
| `keyProperty` | no | Property name in each array element whose value is the first capturing-group match (i.e. the column-name suffix). When omitted, the captured group is used only for grouping and is not included as a property. |
| `patterns` | **yes** | A map of Java regex patterns to field mapping objects.  Each pattern must contain at least one capturing group.  The first group determines which array element the column value belongs to.  The `name` in the mapping specifies the property name within that element.  `dataType`, `format`, and `valueMappings` are supported. |

Precedence: `fieldMappings` > `columnPatterns` > `listMappings`.  A column
already handled by a higher-precedence mapping is excluded from list processing.

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

### TO_CAMEL_CASE row mapping

Converts all column names from `snake_case` to `camelCase` automatically:

```yaml
outbox:
  kafka:
    bootstrapServers: localhost:9092
  tables:
    - tableName: orders_outbox
      staticTopic: orders
      rowMappingStrategy: TO_CAMEL_CASE
      acknowledgementStrategy: STATUS
      statusColumn: status
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

### CUSTOM row mapping with nested objects

Map specific columns to target JSON paths, including nested objects via
dot-separated paths:

```yaml
outbox:
  kafka:
    bootstrapServers: localhost:9092
  tables:
    - tableName: orders_outbox
      staticTopic: orders
      rowMappingStrategy: CUSTOM
      acknowledgementStrategy: STATUS
      statusColumn: status
      fieldMappings:
        order_id:
          name: orderId
        customer_name:
          name: customer.name
        customer_email:
          name: customer.email
        city:
          name: customer.address.city
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

### CUSTOM row mapping with data type conversion

Use `dataType` to convert column values to specific types in the JSON output:

```yaml
outbox:
  kafka:
    bootstrapServers: localhost:9092
  tables:
    - tableName: orders_outbox
      staticTopic: orders
      rowMappingStrategy: CUSTOM
      fieldMappings:
        order_id:
          name: orderId
          dataType: STRING
        total_amount:
          name: totalAmount
          dataType: DOUBLE
        is_active:
          name: active
          dataType: BOOLEAN
```

### CUSTOM row mapping with date/datetime formatting

Format temporal columns using `DateTimeFormatter` patterns:

```yaml
outbox:
  kafka:
    bootstrapServers: localhost:9092
  tables:
    - tableName: orders_outbox
      staticTopic: orders
      rowMappingStrategy: CUSTOM
      fieldMappings:
        order_id:
          name: orderId
        created_at:
          name: createdAt
          dataType: DATETIME
          format: "yyyy-MM-dd'T'HH:mm:ss"
        order_date:
          name: orderDate
          dataType: DATE
          format: "yyyy-MM-dd"
```

A row with `created_at=2024-01-15 10:30:45` and `order_date=2024-01-15` produces:

```json
{"orderId":"ORD-001","createdAt":"2024-01-15T10:30:45","orderDate":"2024-01-15"}
```

### CUSTOM row mapping with value mapping

Translate raw database values (e.g. integer codes) to human-readable strings:

```yaml
outbox:
  kafka:
    bootstrapServers: localhost:9092
  tables:
    - tableName: orders_outbox
      staticTopic: orders
      rowMappingStrategy: CUSTOM
      fieldMappings:
        order_id:
          name: orderId
        status_code:
          name: status
          valueMappings:
            "1": ACTIVE
            "2": INACTIVE
            "3": DELETED
        priority:
          name: priority
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

### CUSTOM row mapping with column patterns (generic prefix mapping)

Use `columnPatterns` to map groups of columns generically using Java regular
expressions.  The `name` may contain back-references (`$1`, `$2`, …) that are
resolved against the capturing groups of the matched column name.  This avoids
having to list every column individually in `fieldMappings`.

```yaml
outbox:
  kafka:
    bootstrapServers: localhost:9092
  tables:
    - tableName: orders_outbox
      staticTopic: orders
      rowMappingStrategy: CUSTOM
      columnPatterns:
        "neu_(.*)":
          name: "neu.$1"
        "alt_(.*)":
          name: "alt.$1"
```

A row with columns `neu_preis=10.0`, `neu_menge=2`, `alt_preis=8.0`, and
`alt_menge=3` produces:

```json
{
  "neu": { "preis": 10.0, "menge": 2 },
  "alt": { "preis": 8.0,  "menge": 3 }
}
```

You can combine `columnPatterns` with explicit `fieldMappings`.  Explicit
mappings always take precedence: a column already covered by `fieldMappings`
is skipped when evaluating patterns.

```yaml
columnPatterns:
  "neu_(.*)":
    name: "neu.$1"
fieldMappings:
  neu_preis:
    name: specialPrice   # explicit mapping wins for this column
    dataType: DOUBLE
```

### CUSTOM row mapping with list mappings (paired columns → JSON array)

Use `listMappings` to collect groups of paired columns into a JSON array.
Each unique first capturing-group value produces one array element.  This
is ideal for tables with paired prefix columns (e.g. `new_*`/`old_*`):

```yaml
outbox:
  kafka:
    bootstrapServers: localhost:9092
  tables:
    - tableName: product_audit
      staticTopic: product-audit
      rowMappingStrategy: CUSTOM
      fieldMappings:
        audit_ts:
          name: timestamp
          dataType: DATETIME
          format: "yyyy-MM-dd'T'HH:mm:ss"
        action:
          name: action
          valueMappings:
            "I": INSERT
            "U": UPDATE
            "D": DELETE
        product_key:
          name: productKey
      listMappings:
        modifications:
          keyProperty: attribute
          patterns:
            "new_(.*)":
              name: after
            "old_(.*)":
              name: before
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

You can combine `listMappings` with `fieldMappings` and `columnPatterns`.
Precedence order: `fieldMappings` > `columnPatterns` > `listMappings`.
A column already handled by a higher-precedence mapping is excluded from
list processing.
