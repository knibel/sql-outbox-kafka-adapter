# sql-outbox-kafka-adapter

A Spring Boot auto-configuration library that implements the **Transactional Outbox Pattern**:
messages are first written to a SQL outbox table inside the same database transaction as the
business operation, and then a background poller reads those rows and publishes them reliably
to Kafka.

---

## How it works

```
Business service
    │
    ├─► INSERT INTO outbox_message (topic, key, payload) -- same DB transaction
    │
OutboxPoller (scheduled)
    │
    ├─► SELECT … FROM outbox_message WHERE processed = false LIMIT n
    ├─► KafkaTemplate.send(topic, key, payload).get()  -- synchronous, confirmed delivery
    └─► Processing strategy (configurable)
            ├─► DELETE  strategy: DELETE FROM outbox_message WHERE id = ?
            └─► MARK_AS_PROCESSED strategy: UPDATE outbox_message SET processed = true …
```

---

## Database schema

```sql
CREATE TABLE outbox_message (
    id           UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    topic        VARCHAR(255) NOT NULL,
    message_key  VARCHAR(255),
    payload      TEXT         NOT NULL,
    created_at   TIMESTAMP    NOT NULL DEFAULT now(),
    processed    BOOLEAN      NOT NULL DEFAULT FALSE,
    processed_at TIMESTAMP
);
```

---

## Getting started

### 1. Add the dependency

```xml
<dependency>
    <groupId>com.knibel</groupId>
    <artifactId>sql-outbox-kafka-adapter</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>
```

### 2. Configure Spring Data JPA and Spring Kafka in your application as usual.

### 3. Configure the outbox adapter (optional — defaults shown)

```yaml
outbox:
  polling-interval-ms: 1000        # how often to poll for new messages (ms)
  batch-size: 100                  # max messages per polling cycle
  processing-strategy: delete      # delete | mark-as-processed
```

### 4. Write to the outbox table inside your business transaction

```java
@Transactional
public void createOrder(Order order) {
    orderRepository.save(order);
    outboxRepository.save(new OutboxMessage(
        "orders",                           // Kafka topic
        order.getId().toString(),           // Kafka message key
        objectMapper.writeValueAsString(order)  // JSON payload
    ));
}
```

The rest is handled automatically by the poller.

---

## Processing strategies

| Strategy | Behaviour | Best for |
|---|---|---|
| `delete` *(default)* | Deletes the outbox row after successful Kafka delivery | Low-volume tables, no audit requirement |
| `mark-as-processed` | Sets `processed = true` and `processed_at` on the row | Audit trail, debugging, deferred archival |

### Delete strategy

The `DeleteAfterProcessingStrategy` keeps the outbox table small by immediately removing
each row once the corresponding Kafka message has been acknowledged:

```
outbox_message row
        │
        ├─► kafkaTemplate.send(…).get()  ← blocks until broker ACK
        └─► outboxRepository.delete(message)
```

This is the default. No separate cleanup job is needed.

### Mark-as-processed strategy

The `MarkAsProcessedStrategy` retains the row for auditing but marks it so the poller
won't pick it up again:

```
outbox_message row
        │
        ├─► kafkaTemplate.send(…).get()
        └─► message.markAsProcessed()
            outboxRepository.save(message)
```

A periodic archival/purge job should be run to delete old processed rows when using this
strategy.

---

## Customisation

All beans are annotated with `@ConditionalOnMissingBean`, so you can override any of them
by declaring your own bean:

```java
@Bean
public OutboxProcessingStrategy myCustomStrategy() {
    return (message, repository) -> {
        // your logic here
    };
}
```
