package de.knibel.outbox.domain;

import java.util.Map;

/**
 * Immutable value object representing one outbox row mapped to a Kafka record.
 *
 * @param id       String representation of the row's primary-key value.
 *                 Used as the idempotency anchor and for bulk UPDATE statements.
 * @param kafkaKey Kafka record key (may be null if not configured).
 * @param topic    Kafka topic to publish to (never null).
 * @param payload  Kafka record value (JSON string from the payload column).
 * @param headers  Kafka record headers parsed from the optional headers column
 *                 (empty map if the column is absent or null in the row).
 */
public record OutboxRecord(
        String id,
        String kafkaKey,
        String topic,
        String payload,
        Map<String, String> headers) {
}
