package com.knibel.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

/**
 * Entity representing a single outbox message stored in the SQL outbox table.
 *
 * <p>Each row corresponds to one Kafka message that needs to be (or has been) published.
 * The table is expected to be created with a schema such as:
 *
 * <pre>{@code
 * CREATE TABLE outbox_message (
 *     id          UUID        PRIMARY KEY,
 *     topic       VARCHAR(255) NOT NULL,
 *     message_key VARCHAR(255),
 *     payload     TEXT        NOT NULL,
 *     created_at  TIMESTAMP   NOT NULL,
 *     processed   BOOLEAN     NOT NULL DEFAULT FALSE,
 *     processed_at TIMESTAMP
 * );
 * }</pre>
 */
@Entity
@Table(name = "outbox_message")
public class OutboxMessage {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(nullable = false, updatable = false)
    private UUID id;

    @Column(nullable = false)
    private String topic;

    @Column(name = "message_key")
    private String messageKey;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    @Column(nullable = false)
    private boolean processed = false;

    @Column(name = "processed_at")
    private Instant processedAt;

    protected OutboxMessage() {
    }

    public OutboxMessage(String topic, String messageKey, String payload) {
        this.topic = topic;
        this.messageKey = messageKey;
        this.payload = payload;
    }

    public UUID getId() {
        return id;
    }

    public String getTopic() {
        return topic;
    }

    public String getMessageKey() {
        return messageKey;
    }

    public String getPayload() {
        return payload;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public boolean isProcessed() {
        return processed;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }

    public void markAsProcessed() {
        this.processed = true;
        this.processedAt = Instant.now();
    }
}
