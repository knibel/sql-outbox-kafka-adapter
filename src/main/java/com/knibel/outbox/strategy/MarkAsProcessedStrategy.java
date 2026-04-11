package com.knibel.outbox.strategy;

import com.knibel.outbox.OutboxMessage;
import com.knibel.outbox.OutboxRepository;

/**
 * Processing strategy that <strong>marks</strong> the outbox row as processed
 * (sets {@code processed = true} and records {@code processed_at}) after the message
 * has been successfully published to Kafka.
 *
 * <p>The row is retained in the database, which is useful for auditing or debugging.
 * A separate cleanup/archival job is typically run periodically to purge old processed rows.
 * Use {@link DeleteAfterProcessingStrategy} if you prefer to delete rows immediately.
 */
public class MarkAsProcessedStrategy implements OutboxProcessingStrategy {

    @Override
    public void onSuccess(OutboxMessage message, OutboxRepository repository) {
        message.markAsProcessed();
        repository.save(message);
    }
}
