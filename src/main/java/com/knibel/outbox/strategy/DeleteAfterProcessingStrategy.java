package com.knibel.outbox.strategy;

import com.knibel.outbox.OutboxMessage;
import com.knibel.outbox.OutboxRepository;

/**
 * Processing strategy that <strong>deletes</strong> the outbox row from the database
 * immediately after the message has been successfully published to Kafka.
 *
 * <p>This keeps the outbox table small and avoids the need for a separate cleanup job,
 * but means there is no persistent audit trail of sent messages inside the outbox table.
 * Use {@link MarkAsProcessedStrategy} if you need to retain the rows for auditing.
 */
public class DeleteAfterProcessingStrategy implements OutboxProcessingStrategy {

    @Override
    public void onSuccess(OutboxMessage message, OutboxRepository repository) {
        repository.delete(message);
    }
}
