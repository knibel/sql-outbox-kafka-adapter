package com.knibel.outbox.strategy;

import com.knibel.outbox.OutboxMessage;
import com.knibel.outbox.OutboxRepository;

/**
 * Strategy that defines what happens to an {@link OutboxMessage} after it has been
 * successfully published to Kafka.
 */
public interface OutboxProcessingStrategy {

    /**
     * Called after {@code message} has been successfully transmitted to Kafka.
     *
     * @param message    the message that was just published
     * @param repository the repository used to persist any state change
     */
    void onSuccess(OutboxMessage message, OutboxRepository repository);
}
