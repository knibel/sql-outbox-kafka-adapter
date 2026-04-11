package com.github.knibel.outbox.strategy;

import com.github.knibel.outbox.config.OutboxTableProperties;
import com.github.knibel.outbox.jdbc.OutboxRepository;
import java.util.List;

/**
 * Strategy applied to outbox rows <em>after</em> they have been successfully
 * published to Kafka.
 *
 * <p>Two built-in implementations are provided:
 * <ul>
 *   <li>{@link MarkDoneStrategy} – updates the row's status column to the
 *       configured {@code doneValue} (default behaviour, retains the row for
 *       auditing / debugging).
 *   <li>{@link DeleteAfterPublishStrategy} – deletes the row immediately,
 *       keeping the outbox table small without a separate cleanup job.
 * </ul>
 *
 * <p>Custom strategies can be implemented and wired into
 * {@link com.github.knibel.outbox.config.OutboxTableProperties} by subclassing
 * or by directly constructing the desired implementation.
 */
public interface PostPublishStrategy {

    /**
     * Called once per successful poll cycle, after all records in the batch
     * have been acknowledged by the Kafka broker.
     *
     * @param config     configuration of the outbox table being processed.
     * @param ids        IDs of the rows that were just published.
     * @param repository the JDBC repository used to apply the post-publish action.
     */
    void onSuccess(OutboxTableProperties config, List<String> ids, OutboxRepository repository);
}
