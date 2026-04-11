package com.github.knibel.outbox.strategy;

import com.github.knibel.outbox.config.OutboxTableProperties;
import com.github.knibel.outbox.jdbc.OutboxRepository;
import java.util.List;

/**
 * Post-publish strategy that <strong>deletes</strong> outbox rows immediately
 * after they have been successfully published to Kafka.
 *
 * <p>Choosing this strategy keeps the outbox table small without requiring a
 * separate cleanup job. Because the delete happens only after the Kafka broker
 * has acknowledged every record in the batch, no messages are lost.
 *
 * <p>Trade-offs compared with {@link MarkDoneStrategy}:
 * <ul>
 *   <li><b>Pro:</b> no need to purge old {@code DONE} rows; table stays lean.
 *   <li><b>Con:</b> no in-table audit trail of sent messages. If you need an
 *       audit trail, use {@link MarkDoneStrategy} and archive {@code DONE} rows
 *       to a separate table.
 * </ul>
 *
 * <p>Enable per table by setting
 * {@code outbox.tables[n].processingStrategy: DELETE} in {@code application.yml}.
 */
public class DeleteAfterPublishStrategy implements PostPublishStrategy {

    @Override
    public void onSuccess(OutboxTableProperties config, List<String> ids, OutboxRepository repository) {
        repository.deleteByIds(config, ids);
    }
}
