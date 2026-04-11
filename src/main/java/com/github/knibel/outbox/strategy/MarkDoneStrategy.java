package com.github.knibel.outbox.strategy;

import com.github.knibel.outbox.config.OutboxTableProperties;
import com.github.knibel.outbox.jdbc.OutboxRepository;
import java.util.List;

/**
 * Post-publish strategy that <strong>marks</strong> successfully published rows
 * as done by updating the status column to the configured {@code doneValue}.
 *
 * <p>This is the <em>default</em> strategy. Rows are retained in the outbox
 * table, which is useful for auditing, debugging, and replay. A separate
 * archival or purge job can remove old {@code DONE} rows at an appropriate
 * cadence.
 *
 * <p>Use {@link DeleteAfterPublishStrategy} if you prefer to keep the table
 * small by removing rows immediately after publication.
 */
public class MarkDoneStrategy implements PostPublishStrategy {

    @Override
    public void onSuccess(OutboxTableProperties config, List<String> ids, OutboxRepository repository) {
        repository.markDone(config, ids);
    }
}
