package de.knibel.outbox.jdbc.selection;

import de.knibel.outbox.config.OutboxTableProperties;

/**
 * Uses a user-provided custom SQL query to select pending outbox rows.
 *
 * <p>The query is executed as-is — no additional {@code FOR UPDATE SKIP LOCKED}
 * or row-limiting is appended.  The query must return all columns required by
 * the configured row-mapping strategy.
 */
public class CustomQuerySelectionStrategy implements SelectionStrategy {

    @Override
    public SelectionQuery buildClaimQuery(OutboxTableProperties config, String selectList) {
        return new SelectionQuery(config.getCustomQuery(), new Object[0]);
    }
}
