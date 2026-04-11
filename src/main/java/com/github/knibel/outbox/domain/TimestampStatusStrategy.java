package com.github.knibel.outbox.domain;

import com.github.knibel.outbox.config.OutboxTableProperties;
import com.github.knibel.outbox.jdbc.SqlIdentifier;

/**
 * {@link StatusStrategy} for nullable timestamp columns.
 *
 * <p>A {@code NULL} value means the row is pending; a non-null timestamp means
 * the row has been processed.  There is no IN_PROGRESS state, so this strategy
 * provides <em>at-least-once</em> semantics.
 *
 * <p>Typical schema:
 * <pre>
 * ALTER TABLE orders_outbox ADD COLUMN processed_at TIMESTAMP WITH TIME ZONE;
 * </pre>
 */
public class TimestampStatusStrategy implements StatusStrategy {

    @Override
    public SqlFragment pendingClause(OutboxTableProperties config) {
        String col = SqlIdentifier.quote(config.getStatusColumn());
        return SqlFragment.noParams(col + " IS NULL");
    }

    @Override
    public boolean hasInProgressState() {
        return false;
    }

    @Override
    public SqlFragment claimSetFragment(OutboxTableProperties config) {
        // Not used – hasInProgressState() returns false.
        return null;
    }

    @Override
    public SqlFragment doneSetFragment(OutboxTableProperties config) {
        String col = SqlIdentifier.quote(config.getStatusColumn());
        // Use SQL CURRENT_TIMESTAMP expression; no bind parameter needed.
        return SqlFragment.noParams(col + " = CURRENT_TIMESTAMP");
    }

    @Override
    public SqlFragment failedSetFragment(OutboxTableProperties config) {
        // No failed state for TIMESTAMP; reset to pending (NULL) for retry.
        String col = SqlIdentifier.quote(config.getStatusColumn());
        return SqlFragment.noParams(col + " = NULL");
    }

    @Override
    public SqlFragment resetSetFragment(OutboxTableProperties config) {
        String col = SqlIdentifier.quote(config.getStatusColumn());
        return SqlFragment.noParams(col + " = NULL");
    }

    @Override
    public SqlFragment stuckClause(OutboxTableProperties config) {
        // No IN_PROGRESS state → no stuck-row detection.
        return null;
    }
}
