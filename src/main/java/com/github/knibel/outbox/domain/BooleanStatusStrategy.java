package com.github.knibel.outbox.domain;

import com.github.knibel.outbox.config.OutboxTableProperties;
import com.github.knibel.outbox.jdbc.SqlIdentifier;

/**
 * {@link StatusStrategy} for boolean columns.
 *
 * <p>{@code false} means the row is pending; {@code true} means it has been
 * processed.  There is no IN_PROGRESS state, so this strategy provides
 * <em>at-least-once</em> semantics.
 *
 * <p>Typical schema:
 * <pre>
 * ALTER TABLE orders_outbox ADD COLUMN processed BOOLEAN NOT NULL DEFAULT FALSE;
 * </pre>
 */
public class BooleanStatusStrategy implements StatusStrategy {

    @Override
    public SqlFragment pendingClause(OutboxTableProperties config) {
        String col = SqlIdentifier.quote(config.getStatusColumn());
        return SqlFragment.of(col + " = ?", false);
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
        return SqlFragment.of(col + " = ?", true);
    }

    @Override
    public SqlFragment failedSetFragment(OutboxTableProperties config) {
        // No failed state for BOOLEAN; reset to pending (false) for retry.
        String col = SqlIdentifier.quote(config.getStatusColumn());
        return SqlFragment.of(col + " = ?", false);
    }

    @Override
    public SqlFragment resetSetFragment(OutboxTableProperties config) {
        String col = SqlIdentifier.quote(config.getStatusColumn());
        return SqlFragment.of(col + " = ?", false);
    }

    @Override
    public SqlFragment stuckClause(OutboxTableProperties config) {
        // No IN_PROGRESS state → no stuck-row detection.
        return null;
    }
}
