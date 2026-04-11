package com.github.knibel.outbox.domain;

import com.github.knibel.outbox.config.OutboxTableProperties;
import com.github.knibel.outbox.jdbc.SqlIdentifier;

/**
 * {@link StatusStrategy} for string/enum columns.
 *
 * <p>Provides three states: PENDING → IN_PROGRESS → DONE (or FAILED).
 * This is the only strategy that supports stuck-row recovery and provides
 * <em>effectively-once</em> semantics.
 */
public class EnumStatusStrategy implements StatusStrategy {

    @Override
    public SqlFragment pendingClause(OutboxTableProperties config) {
        String col = SqlIdentifier.quote(config.getStatusColumn());
        return SqlFragment.of(col + " = ?", config.getPendingValue());
    }

    @Override
    public boolean hasInProgressState() {
        return true;
    }

    @Override
    public SqlFragment claimSetFragment(OutboxTableProperties config) {
        String col = SqlIdentifier.quote(config.getStatusColumn());
        return SqlFragment.of(col + " = ?", config.getInProgressValue());
    }

    @Override
    public SqlFragment doneSetFragment(OutboxTableProperties config) {
        String col = SqlIdentifier.quote(config.getStatusColumn());
        return SqlFragment.of(col + " = ?", config.getDoneValue());
    }

    @Override
    public SqlFragment failedSetFragment(OutboxTableProperties config) {
        String col = SqlIdentifier.quote(config.getStatusColumn());
        return SqlFragment.of(col + " = ?", config.getFailedValue());
    }

    @Override
    public SqlFragment resetSetFragment(OutboxTableProperties config) {
        String col = SqlIdentifier.quote(config.getStatusColumn());
        return SqlFragment.of(col + " = ?", config.getPendingValue());
    }

    @Override
    public SqlFragment stuckClause(OutboxTableProperties config) {
        if (config.getUpdatedAtColumn() == null) {
            return null;
        }
        String statusCol = SqlIdentifier.quote(config.getStatusColumn());
        String updatedCol = SqlIdentifier.quote(config.getUpdatedAtColumn());
        // Compute the cutoff timestamp in Java so the WHERE clause uses only
        // standard SQL parameter binding (no vendor-specific INTERVAL syntax).
        long cutoffMillis = System.currentTimeMillis() - config.getStuckTtlSeconds() * 1_000L;
        java.sql.Timestamp cutoff = new java.sql.Timestamp(cutoffMillis);
        String sql = statusCol + " = ? AND " + updatedCol + " < ?";
        return SqlFragment.of(sql, config.getInProgressValue(), cutoff);
    }
}
