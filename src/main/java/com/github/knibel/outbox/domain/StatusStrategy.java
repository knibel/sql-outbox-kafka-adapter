package com.github.knibel.outbox.domain;

import com.github.knibel.outbox.config.OutboxTableProperties;

/**
 * Encapsulates the SQL logic for reading and updating a specific status column,
 * allowing different column types (enum string, timestamp, boolean) to be
 * handled transparently by {@link com.github.knibel.outbox.jdbc.OutboxRepository}.
 *
 * <p>All returned {@link SqlFragment}s contain only {@code ?} placeholders;
 * no literal values are concatenated into SQL to prevent injection.
 */
public interface StatusStrategy {

    /**
     * SQL WHERE clause that selects rows not yet processed.
     *
     * <p>Examples:
     * <ul>
     *   <li>ENUM:      {@code "status" = ?}  (param: pendingValue)
     *   <li>TIMESTAMP: {@code "processed_at" IS NULL}  (no params)
     *   <li>BOOLEAN:   {@code "done" = ?}  (param: false)
     * </ul>
     */
    SqlFragment pendingClause(OutboxTableProperties config);

    /**
     * Whether this strategy supports an explicit IN_PROGRESS state.
     *
     * <p>When {@code true}, {@link com.github.knibel.outbox.jdbc.OutboxRepository}
     * will atomically claim rows to IN_PROGRESS before publishing to Kafka,
     * enabling stuck-row recovery and effectively-once semantics.
     *
     * <p>When {@code false} (TIMESTAMP / BOOLEAN), rows go directly from pending
     * to done; the strategy provides at-least-once semantics only.
     */
    boolean hasInProgressState();

    /**
     * SQL SET expression for the status column when claiming a row as in-progress.
     * Called only when {@link #hasInProgressState()} returns {@code true}.
     *
     * <p>Example: {@code "status" = ?}  (param: inProgressValue)
     */
    SqlFragment claimSetFragment(OutboxTableProperties config);

    /**
     * SQL SET expression for the status column when marking a row as done.
     *
     * <p>Examples:
     * <ul>
     *   <li>ENUM:      {@code "status" = ?}  (param: doneValue)
     *   <li>TIMESTAMP: {@code "processed_at" = CURRENT_TIMESTAMP}  (no params)
     *   <li>BOOLEAN:   {@code "done" = ?}  (param: true)
     * </ul>
     */
    SqlFragment doneSetFragment(OutboxTableProperties config);

    /**
     * SQL SET expression for the status column when a row cannot be delivered.
     * May be the same as {@link #doneSetFragment} for strategies without a
     * failed state (e.g. they revert to pending for automatic retry instead).
     */
    SqlFragment failedSetFragment(OutboxTableProperties config);

    /**
     * SQL SET expression for resetting a row back to pending (used by
     * stuck-row recovery).  Returns {@code null} if not applicable for this
     * strategy.
     */
    SqlFragment resetSetFragment(OutboxTableProperties config);

    /**
     * SQL WHERE clause that identifies stuck rows (claimed to IN_PROGRESS but
     * not completed within {@link OutboxTableProperties#getStuckTtlSeconds()}).
     *
     * <p>Returns {@code null} when stuck-row recovery is not supported
     * (e.g. TIMESTAMP / BOOLEAN strategies, or when
     * {@link OutboxTableProperties#getUpdatedAtColumn()} is not configured).
     */
    SqlFragment stuckClause(OutboxTableProperties config);
}
