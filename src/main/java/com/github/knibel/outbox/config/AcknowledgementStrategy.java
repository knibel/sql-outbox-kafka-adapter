package com.github.knibel.outbox.config;

/**
 * Defines how the outbox adapter acknowledges that a row has been
 * successfully published to Kafka.
 *
 * <ul>
 *   <li>{@link #STATUS} – updates a status column to the configured
 *       {@code doneValue} (e.g. {@code "DONE"}).  Rows with
 *       {@code statusColumn = pendingValue} are selected as pending.
 *       Requires {@code statusColumn}, {@code pendingValue}, and
 *       {@code doneValue} to be configured.</li>
 *   <li>{@link #DELETE} – deletes the row from the table after publishing.
 *       All rows present in the table are treated as pending; no additional
 *       column configuration is needed.</li>
 *   <li>{@link #TIMESTAMP} – writes the current timestamp into a dedicated
 *       {@code processedAtColumn}.  Rows where that column is {@code NULL}
 *       are selected as pending.  Requires {@code processedAtColumn} to be
 *       configured.</li>
 * </ul>
 */
public enum AcknowledgementStrategy {

    /**
     * Mark rows as processed by updating {@code statusColumn} to
     * {@code doneValue}.  Pending rows are identified by
     * {@code statusColumn = pendingValue}.
     */
    STATUS,

    /**
     * Mark rows as processed by deleting them from the outbox table.
     * No filter column is required; every row present is considered pending.
     */
    DELETE,

    /**
     * Mark rows as processed by writing {@code NOW()} into
     * {@code processedAtColumn}.  Rows with a {@code NULL} value in that
     * column are considered pending.
     */
    TIMESTAMP,

    /**
     * Mark rows as processed by executing a user-provided SQL statement
     * ({@code customAcknowledgementQuery}).  The statement receives the
     * row's {@code idColumn} value as a single bind parameter ({@code ?}).
     */
    CUSTOM
}
