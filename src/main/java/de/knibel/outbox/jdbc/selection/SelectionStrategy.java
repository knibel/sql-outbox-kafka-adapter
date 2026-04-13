package de.knibel.outbox.jdbc.selection;

import de.knibel.outbox.config.OutboxTableProperties;

/**
 * Strategy for building the SQL query used to select pending outbox rows.
 *
 * <p>Implementations decide <em>how</em> rows are selected (e.g.
 * auto-generated SQL vs. user-provided custom query).  All columns of the
 * row are always selected ({@code SELECT *}).
 *
 * @see SimpleSelectionStrategy
 * @see CustomQuerySelectionStrategy
 */
public interface SelectionStrategy {

    /**
     * Builds the SQL SELECT statement and its bind parameters for claiming
     * a batch of pending outbox rows.
     *
     * @param config table-specific configuration
     * @return a {@link SelectionQuery} containing the SQL and parameters
     */
    SelectionQuery buildClaimQuery(OutboxTableProperties config);
}
