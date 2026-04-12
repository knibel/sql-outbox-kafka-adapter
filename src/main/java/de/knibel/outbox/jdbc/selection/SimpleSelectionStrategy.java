package de.knibel.outbox.jdbc.selection;

import de.knibel.outbox.config.AcknowledgementStrategy;
import de.knibel.outbox.config.OutboxTableProperties;
import de.knibel.outbox.jdbc.SqlIdentifier;

/**
 * Builds an auto-generated SQL SELECT statement based on the table
 * configuration (ID column, acknowledgement strategy, batch size, etc.).
 *
 * <p>Uses {@code FOR UPDATE SKIP LOCKED} for concurrent-safe row claiming
 * and places the row-limiting clause inside a subquery to remain compatible
 * with Oracle (which does not allow {@code FETCH FIRST} together with
 * {@code FOR UPDATE}).
 */
public class SimpleSelectionStrategy implements SelectionStrategy {

    @Override
    public SelectionQuery buildClaimQuery(OutboxTableProperties config, String selectList) {
        String table = SqlIdentifier.quote(config.getTableName());
        String idCol = SqlIdentifier.quote(config.getIdColumn());

        AcknowledgementStrategy strategy = config.getAcknowledgementStrategy();

        if (strategy == AcknowledgementStrategy.STATUS) {
            String statusCol = SqlIdentifier.quote(config.getStatusColumn());
            String sql = "SELECT " + selectList
                    + " FROM " + table
                    + " WHERE " + idCol + " IN ("
                    +   "SELECT " + idCol + " FROM " + table
                    +   " WHERE " + statusCol + " = ?"
                    +   " ORDER BY " + idCol
                    +   " FETCH FIRST ? ROWS ONLY"
                    + ") FOR UPDATE SKIP LOCKED";
            return new SelectionQuery(sql,
                    new Object[]{ config.getPendingValue(), config.getBatchSize() });

        } else if (strategy == AcknowledgementStrategy.TIMESTAMP) {
            String processedAtCol = SqlIdentifier.quote(config.getProcessedAtColumn());
            String sql = "SELECT " + selectList
                    + " FROM " + table
                    + " WHERE " + idCol + " IN ("
                    +   "SELECT " + idCol + " FROM " + table
                    +   " WHERE " + processedAtCol + " IS NULL"
                    +   " ORDER BY " + idCol
                    +   " FETCH FIRST ? ROWS ONLY"
                    + ") FOR UPDATE SKIP LOCKED";
            return new SelectionQuery(sql,
                    new Object[]{ config.getBatchSize() });

        } else {
            // DELETE strategy: every row present is pending
            String sql = "SELECT " + selectList
                    + " FROM " + table
                    + " WHERE " + idCol + " IN ("
                    +   "SELECT " + idCol + " FROM " + table
                    +   " ORDER BY " + idCol
                    +   " FETCH FIRST ? ROWS ONLY"
                    + ") FOR UPDATE SKIP LOCKED";
            return new SelectionQuery(sql,
                    new Object[]{ config.getBatchSize() });
        }
    }
}
