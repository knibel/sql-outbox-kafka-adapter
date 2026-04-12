package de.knibel.outbox.jdbc.selection;

/**
 * Encapsulates a SQL query string and its bind parameters.
 *
 * @param sql    the SQL SELECT statement
 * @param params bind parameters for the SQL statement (may be empty)
 */
public record SelectionQuery(String sql, Object[] params) {}
