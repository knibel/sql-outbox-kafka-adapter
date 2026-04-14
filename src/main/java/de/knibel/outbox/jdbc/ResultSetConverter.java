package de.knibel.outbox.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Converts a positioned {@link ResultSet} row to a plain
 * {@code Map<String, Object>} (column-label → value).
 *
 * <p>This is the single point where JDBC result-set metadata is accessed
 * in the row-mapping pipeline.  Downstream consumers (such as
 * {@link de.knibel.outbox.repository.OutboxRowMapper} implementations)
 * operate purely on the returned map and have no JDBC dependency.
 */
public final class ResultSetConverter {

    private ResultSetConverter() {}

    /**
     * Reads all columns from the current row and returns them as a
     * {@link LinkedHashMap} preserving insertion order.
     *
     * <p>Column labels (as returned by
     * {@link ResultSetMetaData#getColumnLabel(int)}) are used as map
     * keys, preserving the original casing from the database / query.
     *
     * @param rs positioned result-set row (must not be closed)
     * @return column-label → value map; never {@code null}
     * @throws SQLException if a database access error occurs
     */
    public static Map<String, Object> toMap(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();
        Map<String, Object> row = new LinkedHashMap<>(columnCount);
        for (int i = 1; i <= columnCount; i++) {
            row.put(meta.getColumnLabel(i), rs.getObject(i));
        }
        return row;
    }
}
