package de.knibel.outbox.repository;

import java.util.Map;

/**
 * Strategy for mapping a single outbox row to a structured payload map.
 *
 * <p>This interface is <em>not</em> dependent on JDBC, Jackson, or any
 * specific persistence / serialization technology.  It receives the raw
 * row data as a plain {@code Map<String, Object>} (column-name → value)
 * and returns a structured (possibly nested) map that represents the
 * JSON payload to be published.
 *
 * <p>Each implementation is pre-configured at construction time; the
 * {@link #mapRow(Map)} method does not receive configuration parameters,
 * keeping the call-site minimal and the mapping logic self-contained.
 *
 * <p>This design allows implementations to be unit-tested with plain
 * maps (no {@code ResultSet} mocking required) and makes the mapping
 * layer extractable as a standalone library with zero JDBC/Jackson
 * dependencies.
 *
 * @see de.knibel.outbox.jdbc.rowmapper.PayloadColumnRowMapper
 * @see de.knibel.outbox.jdbc.rowmapper.CamelCaseRowMapper
 * @see de.knibel.outbox.jdbc.rowmapper.CustomFieldRowMapper
 * @see de.knibel.outbox.jdbc.rowmapper.MappingRuleRowMapper
 */
public interface OutboxRowMapper {

    /**
     * Maps a single row (represented as a column-name → value map)
     * to a structured payload map.
     *
     * <p>The input map preserves the original column-name casing from
     * the database.  Implementations that need case-insensitive lookups
     * are responsible for normalising keys as needed.
     *
     * @param row column-name → raw value from the database row
     *            (never {@code null}, but individual values may be {@code null})
     * @return structured nested map representing the payload;
     *         never {@code null}
     */
    Map<String, Object> mapRow(Map<String, Object> row);
}
