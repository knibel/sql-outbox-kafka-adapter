package de.knibel.outbox.jdbc.rowmapper;

import de.knibel.outbox.config.OutboxTableProperties;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Strategy for mapping SQL result-set rows to JSON payload strings.
 *
 * <p>Each implementation corresponds to a
 * {@link de.knibel.outbox.config.RowMappingStrategy} configuration option
 * and defines how the payload is extracted from the result set.
 *
 * <p>All columns of the row are always selected ({@code SELECT *});
 * the mapper only controls <em>how</em> columns are mapped to a JSON payload.
 *
 * @deprecated Use {@link de.knibel.outbox.repository.OutboxRowMapper} instead.
 *             This interface couples mapping logic to JDBC ({@code ResultSet})
 *             and serialization (returns JSON string).  The new
 *             {@code OutboxRowMapper} works with plain {@code Map<String, Object>}
 *             and is persistence/serialization-independent.
 *
 * @see PayloadColumnMapper
 * @see CamelCasePayloadMapper
 * @see CustomFieldPayloadMapper
 * @see MappingRulePayloadMapper
 * @see de.knibel.outbox.repository.OutboxRowMapper
 */
@Deprecated(since = "0.4.0", forRemoval = true)
public interface PayloadMapper {

    /**
     * Maps the payload portion of the current result-set row to a JSON string.
     *
     * @param rs     positioned result-set row
     * @param config table-specific configuration
     * @return JSON string representing the Kafka record value
     * @throws SQLException if a database access error occurs
     */
    String mapPayload(ResultSet rs, OutboxTableProperties config) throws SQLException;
}
