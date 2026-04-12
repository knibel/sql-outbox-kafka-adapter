package de.knibel.outbox.jdbc.rowmapper;

import de.knibel.outbox.config.OutboxTableProperties;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Strategy for mapping SQL result-set rows to JSON payload strings.
 *
 * <p>Each implementation corresponds to a
 * {@link de.knibel.outbox.config.RowMappingStrategy} configuration option
 * and handles both the SELECT column list and the payload extraction.
 *
 * @see PayloadColumnMapper
 * @see CamelCasePayloadMapper
 * @see CustomFieldPayloadMapper
 */
public interface PayloadMapper {

    /**
     * Builds the comma-separated SELECT column list required by this mapper.
     *
     * <p>The list includes both metadata columns (id, key, topic, headers)
     * and any payload-specific columns.
     *
     * @param config table-specific configuration
     * @return the column list for the SQL SELECT clause
     */
    String buildSelectList(OutboxTableProperties config);

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
