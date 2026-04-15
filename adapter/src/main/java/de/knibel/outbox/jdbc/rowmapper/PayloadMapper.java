package de.knibel.outbox.jdbc.rowmapper;

import de.knibel.outbox.config.OutboxTableProperties;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Strategy for mapping SQL result-set rows to JSON payload strings.
 *
 * <p>The primary implementation is
 * {@link MappingRulePayloadMapper}, which processes the ordered
 * {@link de.knibel.outbox.config.MappingRule} list from configuration.
 *
 * <p>All columns of the row are always selected ({@code SELECT *});
 * the mapper only controls <em>how</em> columns are mapped to a JSON payload.
 *
 * @see MappingRulePayloadMapper
 */
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
