package de.knibel.outbox.jdbc.rowmapper;

import de.knibel.outbox.config.OutboxTableProperties;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Reads the Kafka record value from a single pre-serialized JSON column
 * ({@code payloadColumn}).
 *
 * <p>This is the default row-mapping strategy and preserves backward
 * compatibility with the original behaviour.
 */
public class PayloadColumnMapper implements PayloadMapper {

    @Override
    public String mapPayload(ResultSet rs, OutboxTableProperties config) throws SQLException {
        return rs.getString(config.getPayloadColumn());
    }
}
