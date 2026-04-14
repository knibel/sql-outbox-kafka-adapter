package de.knibel.outbox.jdbc.rowmapper;

import de.knibel.outbox.config.OutboxTableProperties;
import de.knibel.outbox.repository.OutboxRowMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

/**
 * Reads the Kafka record value from a single pre-serialized JSON column
 * ({@code payloadColumn}).
 *
 * <p>This is the default row-mapping strategy and preserves backward
 * compatibility with the original behaviour.
 *
 * @deprecated Use {@link PayloadColumnRowMapper} instead.
 */
@Deprecated(since = "0.4.0", forRemoval = true)
@SuppressWarnings("removal") // Implements own deprecated PayloadMapper
public class PayloadColumnMapper implements PayloadMapper, OutboxRowMapper {

    @Override
    public String mapPayload(ResultSet rs, OutboxTableProperties config) throws SQLException {
        return rs.getString(config.getPayloadColumn());
    }

    /**
     * Not meaningful for this mapper — always returns an empty map.
     * Use {@link PayloadColumnRowMapper} for proper {@code Map}-based mapping.
     */
    @Override
    public Map<String, Object> mapRow(Map<String, Object> row) {
        return Collections.emptyMap();
    }
}
