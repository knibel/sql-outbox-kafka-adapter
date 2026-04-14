package de.knibel.outbox.jdbc.rowmapper;

import de.knibel.outbox.repository.OutboxRowMapper;
import java.util.Collections;
import java.util.Map;

/**
 * Returns the raw value of a single configured payload column as the
 * entire payload.
 *
 * <p>The returned map contains a single entry whose key is the
 * configured column name and whose value is the column's raw value
 * (typically a pre-serialized JSON string).
 *
 * <p>This is the default row-mapping strategy and preserves backward
 * compatibility with the original behaviour.
 *
 * @see PayloadColumnMapper
 */
@SuppressWarnings("removal") // Intentionally implements deprecated PayloadMapper during transition
public class PayloadColumnRowMapper implements PayloadMapper, OutboxRowMapper {

    private final String payloadColumn;

    /**
     * @param payloadColumn the column whose value is the raw JSON payload
     */
    public PayloadColumnRowMapper(String payloadColumn) {
        this.payloadColumn = payloadColumn;
    }

    @Override
    public Map<String, Object> mapRow(Map<String, Object> row) {
        Object value = row.get(payloadColumn);
        return Collections.singletonMap(payloadColumn, value);
    }

    /**
     * Returns the configured payload column name.  Used by the adapter
     * layer to extract the raw JSON string from the map returned by
     * {@link #mapRow(Map)}.
     */
    public String getPayloadColumn() {
        return payloadColumn;
    }

    // ── PayloadMapper bridge (deprecated path) ───────────────────────────────

    @Override
    public String mapPayload(java.sql.ResultSet rs,
                             de.knibel.outbox.config.OutboxTableProperties config)
            throws java.sql.SQLException {
        return rs.getString(config.getPayloadColumn());
    }
}
