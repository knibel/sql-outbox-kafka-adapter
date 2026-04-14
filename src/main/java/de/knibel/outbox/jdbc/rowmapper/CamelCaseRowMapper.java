package de.knibel.outbox.jdbc.rowmapper;

import de.knibel.outbox.jdbc.RowMapperUtil;
import de.knibel.outbox.repository.OutboxRowMapper;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Converts all column names from {@code snake_case} to {@code camelCase}
 * and optionally injects static fields.
 *
 * <p>Operates entirely on a plain {@code Map<String, Object>} — no JDBC
 * or Jackson dependency at mapping time.
 *
 * @see CamelCasePayloadMapper
 */
@SuppressWarnings("removal") // Intentionally implements deprecated PayloadMapper during transition
public class CamelCaseRowMapper implements PayloadMapper, OutboxRowMapper {

    private final Map<String, String> staticFields;

    /**
     * @param staticFields constant key-value pairs injected after
     *                     column-based mapping (may be empty, never {@code null})
     */
    public CamelCaseRowMapper(Map<String, String> staticFields) {
        this.staticFields = staticFields;
    }

    @Override
    public Map<String, Object> mapRow(Map<String, Object> row) {
        Map<String, Object> payload = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            String camelKey = RowMapperUtil.toCamelCase(entry.getKey());
            payload.put(camelKey, entry.getValue());
        }
        RowMapperUtil.applyStaticFields(payload, staticFields);
        return payload;
    }

    // ── PayloadMapper bridge (deprecated path) ───────────────────────────────

    @Override
    public String mapPayload(java.sql.ResultSet rs,
                             de.knibel.outbox.config.OutboxTableProperties config)
            throws java.sql.SQLException {
        try {
            return RowMapperUtil.buildCamelCasePayload(rs,
                    new com.fasterxml.jackson.databind.ObjectMapper(), config.getStaticFields());
        } catch (Exception e) {
            throw new java.sql.SQLException("Failed to build camelCase payload", e);
        }
    }
}
