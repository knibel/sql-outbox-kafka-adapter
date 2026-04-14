package de.knibel.outbox.jdbc.rowmapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.knibel.outbox.config.OutboxTableProperties;
import de.knibel.outbox.jdbc.RowMapperUtil;
import de.knibel.outbox.repository.OutboxRowMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

/**
 * Uses the explicit {@code fieldMappings} and optional regex-based
 * {@code columnPatterns} to map source columns to target JSON paths.
 * Supports nested objects via dot-separated paths, data type conversion,
 * date/datetime formatting, and value mapping.
 *
 * @deprecated Use {@link CustomFieldRowMapper} instead.
 */
@Deprecated(since = "0.4.0", forRemoval = true)
@SuppressWarnings("removal") // Implements own deprecated PayloadMapper
public class CustomFieldPayloadMapper implements PayloadMapper, OutboxRowMapper {

    private final ObjectMapper objectMapper;

    public CustomFieldPayloadMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public String mapPayload(ResultSet rs, OutboxTableProperties config) throws SQLException {
        try {
            return RowMapperUtil.buildCustomPayload(
                    rs, config.getFieldMappings(), config.getCompiledColumnPatterns(),
                    config.getListMappings(), objectMapper, config.getStaticFields());
        } catch (Exception e) {
            throw new SQLException("Failed to build custom payload", e);
        }
    }

    /**
     * Not meaningful for this mapper — configuration is not available at
     * construction time.  Use {@link CustomFieldRowMapper} for proper
     * {@code Map}-based mapping.
     */
    @Override
    public Map<String, Object> mapRow(Map<String, Object> row) {
        return Collections.emptyMap();
    }
}
