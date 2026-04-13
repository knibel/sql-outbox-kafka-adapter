package de.knibel.outbox.jdbc.rowmapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.knibel.outbox.config.OutboxTableProperties;
import de.knibel.outbox.jdbc.RowMapperUtil;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Uses the explicit {@code fieldMappings} configuration to map source columns
 * to target JSON paths.  Supports nested objects via dot-separated paths,
 * data type conversion, date/datetime formatting, and value mapping.
 */
public class CustomFieldPayloadMapper implements PayloadMapper {

    private final ObjectMapper objectMapper;

    public CustomFieldPayloadMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public String mapPayload(ResultSet rs, OutboxTableProperties config) throws SQLException {
        try {
            return RowMapperUtil.buildCustomPayload(
                    rs, config.getFieldMappings(), objectMapper, config.getStaticFields());
        } catch (Exception e) {
            throw new SQLException("Failed to build custom payload", e);
        }
    }
}
