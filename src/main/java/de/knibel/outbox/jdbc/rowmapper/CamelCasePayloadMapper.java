package de.knibel.outbox.jdbc.rowmapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.knibel.outbox.config.OutboxTableProperties;
import de.knibel.outbox.jdbc.RowMapperUtil;
import de.knibel.outbox.repository.OutboxRowMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Selects all columns from the row and converts every {@code snake_case}
 * column name to {@code camelCase} in the resulting JSON payload.
 *
 * <p>Example: {@code order_id → orderId}, {@code customer_first_name → customerFirstName}.
 *
 * @deprecated Use {@link CamelCaseRowMapper} instead.
 */
@Deprecated(since = "0.4.0", forRemoval = true)
@SuppressWarnings("removal") // Implements own deprecated PayloadMapper
public class CamelCasePayloadMapper implements PayloadMapper, OutboxRowMapper {

    private final ObjectMapper objectMapper;

    public CamelCasePayloadMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public String mapPayload(ResultSet rs, OutboxTableProperties config) throws SQLException {
        try {
            return RowMapperUtil.buildCamelCasePayload(rs, objectMapper, config.getStaticFields());
        } catch (Exception e) {
            throw new SQLException("Failed to build camelCase payload", e);
        }
    }

    @Override
    public Map<String, Object> mapRow(Map<String, Object> row) {
        Map<String, Object> payload = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            String camelKey = RowMapperUtil.toCamelCase(entry.getKey());
            payload.put(camelKey, entry.getValue());
        }
        return payload;
    }
}
