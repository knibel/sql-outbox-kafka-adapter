package de.knibel.outbox.jdbc.rowmapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.knibel.outbox.config.OutboxTableProperties;
import de.knibel.outbox.jdbc.RowMapperUtil;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Selects all columns from the row and converts every {@code snake_case}
 * column name to {@code camelCase} in the resulting JSON payload.
 *
 * <p>Example: {@code order_id → orderId}, {@code customer_first_name → customerFirstName}.
 */
public class CamelCasePayloadMapper implements PayloadMapper {

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
}
