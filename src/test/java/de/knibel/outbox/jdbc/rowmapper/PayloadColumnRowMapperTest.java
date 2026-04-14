package de.knibel.outbox.jdbc.rowmapper;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link PayloadColumnRowMapper}.
 * Tests use plain {@code Map<String, Object>} — no ResultSet mocking.
 */
class PayloadColumnRowMapperTest {

    @Test
    void mapRow_returnsPayloadColumnValue() {
        PayloadColumnRowMapper mapper = new PayloadColumnRowMapper("payload");

        Map<String, Object> row = Map.of(
                "id", "123",
                "payload", "{\"key\":\"value\"}",
                "status", "PENDING");

        Map<String, Object> result = mapper.mapRow(row);

        assertThat(result).hasSize(1)
                          .containsEntry("payload", "{\"key\":\"value\"}");
    }

    @Test
    void mapRow_returnsNullValueWhenColumnMissing() {
        PayloadColumnRowMapper mapper = new PayloadColumnRowMapper("payload");

        Map<String, Object> row = Map.of("id", "123", "status", "PENDING");
        // payload column not present → null in result

        Map<String, Object> result = mapper.mapRow(row);

        assertThat(result).hasSize(1)
                          .containsKey("payload");
        assertThat(result.get("payload")).isNull();
    }

    @Test
    void mapRow_customColumnName() {
        PayloadColumnRowMapper mapper = new PayloadColumnRowMapper("data");

        Map<String, Object> row = Map.of("data", "{\"a\":1}");

        Map<String, Object> result = mapper.mapRow(row);

        assertThat(result).containsEntry("data", "{\"a\":1}");
    }

    @Test
    void getPayloadColumn_returnsConfiguredName() {
        PayloadColumnRowMapper mapper = new PayloadColumnRowMapper("my_col");
        assertThat(mapper.getPayloadColumn()).isEqualTo("my_col");
    }
}
