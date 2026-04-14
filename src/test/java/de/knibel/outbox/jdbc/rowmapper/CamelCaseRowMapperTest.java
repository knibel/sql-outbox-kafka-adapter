package de.knibel.outbox.jdbc.rowmapper;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link CamelCaseRowMapper}.
 * Tests use plain {@code Map<String, Object>} — no ResultSet mocking.
 */
class CamelCaseRowMapperTest {

    @Test
    void mapRow_convertsSnakeCaseKeys() {
        CamelCaseRowMapper mapper = new CamelCaseRowMapper(Map.of());

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("order_id", "ORD-001");
        row.put("customer_first_name", "John");
        row.put("total_amount", 99.95);

        Map<String, Object> result = mapper.mapRow(row);

        assertThat(result).containsEntry("orderId", "ORD-001")
                          .containsEntry("customerFirstName", "John")
                          .containsEntry("totalAmount", 99.95);
    }

    @Test
    void mapRow_handlesAlreadyCamelCase() {
        CamelCaseRowMapper mapper = new CamelCaseRowMapper(Map.of());

        Map<String, Object> row = Map.of("camelCase", "value");

        Map<String, Object> result = mapper.mapRow(row);

        assertThat(result).containsEntry("camelcase", "value");
    }

    @Test
    void mapRow_handlesUpperCase() {
        CamelCaseRowMapper mapper = new CamelCaseRowMapper(Map.of());

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("ORDER_ID", "123");
        row.put("TOTAL_AMOUNT", 99.95);

        Map<String, Object> result = mapper.mapRow(row);

        assertThat(result).containsEntry("orderId", "123")
                          .containsEntry("totalAmount", 99.95);
    }

    @Test
    void mapRow_appliesStaticFields() {
        CamelCaseRowMapper mapper = new CamelCaseRowMapper(Map.of(
                "eventType", "OrderCreated",
                "meta.source", "outbox-adapter"));

        Map<String, Object> row = Map.of("order_id", "ORD-001");

        Map<String, Object> result = mapper.mapRow(row);

        assertThat(result).containsEntry("orderId", "ORD-001")
                          .containsEntry("eventType", "OrderCreated");

        @SuppressWarnings("unchecked")
        Map<String, Object> meta = (Map<String, Object>) result.get("meta");
        assertThat(meta).containsEntry("source", "outbox-adapter");
    }

    @Test
    void mapRow_preservesNullValues() {
        CamelCaseRowMapper mapper = new CamelCaseRowMapper(Map.of());

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("order_id", "123");
        row.put("customer_name", null);

        Map<String, Object> result = mapper.mapRow(row);

        assertThat(result).containsEntry("orderId", "123")
                          .containsKey("customerName");
        assertThat(result.get("customerName")).isNull();
    }

    @Test
    void mapRow_emptyStaticFields() {
        CamelCaseRowMapper mapper = new CamelCaseRowMapper(Map.of());

        Map<String, Object> row = Map.of("id", "123");

        Map<String, Object> result = mapper.mapRow(row);

        assertThat(result).containsEntry("id", "123");
    }
}
