package de.knibel.outbox.jdbc.rowmapper;

import de.knibel.outbox.config.FieldDataType;
import de.knibel.outbox.config.FieldMapping;
import de.knibel.outbox.config.ListMapping;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link CustomFieldRowMapper}.
 * Tests use plain {@code Map<String, Object>} — no ResultSet mocking.
 */
class CustomFieldRowMapperTest {

    // ── Explicit field mappings ──────────────────────────────────────────────

    @Test
    @SuppressWarnings("unchecked")
    void explicitFieldMappings_mapsColumnsToTargetPaths() {
        Map<String, FieldMapping> fieldMappings = new LinkedHashMap<>();
        fieldMappings.put("order_id", new FieldMapping("orderId", FieldDataType.STRING));
        fieldMappings.put("total_amount", new FieldMapping("totalAmount", FieldDataType.DOUBLE));
        fieldMappings.put("customer_name", new FieldMapping("customer.name", null));

        CustomFieldRowMapper mapper = new CustomFieldRowMapper(
                fieldMappings, Map.of(), Map.of(), Map.of());

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("order_id", "ORD-001");
        row.put("total_amount", new BigDecimal("99.95"));
        row.put("customer_name", "John Doe");

        Map<String, Object> result = mapper.mapRow(row);

        assertThat(result.get("orderId")).isEqualTo("ORD-001");
        assertThat(result.get("totalAmount")).isEqualTo(99.95);
        Map<String, Object> customer = (Map<String, Object>) result.get("customer");
        assertThat(customer.get("name")).isEqualTo("John Doe");
    }

    @Test
    void explicitFieldMappings_withValueMappings() {
        FieldMapping fm = new FieldMapping("status", null);
        fm.setValueMappings(Map.of("1", "ACTIVE", "2", "INACTIVE"));

        CustomFieldRowMapper mapper = new CustomFieldRowMapper(
                Map.of("status_code", fm), Map.of(), Map.of(), Map.of());

        Map<String, Object> row = Map.of("status_code", "1");

        Map<String, Object> result = mapper.mapRow(row);

        assertThat(result.get("status")).isEqualTo("ACTIVE");
    }

    // ── Pattern-based column mappings ────────────────────────────────────────

    @Test
    @SuppressWarnings("unchecked")
    void columnPatterns_mapsMatchingColumns() {
        Map<Pattern, FieldMapping> patterns = new LinkedHashMap<>();
        patterns.put(Pattern.compile("neu_(.*)"), new FieldMapping("neu.$1", null));
        patterns.put(Pattern.compile("alt_(.*)"), new FieldMapping("alt.$1", null));

        CustomFieldRowMapper mapper = new CustomFieldRowMapper(
                Map.of(), patterns, Map.of(), Map.of());

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("neu_preis", 10.0);
        row.put("alt_preis", 8.0);
        row.put("neu_menge", 5);

        Map<String, Object> result = mapper.mapRow(row);

        Map<String, Object> neu = (Map<String, Object>) result.get("neu");
        assertThat(neu).containsEntry("preis", 10.0)
                       .containsEntry("menge", 5);
        Map<String, Object> alt = (Map<String, Object>) result.get("alt");
        assertThat(alt).containsEntry("preis", 8.0);
    }

    @Test
    void fieldMappingsTakePrecedenceOverPatterns() {
        Map<String, FieldMapping> fieldMappings = new LinkedHashMap<>();
        fieldMappings.put("neu_preis", new FieldMapping("explicitPrice", null));

        Map<Pattern, FieldMapping> patterns = new LinkedHashMap<>();
        patterns.put(Pattern.compile("neu_(.*)"), new FieldMapping("neu.$1", null));

        CustomFieldRowMapper mapper = new CustomFieldRowMapper(
                fieldMappings, patterns, Map.of(), Map.of());

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("neu_preis", 10.0);
        row.put("neu_menge", 5);

        Map<String, Object> result = mapper.mapRow(row);

        assertThat(result.get("explicitPrice")).isEqualTo(10.0);
        assertThat(result).doesNotContainKey("preis");
    }

    // ── List mappings ────────────────────────────────────────────────────────

    @Test
    @SuppressWarnings("unchecked")
    void listMappings_groupsColumnsIntoArray() {
        ListMapping listMapping = new ListMapping();
        listMapping.setKeyProperty("attribute");
        Map<String, FieldMapping> patterns = new LinkedHashMap<>();
        patterns.put("new_(.*)", new FieldMapping("after", null));
        patterns.put("old_(.*)", new FieldMapping("before", null));
        listMapping.setPatterns(patterns);

        CustomFieldRowMapper mapper = new CustomFieldRowMapper(
                Map.of(), Map.of(), Map.of("modifications", listMapping), Map.of());

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("new_price", 10.0);
        row.put("old_price", 8.0);
        row.put("new_stock", 100);
        row.put("old_stock", 50);

        Map<String, Object> result = mapper.mapRow(row);

        List<Map<String, Object>> modifications =
                (List<Map<String, Object>>) result.get("modifications");
        assertThat(modifications).hasSize(2);

        Map<String, Object> priceEntry = modifications.get(0);
        assertThat(priceEntry.get("attribute")).isEqualTo("price");
        assertThat(priceEntry.get("after")).isEqualTo(10.0);
        assertThat(priceEntry.get("before")).isEqualTo(8.0);
    }

    // ── Static fields ────────────────────────────────────────────────────────

    @Test
    void staticFields_injectedLast() {
        Map<String, FieldMapping> fieldMappings = new LinkedHashMap<>();
        fieldMappings.put("id", new FieldMapping("id", null));

        CustomFieldRowMapper mapper = new CustomFieldRowMapper(
                fieldMappings, Map.of(), Map.of(),
                Map.of("source", "outbox-adapter"));

        Map<String, Object> row = Map.of("id", "123");

        Map<String, Object> result = mapper.mapRow(row);

        assertThat(result).containsEntry("id", "123")
                          .containsEntry("source", "outbox-adapter");
    }
}
