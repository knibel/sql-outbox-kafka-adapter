package de.knibel.outbox.datamapper;

import de.knibel.outbox.config.DataMapperConfig;
import de.knibel.outbox.config.FieldDataType;
import de.knibel.outbox.config.GroupConfig;
import de.knibel.outbox.config.MappingRule;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link MappingRuleDataMapper}.
 *
 * <p>These tests verify the technology-independent mapping logic using
 * plain {@link Map} input, without any JDBC dependencies.
 */
class MappingRuleDataMapperTest {

    private final OutboxDataMapper mapper = new MappingRuleDataMapper();

    // ── Explicit column mapping ─────────────────────────────────────────

    @Test
    @SuppressWarnings("unchecked")
    void explicitMapping_mapsColumnToTarget() {
        Map<String, Object> row = rowOf("order_id", "ORD-001", "customer_name", "John Doe");

        DataMapperConfig config = configWith(
                rule("order_id", "orderId"),
                rule("customer_name", "customer.name"));

        Map<String, Object> result = mapper.map(row, config);

        assertThat(result.get("orderId")).isEqualTo("ORD-001");
        Map<String, Object> customer = (Map<String, Object>) result.get("customer");
        assertThat(customer.get("name")).isEqualTo("John Doe");
    }

    @Test
    void explicitMapping_withDataType() {
        Map<String, Object> row = rowOf(
                "amount", new BigDecimal("99.95"),
                "is_active", 1);

        MappingRule amountRule = rule("amount", "totalAmount");
        amountRule.setDataType(FieldDataType.DOUBLE);

        MappingRule activeRule = rule("is_active", "active");
        activeRule.setDataType(FieldDataType.BOOLEAN);

        DataMapperConfig config = configWith(amountRule, activeRule);

        Map<String, Object> result = mapper.map(row, config);

        assertThat(result.get("totalAmount")).isEqualTo(99.95);
    }

    @Test
    void explicitMapping_withValueMappings() {
        Map<String, Object> row = rowOf("action", "I");

        MappingRule actionRule = rule("action", "action");
        actionRule.setValueMappings(Map.of("I", "INSERT", "U", "UPDATE", "D", "DELETE"));

        DataMapperConfig config = configWith(actionRule);

        Map<String, Object> result = mapper.map(row, config);

        assertThat(result.get("action")).isEqualTo("INSERT");
    }

    @Test
    void explicitMapping_withDateTimeFormatting() {
        Map<String, Object> row = rowOf(
                "created_at", LocalDateTime.of(2025, 1, 15, 10, 30, 0),
                "order_date", LocalDate.of(2025, 1, 15));

        MappingRule datetimeRule = rule("created_at", "createdAt");
        datetimeRule.setDataType(FieldDataType.DATETIME);
        datetimeRule.setFormat("yyyy-MM-dd'T'HH:mm:ss");

        MappingRule dateRule = rule("order_date", "orderDate");
        dateRule.setDataType(FieldDataType.DATE);
        dateRule.setFormat("yyyy-MM-dd");

        DataMapperConfig config = configWith(datetimeRule, dateRule);

        Map<String, Object> result = mapper.map(row, config);

        assertThat(result.get("createdAt")).isEqualTo("2025-01-15T10:30:00");
        assertThat(result.get("orderDate")).isEqualTo("2025-01-15");
    }

    // ── Static value injection ──────────────────────────────────────────

    @Test
    void staticRule_injectsConstant() {
        Map<String, Object> row = rowOf("order_id", "ORD-001");

        MappingRule staticRule = new MappingRule();
        staticRule.setTarget("eventType");
        staticRule.setValue("OrderCreated");

        DataMapperConfig config = configWith(rule("order_id", "orderId"), staticRule);

        Map<String, Object> result = mapper.map(row, config);

        assertThat(result.get("orderId")).isEqualTo("ORD-001");
        assertThat(result.get("eventType")).isEqualTo("OrderCreated");
    }

    // ── CamelCase wildcard ──────────────────────────────────────────────

    @Test
    void camelCaseWildcard_convertsAllColumns() {
        Map<String, Object> row = rowOf(
                "order_id", "ORD-001",
                "customer_first_name", "John",
                "total_amount", 99.95);

        DataMapperConfig config = configWith(rule("*", "_camelCase"));

        Map<String, Object> result = mapper.map(row, config);

        assertThat(result).containsEntry("orderId", "ORD-001")
                           .containsEntry("customerFirstName", "John")
                           .containsEntry("totalAmount", 99.95);
    }

    @Test
    void camelCaseWildcard_skipsExplicitlyHandledColumns() {
        Map<String, Object> row = rowOf(
                "order_id", "ORD-001",
                "customer_name", "John Doe",
                "total_amount", 99.95);

        MappingRule explicit = rule("order_id", "myOrderId");
        MappingRule wildcard = rule("*", "_camelCase");

        DataMapperConfig config = configWith(explicit, wildcard);

        Map<String, Object> result = mapper.map(row, config);

        assertThat(result).containsEntry("myOrderId", "ORD-001")
                           .containsEntry("customerName", "John Doe")
                           .containsEntry("totalAmount", 99.95)
                           .doesNotContainKey("orderId");
    }

    // ── Regex mapping ───────────────────────────────────────────────────

    @Test
    @SuppressWarnings("unchecked")
    void regexMapping_matchesAndTransforms() {
        Map<String, Object> row = rowOf("neu_preis", 10.0, "neu_menge", 5);

        MappingRule regex = rule("/neu_(.*)/", "neu.$1");
        DataMapperConfig config = configWith(regex);

        Map<String, Object> result = mapper.map(row, config);

        Map<String, Object> neu = (Map<String, Object>) result.get("neu");
        assertThat(neu).containsEntry("preis", 10.0)
                        .containsEntry("menge", 5);
    }

    @Test
    void regexMapping_explicitTakesPrecedence() {
        Map<String, Object> row = rowOf("neu_preis", "the_value");

        MappingRule explicit = rule("neu_preis", "overridden.preis");
        MappingRule regex = rule("/neu_(.*)/", "neu.$1");
        DataMapperConfig config = configWith(explicit, regex);

        Map<String, Object> result = mapper.map(row, config);

        assertThat(result).containsKey("overridden")
                           .doesNotContainKey("neu");
    }

    // ── Array grouping ──────────────────────────────────────────────────

    @Test
    @SuppressWarnings("unchecked")
    void groupRule_groupsColumnsIntoArray() {
        Map<String, Object> row = rowOf(
                "new_price", 10.0, "old_price", 8.0,
                "new_stock", 100, "old_stock", 50);

        MappingRule newRule = groupRule("/new_(.*)/", "modifications", "$1", "attribute", "after");
        MappingRule oldRule = groupRule("/old_(.*)/", "modifications", "$1", "attribute", "before");
        DataMapperConfig config = configWith(newRule, oldRule);

        Map<String, Object> result = mapper.map(row, config);

        List<Map<String, Object>> modifications =
                (List<Map<String, Object>>) result.get("modifications");
        assertThat(modifications).hasSize(2);

        Map<String, Object> price = modifications.stream()
                .filter(m -> "price".equals(m.get("attribute")))
                .findFirst().orElseThrow();
        assertThat(price).containsEntry("after", 10.0)
                          .containsEntry("before", 8.0);

        Map<String, Object> stock = modifications.stream()
                .filter(m -> "stock".equals(m.get("attribute")))
                .findFirst().orElseThrow();
        assertThat(stock).containsEntry("after", 100)
                          .containsEntry("before", 50);
    }

    @Test
    @SuppressWarnings("unchecked")
    void groupRule_fieldMappingsTakePrecedence() {
        Map<String, Object> row = rowOf(
                "new_price", 10.0, "old_price", 8.0, "new_name", "the_name");

        MappingRule explicit = rule("new_name", "productName");
        MappingRule newGroup = groupRule("/new_(.*)/", "changes", "$1", "field", "after");
        MappingRule oldGroup = groupRule("/old_(.*)/", "changes", "$1", "field", "before");
        DataMapperConfig config = configWith(explicit, newGroup, oldGroup);

        Map<String, Object> result = mapper.map(row, config);

        // Explicit mapping wins
        assertThat(result.get("productName")).isEqualTo("the_name");

        // List should only contain price (name was handled by explicit mapping)
        List<Map<String, Object>> changes = (List<Map<String, Object>>) result.get("changes");
        assertThat(changes).hasSize(1);
        assertThat(changes.get(0).get("field")).isEqualTo("price");
    }

    // ── Composable rules ────────────────────────────────────────────────

    @Test
    void composableRules_mixExplicitWithWildcardAndStatic() {
        Map<String, Object> row = rowOf(
                "order_id", "ORD-001",
                "customer_name", "John Doe",
                "total_amount", 99.95);

        MappingRule explicit = rule("order_id", "myOrderId");
        MappingRule staticRule = new MappingRule();
        staticRule.setTarget("eventType");
        staticRule.setValue("OrderCreated");
        MappingRule wildcard = rule("*", "_camelCase");

        DataMapperConfig config = configWith(explicit, staticRule, wildcard);

        Map<String, Object> result = mapper.map(row, config);

        assertThat(result).containsEntry("myOrderId", "ORD-001")
                           .containsEntry("eventType", "OrderCreated")
                           .containsEntry("customerName", "John Doe")
                           .containsEntry("totalAmount", 99.95)
                           .doesNotContainKey("orderId");
    }

    // ── Null handling ───────────────────────────────────────────────────

    @Test
    void nullValue_handledCorrectly() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("order_id", null);

        DataMapperConfig config = configWith(rule("order_id", "orderId"));

        Map<String, Object> result = mapper.map(row, config);

        assertThat(result).containsKey("orderId");
        assertThat(result.get("orderId")).isNull();
    }

    // ── Case-insensitive column lookup ──────────────────────────────────

    @Test
    void explicitMapping_caseInsensitiveLookup() {
        Map<String, Object> row = rowOf("ORDER_ID", "ORD-001");

        DataMapperConfig config = configWith(rule("order_id", "orderId"));

        Map<String, Object> result = mapper.map(row, config);

        assertThat(result.get("orderId")).isEqualTo("ORD-001");
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    private static MappingRule rule(String source, String target) {
        MappingRule rule = new MappingRule();
        rule.setSource(source);
        rule.setTarget(target);
        return rule;
    }

    private static MappingRule groupRule(String source, String target,
                                         String by, String keyProperty, String property) {
        MappingRule rule = new MappingRule();
        rule.setSource(source);
        rule.setTarget(target);
        GroupConfig group = new GroupConfig();
        group.setBy(by);
        group.setKeyProperty(keyProperty);
        group.setProperty(property);
        rule.setGroup(group);
        return rule;
    }

    private static DataMapperConfig configWith(MappingRule... rules) {
        List<MappingRule> list = new ArrayList<>();
        for (MappingRule r : rules) {
            list.add(r);
        }
        return () -> list;
    }

    /**
     * Creates a row map from alternating key-value pairs.
     * Uses {@link LinkedHashMap} to preserve insertion order (simulating column order).
     */
    private static Map<String, Object> rowOf(Object... keysAndValues) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            row.put((String) keysAndValues[i], keysAndValues[i + 1]);
        }
        return row;
    }
}
