package de.knibel.outbox.jdbc.rowmapper;

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
 * Unit tests for {@link MappingRuleRowMapper}.
 * Tests use plain {@code Map<String, Object>} — no ResultSet mocking.
 */
class MappingRuleRowMapperTest {

    // ── Explicit column mapping ──────────────────────────────────────────────

    @Test
    @SuppressWarnings("unchecked")
    void explicitMapping_mapsColumnToTarget() {
        MappingRuleRowMapper mapper = mapperWith(
                rule("order_id", "orderId"),
                rule("customer_name", "customer.name"));

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("order_id", "ORD-001");
        row.put("customer_name", "John Doe");

        Map<String, Object> result = mapper.mapRow(row);

        assertThat(result.get("orderId")).isEqualTo("ORD-001");
        Map<String, Object> customer = (Map<String, Object>) result.get("customer");
        assertThat(customer.get("name")).isEqualTo("John Doe");
    }

    @Test
    void explicitMapping_withDataType() {
        MappingRule amountRule = rule("amount", "totalAmount");
        amountRule.setDataType(FieldDataType.DOUBLE);

        MappingRuleRowMapper mapper = mapperWith(amountRule);

        Map<String, Object> row = Map.of("amount", new BigDecimal("99.95"));

        Map<String, Object> result = mapper.mapRow(row);

        assertThat(result.get("totalAmount")).isEqualTo(99.95);
    }

    @Test
    void explicitMapping_withValueMappings() {
        MappingRule actionRule = rule("action", "action");
        actionRule.setValueMappings(Map.of("I", "INSERT", "U", "UPDATE", "D", "DELETE"));

        MappingRuleRowMapper mapper = mapperWith(actionRule);

        Map<String, Object> row = Map.of("action", "I");

        Map<String, Object> result = mapper.mapRow(row);

        assertThat(result.get("action")).isEqualTo("INSERT");
    }

    @Test
    void explicitMapping_withDateTimeFormat() {
        MappingRule datetimeRule = rule("created_at", "createdAt");
        datetimeRule.setDataType(FieldDataType.DATETIME);
        datetimeRule.setFormat("yyyy-MM-dd'T'HH:mm:ss");

        MappingRule dateRule = rule("order_date", "orderDate");
        dateRule.setDataType(FieldDataType.DATE);
        dateRule.setFormat("yyyy-MM-dd");

        MappingRuleRowMapper mapper = mapperWith(datetimeRule, dateRule);

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("created_at", LocalDateTime.of(2024, 3, 15, 10, 30, 0));
        row.put("order_date", LocalDate.of(2024, 3, 15));

        Map<String, Object> result = mapper.mapRow(row);

        assertThat(result.get("createdAt")).isEqualTo("2024-03-15T10:30:00");
        assertThat(result.get("orderDate")).isEqualTo("2024-03-15");
    }

    // ── Static value injection ───────────────────────────────────────────────

    @Test
    @SuppressWarnings("unchecked")
    void staticRule_injectsConstantValue() {
        MappingRule staticRule = new MappingRule();
        staticRule.setTarget("eventType");
        staticRule.setValue("OrderCreated");

        MappingRule staticNested = new MappingRule();
        staticNested.setTarget("meta.source");
        staticNested.setValue("outbox-adapter");

        MappingRuleRowMapper mapper = mapperWith(
                rule("order_id", "orderId"),
                staticRule, staticNested);

        Map<String, Object> row = Map.of("order_id", "ORD-001");

        Map<String, Object> result = mapper.mapRow(row);

        assertThat(result.get("eventType")).isEqualTo("OrderCreated");
        Map<String, Object> meta = (Map<String, Object>) result.get("meta");
        assertThat(meta.get("source")).isEqualTo("outbox-adapter");
    }

    // ── Raw payload ──────────────────────────────────────────────────────────

    @Test
    void rawTarget_returnsColumnValue() {
        MappingRule rawRule = rule("payload", "_raw");
        MappingRuleRowMapper mapper = mapperWith(rawRule);

        String expectedJson = "{\"key\":\"value\"}";
        Map<String, Object> row = Map.of("payload", expectedJson);

        Map<String, Object> result = mapper.mapRow(row);

        assertThat(result).hasSize(1)
                          .containsEntry("payload", expectedJson);
    }

    // ── CamelCase wildcard ───────────────────────────────────────────────────

    @Test
    void wildcardCamelCase_convertsAllColumns() {
        MappingRule wildcard = rule("*", "_camelCase");
        MappingRuleRowMapper mapper = mapperWith(wildcard);

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("order_id", "ORD-001");
        row.put("customer_name", "John Doe");
        row.put("total_amount", 99.95);

        Map<String, Object> result = mapper.mapRow(row);

        assertThat(result).containsEntry("orderId", "ORD-001")
                          .containsEntry("customerName", "John Doe")
                          .containsEntry("totalAmount", 99.95);
    }

    @Test
    void wildcardCamelCase_skipsAlreadyHandledColumns() {
        MappingRule explicit = rule("order_id", "myOrderId");
        MappingRule wildcard = rule("*", "_camelCase");
        MappingRuleRowMapper mapper = mapperWith(explicit, wildcard);

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("order_id", "ORD-001");
        row.put("customer_name", "John Doe");
        row.put("total_amount", 99.95);

        Map<String, Object> result = mapper.mapRow(row);

        assertThat(result).containsEntry("myOrderId", "ORD-001")
                          .containsEntry("customerName", "John Doe")
                          .containsEntry("totalAmount", 99.95)
                          .doesNotContainKey("orderId");
    }

    // ── Regex pattern mapping ────────────────────────────────────────────────

    @Test
    @SuppressWarnings("unchecked")
    void regexMapping_mapsColumnsByPattern() {
        MappingRule regex = rule("/neu_(.*)/", "neu.$1");
        MappingRuleRowMapper mapper = mapperWith(regex);

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("neu_preis", 10.0);
        row.put("neu_menge", 5);

        Map<String, Object> result = mapper.mapRow(row);

        Map<String, Object> neu = (Map<String, Object>) result.get("neu");
        assertThat(neu).containsEntry("preis", 10.0)
                       .containsEntry("menge", 5);
    }

    @Test
    @SuppressWarnings("unchecked")
    void regexMapping_explicitTakesPrecedence() {
        MappingRule explicit = rule("neu_preis", "overridden.preis");
        MappingRule regex = rule("/neu_(.*)/", "neu.$1");
        MappingRuleRowMapper mapper = mapperWith(explicit, regex);

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("neu_preis", "explicit_val");

        Map<String, Object> result = mapper.mapRow(row);

        Map<String, Object> overridden = (Map<String, Object>) result.get("overridden");
        assertThat(overridden).containsEntry("preis", "explicit_val");
        assertThat(result).doesNotContainKey("neu");
    }

    // ── Array grouping ───────────────────────────────────────────────────────

    @Test
    @SuppressWarnings("unchecked")
    void groupRule_groupsColumnsIntoArray() {
        MappingRule newRule = groupRule("/new_(.*)/", "modifications", "$1", "attribute", "after");
        MappingRule oldRule = groupRule("/old_(.*)/", "modifications", "$1", "attribute", "before");
        MappingRuleRowMapper mapper = mapperWith(newRule, oldRule);

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

        Map<String, Object> stockEntry = modifications.get(1);
        assertThat(stockEntry.get("attribute")).isEqualTo("stock");
        assertThat(stockEntry.get("after")).isEqualTo(100);
        assertThat(stockEntry.get("before")).isEqualTo(50);
    }

    @Test
    @SuppressWarnings("unchecked")
    void groupRule_withoutKeyProperty() {
        MappingRule newRule = groupRule("/new_(.*)/", "items", "$1", null, "after");
        MappingRule oldRule = groupRule("/old_(.*)/", "items", "$1", null, "before");
        MappingRuleRowMapper mapper = mapperWith(newRule, oldRule);

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("new_price", 10.0);
        row.put("old_price", 8.0);

        Map<String, Object> result = mapper.mapRow(row);

        List<Map<String, Object>> items =
                (List<Map<String, Object>>) result.get("items");
        assertThat(items).hasSize(1);
        assertThat(items.get(0)).doesNotContainKey("price");
        assertThat(items.get(0).get("after")).isEqualTo(10.0);
        assertThat(items.get(0).get("before")).isEqualTo(8.0);
    }

    @Test
    void groupRule_nonMatchingColumnsProduceNoList() {
        MappingRule newGroup = groupRule("/new_(.*)/", "changes", "$1", "field", "after");
        MappingRuleRowMapper mapper = mapperWith(newGroup);

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", "123");
        row.put("status", "PENDING");

        Map<String, Object> result = mapper.mapRow(row);

        assertThat(result).isEmpty();
    }

    @Test
    @SuppressWarnings("unchecked")
    void groupRule_withDataTypeConversion() {
        MappingRule newRule = groupRule("/new_(.*)/", "changes", "$1", "field", "after");
        newRule.setDataType(FieldDataType.DOUBLE);
        MappingRule oldRule = groupRule("/old_(.*)/", "changes", "$1", "field", "before");
        oldRule.setDataType(FieldDataType.DOUBLE);
        MappingRuleRowMapper mapper = mapperWith(newRule, oldRule);

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("new_amount", new BigDecimal("99.95"));
        row.put("old_amount", new BigDecimal("88.50"));

        Map<String, Object> result = mapper.mapRow(row);

        List<Map<String, Object>> changes =
                (List<Map<String, Object>>) result.get("changes");
        assertThat(changes).hasSize(1);
        assertThat(changes.get(0).get("after")).isEqualTo(99.95);
        assertThat(changes.get(0).get("before")).isEqualTo(88.5);
    }

    // ── Composable rules (mix and match) ─────────────────────────────────────

    @Test
    void composableRules_mixExplicitWithWildcardAndStatic() {
        MappingRule explicit = rule("order_id", "myOrderId");
        MappingRule staticRule = new MappingRule();
        staticRule.setTarget("eventType");
        staticRule.setValue("OrderCreated");
        MappingRule wildcard = rule("*", "_camelCase");

        MappingRuleRowMapper mapper = mapperWith(explicit, staticRule, wildcard);

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("order_id", "ORD-001");
        row.put("customer_name", "John Doe");
        row.put("total_amount", 99.95);

        Map<String, Object> result = mapper.mapRow(row);

        assertThat(result).containsEntry("myOrderId", "ORD-001")
                          .containsEntry("eventType", "OrderCreated")
                          .containsEntry("customerName", "John Doe")
                          .containsEntry("totalAmount", 99.95)
                          .doesNotContainKey("orderId");
    }

    @Test
    void nullValue_handledCorrectly() {
        MappingRuleRowMapper mapper = mapperWith(rule("order_id", "orderId"));

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("order_id", null);

        Map<String, Object> result = mapper.mapRow(row);

        assertThat(result).containsKey("orderId");
        assertThat(result.get("orderId")).isNull();
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

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

    private static MappingRuleRowMapper mapperWith(MappingRule... rules) {
        List<MappingRule> list = new ArrayList<>();
        for (MappingRule r : rules) {
            list.add(r);
        }
        return new MappingRuleRowMapper(list);
    }
}
