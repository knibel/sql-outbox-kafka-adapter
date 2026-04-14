package de.knibel.outbox.jdbc.rowmapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.knibel.outbox.config.FieldDataType;
import de.knibel.outbox.config.GroupConfig;
import de.knibel.outbox.config.MappingRule;
import de.knibel.outbox.config.OutboxTableProperties;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the unified {@link MappingRulePayloadMapper}.
 */
@SuppressWarnings("removal") // Testing deprecated MappingRulePayloadMapper
class MappingRulePayloadMapperTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final MappingRulePayloadMapper mapper = new MappingRulePayloadMapper(objectMapper);

    // ── Explicit column mapping (replaces fieldMappings) ─────────────────────

    @Test
    @SuppressWarnings("unchecked")
    void explicitMapping_mapsColumnToTarget() throws Exception {
        ResultSet rs = mockResultSet(
                List.of("order_id", "customer_name"),
                List.of("ORD-001", "John Doe"));

        OutboxTableProperties config = configWith(
                rule("order_id", "orderId"),
                rule("customer_name", "customer.name"));

        String json = mapper.mapPayload(rs, config);
        Map<String, Object> payload = objectMapper.readValue(json, Map.class);

        assertThat(payload.get("orderId")).isEqualTo("ORD-001");
        Map<String, Object> customer = (Map<String, Object>) payload.get("customer");
        assertThat(customer.get("name")).isEqualTo("John Doe");
    }

    @Test
    @SuppressWarnings("unchecked")
    void explicitMapping_withDataType() throws Exception {
        ResultSet rs = mockResultSet(
                List.of("amount", "is_active"),
                List.of(new BigDecimal("99.95"), 1));

        MappingRule amountRule = rule("amount", "totalAmount");
        amountRule.setDataType(FieldDataType.DOUBLE);

        MappingRule activeRule = rule("is_active", "active");
        activeRule.setDataType(FieldDataType.BOOLEAN);

        OutboxTableProperties config = configWith(amountRule, activeRule);

        String json = mapper.mapPayload(rs, config);
        Map<String, Object> payload = objectMapper.readValue(json, Map.class);

        assertThat(payload.get("totalAmount")).isEqualTo(99.95);
    }

    @Test
    @SuppressWarnings("unchecked")
    void explicitMapping_withValueMappings() throws Exception {
        ResultSet rs = mockResultSet(
                List.of("action"),
                List.of("I"));

        MappingRule actionRule = rule("action", "action");
        actionRule.setValueMappings(Map.of("I", "INSERT", "U", "UPDATE", "D", "DELETE"));

        OutboxTableProperties config = configWith(actionRule);

        String json = mapper.mapPayload(rs, config);
        Map<String, Object> payload = objectMapper.readValue(json, Map.class);

        assertThat(payload.get("action")).isEqualTo("INSERT");
    }

    @Test
    @SuppressWarnings("unchecked")
    void explicitMapping_withDateTimeFormat() throws Exception {
        ResultSet rs = mockResultSet(
                List.of("created_at", "order_date"),
                List.of(LocalDateTime.of(2024, 3, 15, 10, 30, 0),
                         LocalDate.of(2024, 3, 15)));

        MappingRule datetimeRule = rule("created_at", "createdAt");
        datetimeRule.setDataType(FieldDataType.DATETIME);
        datetimeRule.setFormat("yyyy-MM-dd'T'HH:mm:ss");

        MappingRule dateRule = rule("order_date", "orderDate");
        dateRule.setDataType(FieldDataType.DATE);
        dateRule.setFormat("yyyy-MM-dd");

        OutboxTableProperties config = configWith(datetimeRule, dateRule);

        String json = mapper.mapPayload(rs, config);
        Map<String, Object> payload = objectMapper.readValue(json, Map.class);

        assertThat(payload.get("createdAt")).isEqualTo("2024-03-15T10:30:00");
        assertThat(payload.get("orderDate")).isEqualTo("2024-03-15");
    }

    // ── Static value injection (replaces staticFields) ───────────────────────

    @Test
    @SuppressWarnings("unchecked")
    void staticRule_injectsConstantValue() throws Exception {
        ResultSet rs = mockResultSet(
                List.of("order_id"),
                List.of("ORD-001"));

        MappingRule staticRule = new MappingRule();
        staticRule.setTarget("eventType");
        staticRule.setValue("OrderCreated");

        MappingRule staticNested = new MappingRule();
        staticNested.setTarget("meta.source");
        staticNested.setValue("outbox-adapter");

        OutboxTableProperties config = configWith(
                rule("order_id", "orderId"),
                staticRule, staticNested);

        String json = mapper.mapPayload(rs, config);
        Map<String, Object> payload = objectMapper.readValue(json, Map.class);

        assertThat(payload.get("eventType")).isEqualTo("OrderCreated");
        Map<String, Object> meta = (Map<String, Object>) payload.get("meta");
        assertThat(meta.get("source")).isEqualTo("outbox-adapter");
    }

    // ── Raw payload (replaces PAYLOAD_COLUMN) ────────────────────────────────

    @Test
    void rawTarget_returnsColumnVerbatim() throws Exception {
        String expectedJson = "{\"key\":\"value\"}";
        ResultSet rs = mockResultSetWithString("payload", expectedJson);

        MappingRule rawRule = rule("payload", "_raw");
        OutboxTableProperties config = configWith(rawRule);

        String result = mapper.mapPayload(rs, config);
        assertThat(result).isEqualTo(expectedJson);
    }

    // ── CamelCase wildcard (replaces TO_CAMEL_CASE) ──────────────────────────

    @Test
    @SuppressWarnings("unchecked")
    void wildcardCamelCase_convertsAllColumns() throws Exception {
        ResultSet rs = mockResultSet(
                List.of("order_id", "customer_name", "total_amount"),
                List.of("ORD-001", "John Doe", 99.95));

        MappingRule wildcard = rule("*", "_camelCase");
        OutboxTableProperties config = configWith(wildcard);

        String json = mapper.mapPayload(rs, config);
        Map<String, Object> payload = objectMapper.readValue(json, Map.class);

        assertThat(payload).containsEntry("orderId", "ORD-001")
                           .containsEntry("customerName", "John Doe")
                           .containsEntry("totalAmount", 99.95);
    }

    @Test
    @SuppressWarnings("unchecked")
    void wildcardCamelCase_skipsAlreadyHandledColumns() throws Exception {
        ResultSet rs = mockResultSet(
                List.of("order_id", "customer_name", "total_amount"),
                List.of("ORD-001", "John Doe", 99.95));

        MappingRule explicit = rule("order_id", "myOrderId");
        MappingRule wildcard = rule("*", "_camelCase");
        OutboxTableProperties config = configWith(explicit, wildcard);

        String json = mapper.mapPayload(rs, config);
        Map<String, Object> payload = objectMapper.readValue(json, Map.class);

        // order_id is mapped explicitly; wildcard picks up the rest
        assertThat(payload).containsEntry("myOrderId", "ORD-001")
                           .containsEntry("customerName", "John Doe")
                           .containsEntry("totalAmount", 99.95)
                           .doesNotContainKey("orderId"); // not duplicated
    }

    // ── Regex pattern mapping (replaces columnPatterns) ──────────────────────

    @Test
    @SuppressWarnings("unchecked")
    void regexMapping_mapsColumnsByPattern() throws Exception {
        ResultSet rs = mockResultSet(
                List.of("neu_preis", "neu_menge"),
                List.of(10.0, 5));

        MappingRule regex = rule("/neu_(.*)/", "neu.$1");
        OutboxTableProperties config = configWith(regex);

        String json = mapper.mapPayload(rs, config);
        Map<String, Object> payload = objectMapper.readValue(json, Map.class);

        @SuppressWarnings("unchecked")
        Map<String, Object> neu = (Map<String, Object>) payload.get("neu");
        assertThat(neu).containsEntry("preis", 10.0)
                       .containsEntry("menge", 5);
    }

    @Test
    @SuppressWarnings("unchecked")
    void regexMapping_explicitTakesPrecedence() throws Exception {
        ResultSetMetaData meta = mock(ResultSetMetaData.class);
        when(meta.getColumnCount()).thenReturn(1);
        when(meta.getColumnLabel(1)).thenReturn("neu_preis");

        ResultSet rs = mock(ResultSet.class);
        when(rs.getMetaData()).thenReturn(meta);
        when(rs.getObject("neu_preis")).thenReturn("explicit_val");
        when(rs.getObject(1)).thenReturn("pattern_val");

        MappingRule explicit = rule("neu_preis", "overridden.preis");
        MappingRule regex = rule("/neu_(.*)/", "neu.$1");
        OutboxTableProperties config = configWith(explicit, regex);

        String json = mapper.mapPayload(rs, config);
        Map<String, Object> payload = objectMapper.readValue(json, Map.class);

        Map<String, Object> overridden = (Map<String, Object>) payload.get("overridden");
        assertThat(overridden).containsEntry("preis", "explicit_val");
        assertThat(payload).doesNotContainKey("neu");
    }

    // ── Array grouping (replaces listMappings) ───────────────────────────────

    @Test
    @SuppressWarnings("unchecked")
    void groupRule_groupsColumnsIntoArray() throws Exception {
        ResultSet rs = mockResultSet(
                List.of("new_price", "old_price", "new_stock", "old_stock"),
                List.of(10.0, 8.0, 100, 50));

        MappingRule newRule = groupRule("/new_(.*)/", "modifications", "$1", "attribute", "after");
        MappingRule oldRule = groupRule("/old_(.*)/", "modifications", "$1", "attribute", "before");
        OutboxTableProperties config = configWith(newRule, oldRule);

        String json = mapper.mapPayload(rs, config);
        Map<String, Object> payload = objectMapper.readValue(json, Map.class);

        List<Map<String, Object>> modifications = (List<Map<String, Object>>) payload.get("modifications");
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
    void groupRule_withoutKeyProperty() throws Exception {
        ResultSet rs = mockResultSet(
                List.of("new_price", "old_price"),
                List.of(10.0, 8.0));

        MappingRule newRule = groupRule("/new_(.*)/", "items", "$1", null, "after");
        MappingRule oldRule = groupRule("/old_(.*)/", "items", "$1", null, "before");
        OutboxTableProperties config = configWith(newRule, oldRule);

        String json = mapper.mapPayload(rs, config);
        Map<String, Object> payload = objectMapper.readValue(json, Map.class);

        List<Map<String, Object>> items = (List<Map<String, Object>>) payload.get("items");
        assertThat(items).hasSize(1);
        assertThat(items.get(0)).doesNotContainKey("price");
        assertThat(items.get(0).get("after")).isEqualTo(10.0);
        assertThat(items.get(0).get("before")).isEqualTo(8.0);
    }

    @Test
    @SuppressWarnings("unchecked")
    void groupRule_atNestedPath() throws Exception {
        ResultSet rs = mockResultSet(
                List.of("new_x", "old_x"),
                List.of("new_val", "old_val"));

        MappingRule newRule = groupRule("/new_(.*)/", "data.changes", "$1", "field", "after");
        MappingRule oldRule = groupRule("/old_(.*)/", "data.changes", "$1", "field", "before");
        OutboxTableProperties config = configWith(newRule, oldRule);

        String json = mapper.mapPayload(rs, config);
        Map<String, Object> payload = objectMapper.readValue(json, Map.class);

        Map<String, Object> data = (Map<String, Object>) payload.get("data");
        assertThat(data).isNotNull();
        List<Map<String, Object>> changes = (List<Map<String, Object>>) data.get("changes");
        assertThat(changes).hasSize(1);
        assertThat(changes.get(0).get("field")).isEqualTo("x");
    }

    @Test
    @SuppressWarnings("unchecked")
    void groupRule_fieldMappingsTakePrecedence() throws Exception {
        ResultSetMetaData meta = mock(ResultSetMetaData.class);
        when(meta.getColumnCount()).thenReturn(3);
        when(meta.getColumnLabel(1)).thenReturn("new_price");
        when(meta.getColumnLabel(2)).thenReturn("old_price");
        when(meta.getColumnLabel(3)).thenReturn("new_name");

        ResultSet rs = mock(ResultSet.class);
        when(rs.getMetaData()).thenReturn(meta);
        when(rs.getObject("new_name")).thenReturn("explicit_value");
        when(rs.getObject(1)).thenReturn(10.0);
        when(rs.getObject(2)).thenReturn(8.0);
        when(rs.getObject(3)).thenReturn("pattern_value");

        MappingRule explicit = rule("new_name", "productName");
        MappingRule newGroup = groupRule("/new_(.*)/", "changes", "$1", "field", "after");
        MappingRule oldGroup = groupRule("/old_(.*)/", "changes", "$1", "field", "before");
        OutboxTableProperties config = configWith(explicit, newGroup, oldGroup);

        String json = mapper.mapPayload(rs, config);
        Map<String, Object> payload = objectMapper.readValue(json, Map.class);

        // Explicit mapping wins
        assertThat(payload.get("productName")).isEqualTo("explicit_value");

        // List should only contain price (name was handled by explicit mapping)
        List<Map<String, Object>> changes = (List<Map<String, Object>>) payload.get("changes");
        assertThat(changes).hasSize(1);
        assertThat(changes.get(0).get("field")).isEqualTo("price");
    }

    @Test
    void groupRule_nonMatchingColumnsProduceNoList() throws Exception {
        ResultSet rs = mockResultSet(
                List.of("id", "status"),
                List.of("123", "PENDING"));

        MappingRule newGroup = groupRule("/new_(.*)/", "changes", "$1", "field", "after");
        OutboxTableProperties config = configWith(newGroup);

        String json = mapper.mapPayload(rs, config);
        assertThat(json).isEqualTo("{}");
    }

    @Test
    @SuppressWarnings("unchecked")
    void groupRule_withDataTypeConversion() throws Exception {
        ResultSet rs = mockResultSet(
                List.of("new_amount", "old_amount"),
                List.of(new BigDecimal("99.95"), new BigDecimal("88.50")));

        MappingRule newRule = groupRule("/new_(.*)/", "changes", "$1", "field", "after");
        newRule.setDataType(FieldDataType.DOUBLE);
        MappingRule oldRule = groupRule("/old_(.*)/", "changes", "$1", "field", "before");
        oldRule.setDataType(FieldDataType.DOUBLE);

        OutboxTableProperties config = configWith(newRule, oldRule);

        String json = mapper.mapPayload(rs, config);
        Map<String, Object> payload = objectMapper.readValue(json, Map.class);

        List<Map<String, Object>> changes = (List<Map<String, Object>>) payload.get("changes");
        assertThat(changes).hasSize(1);
        assertThat(changes.get(0).get("after")).isEqualTo(99.95);
        assertThat(changes.get(0).get("before")).isEqualTo(88.5);
    }

    // ── Composable rules (mix and match) ─────────────────────────────────────

    @Test
    @SuppressWarnings("unchecked")
    void composableRules_mixExplicitWithWildcardAndStatic() throws Exception {
        ResultSet rs = mockResultSet(
                List.of("order_id", "customer_name", "total_amount"),
                List.of("ORD-001", "John Doe", 99.95));

        MappingRule explicit = rule("order_id", "myOrderId");
        MappingRule staticRule = new MappingRule();
        staticRule.setTarget("eventType");
        staticRule.setValue("OrderCreated");
        MappingRule wildcard = rule("*", "_camelCase");

        OutboxTableProperties config = configWith(explicit, staticRule, wildcard);

        String json = mapper.mapPayload(rs, config);
        Map<String, Object> payload = objectMapper.readValue(json, Map.class);

        assertThat(payload).containsEntry("myOrderId", "ORD-001")
                           .containsEntry("eventType", "OrderCreated")
                           .containsEntry("customerName", "John Doe")
                           .containsEntry("totalAmount", 99.95)
                           .doesNotContainKey("orderId");
    }

    @Test
    @SuppressWarnings("unchecked")
    void nullValue_handledCorrectly() throws Exception {
        ResultSetMetaData meta = mock(ResultSetMetaData.class);
        when(meta.getColumnCount()).thenReturn(1);
        when(meta.getColumnLabel(1)).thenReturn("order_id");

        ResultSet rs = mock(ResultSet.class);
        when(rs.getMetaData()).thenReturn(meta);
        when(rs.getObject("order_id")).thenReturn(null);
        when(rs.getObject(1)).thenReturn(null);

        OutboxTableProperties config = configWith(rule("order_id", "orderId"));

        String json = mapper.mapPayload(rs, config);
        Map<String, Object> payload = objectMapper.readValue(json, Map.class);

        assertThat(payload).containsKey("orderId");
        assertThat(payload.get("orderId")).isNull();
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

    private static OutboxTableProperties configWith(MappingRule... rules) {
        OutboxTableProperties config = new OutboxTableProperties();
        config.setTableName("test_table");
        List<MappingRule> list = new ArrayList<>();
        for (MappingRule r : rules) {
            list.add(r);
        }
        config.setMappings(list);
        return config;
    }

    private static ResultSet mockResultSet(List<String> columns, List<Object> values) throws Exception {
        ResultSetMetaData meta = mock(ResultSetMetaData.class);
        when(meta.getColumnCount()).thenReturn(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            when(meta.getColumnLabel(i + 1)).thenReturn(columns.get(i));
        }

        ResultSet rs = mock(ResultSet.class);
        when(rs.getMetaData()).thenReturn(meta);
        for (int i = 0; i < columns.size(); i++) {
            when(rs.getObject(columns.get(i))).thenReturn(values.get(i));
            when(rs.getObject(i + 1)).thenReturn(values.get(i));
        }
        return rs;
    }

    private static ResultSet mockResultSetWithString(String column, String value) throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(rs.getString(column)).thenReturn(value);
        return rs;
    }
}
