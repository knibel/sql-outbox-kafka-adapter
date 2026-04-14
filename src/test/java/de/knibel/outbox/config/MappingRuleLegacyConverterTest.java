package de.knibel.outbox.config;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for legacy config → unified {@link MappingRule} conversion.
 */
class MappingRuleLegacyConverterTest {

    // ── PAYLOAD_COLUMN conversion ────────────────────────────────────────────

    @Test
    void payloadColumn_convertsToSingleRawRule() {
        OutboxTableProperties config = new OutboxTableProperties();
        config.setTableName("test");
        config.setRowMappingStrategy(RowMappingStrategy.PAYLOAD_COLUMN);
        config.setPayloadColumn("my_payload");
        config.setStaticTopic("topic");

        List<MappingRule> rules = MappingRuleLegacyConverter.convert(config);

        assertThat(rules).hasSize(1);
        assertThat(rules.get(0).getSource()).isEqualTo("my_payload");
        assertThat(rules.get(0).getTarget()).isEqualTo(MappingRule.TARGET_RAW);
    }

    // ── TO_CAMEL_CASE conversion ─────────────────────────────────────────────

    @Test
    void toCamelCase_convertsToWildcardCamelCase() {
        OutboxTableProperties config = new OutboxTableProperties();
        config.setTableName("test");
        config.setRowMappingStrategy(RowMappingStrategy.TO_CAMEL_CASE);
        config.setStaticTopic("topic");

        List<MappingRule> rules = MappingRuleLegacyConverter.convert(config);

        assertThat(rules).hasSize(1);
        assertThat(rules.get(0).getSource()).isEqualTo("*");
        assertThat(rules.get(0).getTarget()).isEqualTo(MappingRule.TARGET_CAMEL_CASE);
    }

    @Test
    void toCamelCase_includesStaticFields() {
        OutboxTableProperties config = new OutboxTableProperties();
        config.setTableName("test");
        config.setRowMappingStrategy(RowMappingStrategy.TO_CAMEL_CASE);
        config.setStaticTopic("topic");

        Map<String, String> statics = new LinkedHashMap<>();
        statics.put("eventType", "OrderCreated");
        statics.put("meta.source", "adapter");
        config.setStaticFields(statics);

        List<MappingRule> rules = MappingRuleLegacyConverter.convert(config);

        // 1 wildcard + 2 static
        assertThat(rules).hasSize(3);
        assertThat(rules.get(0).isWildcardSource()).isTrue();
        assertThat(rules.get(1).isStaticRule()).isTrue();
        assertThat(rules.get(1).getTarget()).isEqualTo("eventType");
        assertThat(rules.get(1).getValue()).isEqualTo("OrderCreated");
        assertThat(rules.get(2).isStaticRule()).isTrue();
        assertThat(rules.get(2).getTarget()).isEqualTo("meta.source");
        assertThat(rules.get(2).getValue()).isEqualTo("adapter");
    }

    // ── CUSTOM conversion ────────────────────────────────────────────────────

    @Test
    void custom_convertsFieldMappings() {
        OutboxTableProperties config = new OutboxTableProperties();
        config.setTableName("test");
        config.setRowMappingStrategy(RowMappingStrategy.CUSTOM);
        config.setStaticTopic("topic");

        Map<String, FieldMapping> fm = new LinkedHashMap<>();
        FieldMapping orderMapping = new FieldMapping("orderId", FieldDataType.STRING);
        fm.put("order_id", orderMapping);

        FieldMapping amountMapping = new FieldMapping("totalAmount", FieldDataType.DOUBLE);
        fm.put("total_amount", amountMapping);
        config.setFieldMappings(fm);

        List<MappingRule> rules = MappingRuleLegacyConverter.convert(config);

        assertThat(rules).hasSize(2);
        assertThat(rules.get(0).getSource()).isEqualTo("order_id");
        assertThat(rules.get(0).getTarget()).isEqualTo("orderId");
        assertThat(rules.get(0).getDataType()).isEqualTo(FieldDataType.STRING);
        assertThat(rules.get(1).getSource()).isEqualTo("total_amount");
        assertThat(rules.get(1).getTarget()).isEqualTo("totalAmount");
        assertThat(rules.get(1).getDataType()).isEqualTo(FieldDataType.DOUBLE);
    }

    @Test
    void custom_convertsColumnPatterns() {
        OutboxTableProperties config = new OutboxTableProperties();
        config.setTableName("test");
        config.setRowMappingStrategy(RowMappingStrategy.CUSTOM);
        config.setStaticTopic("topic");

        Map<String, FieldMapping> patterns = new LinkedHashMap<>();
        FieldMapping patternMapping = new FieldMapping();
        patternMapping.setName("neu.$1");
        patterns.put("neu_(.*)", patternMapping);
        config.setColumnPatterns(patterns);

        List<MappingRule> rules = MappingRuleLegacyConverter.convert(config);

        assertThat(rules).hasSize(1);
        assertThat(rules.get(0).getSource()).isEqualTo("/neu_(.*)/");
        assertThat(rules.get(0).isRegexSource()).isTrue();
        assertThat(rules.get(0).getTarget()).isEqualTo("neu.$1");
    }

    @Test
    void custom_convertsListMappings() {
        OutboxTableProperties config = new OutboxTableProperties();
        config.setTableName("test");
        config.setRowMappingStrategy(RowMappingStrategy.CUSTOM);
        config.setStaticTopic("topic");

        FieldMapping afterMapping = new FieldMapping();
        afterMapping.setName("after");
        FieldMapping beforeMapping = new FieldMapping();
        beforeMapping.setName("before");

        ListMapping listMapping = new ListMapping();
        listMapping.setKeyProperty("attribute");
        Map<String, FieldMapping> patterns = new LinkedHashMap<>();
        patterns.put("new_(.*)", afterMapping);
        patterns.put("old_(.*)", beforeMapping);
        listMapping.setPatterns(patterns);

        config.setListMappings(Map.of("modifications", listMapping));

        List<MappingRule> rules = MappingRuleLegacyConverter.convert(config);

        assertThat(rules).hasSize(2);

        MappingRule newRule = rules.get(0);
        assertThat(newRule.getSource()).isEqualTo("/new_(.*)/");
        assertThat(newRule.getTarget()).isEqualTo("modifications");
        assertThat(newRule.isGroupRule()).isTrue();
        assertThat(newRule.getGroup().getBy()).isEqualTo("$1");
        assertThat(newRule.getGroup().getKeyProperty()).isEqualTo("attribute");
        assertThat(newRule.getGroup().getProperty()).isEqualTo("after");

        MappingRule oldRule = rules.get(1);
        assertThat(oldRule.getSource()).isEqualTo("/old_(.*)/");
        assertThat(oldRule.getTarget()).isEqualTo("modifications");
        assertThat(oldRule.isGroupRule()).isTrue();
        assertThat(oldRule.getGroup().getProperty()).isEqualTo("before");
    }

    @Test
    void custom_convertsFieldMappingsWithValueMappings() {
        OutboxTableProperties config = new OutboxTableProperties();
        config.setTableName("test");
        config.setRowMappingStrategy(RowMappingStrategy.CUSTOM);
        config.setStaticTopic("topic");

        FieldMapping actionMapping = new FieldMapping();
        actionMapping.setName("action");
        actionMapping.setValueMappings(Map.of("I", "INSERT", "U", "UPDATE"));

        config.setFieldMappings(Map.of("action", actionMapping));

        List<MappingRule> rules = MappingRuleLegacyConverter.convert(config);

        assertThat(rules).hasSize(1);
        assertThat(rules.get(0).getValueMappings()).containsEntry("I", "INSERT")
                                                    .containsEntry("U", "UPDATE");
    }

    @Test
    void custom_convertsAllSubMechanisms() {
        OutboxTableProperties config = new OutboxTableProperties();
        config.setTableName("test");
        config.setRowMappingStrategy(RowMappingStrategy.CUSTOM);
        config.setStaticTopic("topic");

        // fieldMappings
        Map<String, FieldMapping> fm = new LinkedHashMap<>();
        fm.put("order_id", new FieldMapping("orderId", null));
        config.setFieldMappings(fm);

        // columnPatterns
        Map<String, FieldMapping> cp = new LinkedHashMap<>();
        FieldMapping patterned = new FieldMapping();
        patterned.setName("neu.$1");
        cp.put("neu_(.*)", patterned);
        config.setColumnPatterns(cp);

        // listMappings
        FieldMapping afterMapping = new FieldMapping();
        afterMapping.setName("after");
        ListMapping listMapping = new ListMapping();
        listMapping.setKeyProperty("field");
        listMapping.setPatterns(Map.of("new_(.*)", afterMapping));
        config.setListMappings(Map.of("changes", listMapping));

        // staticFields
        config.setStaticFields(Map.of("eventType", "OrderCreated"));

        List<MappingRule> rules = MappingRuleLegacyConverter.convert(config);

        // 1 fieldMapping + 1 columnPattern + 1 listMapping + 1 static = 4
        assertThat(rules).hasSize(4);
        assertThat(rules.get(0).getSource()).isEqualTo("order_id"); // fieldMapping
        assertThat(rules.get(1).isRegexSource()).isTrue();          // columnPattern
        assertThat(rules.get(1).getGroup()).isNull();               // not a group rule
        assertThat(rules.get(2).isGroupRule()).isTrue();             // listMapping
        assertThat(rules.get(3).isStaticRule()).isTrue();            // static
    }

    // ── needsConversion ──────────────────────────────────────────────────────

    @Test
    void needsConversion_trueWhenNoMappingsSet() {
        OutboxTableProperties config = new OutboxTableProperties();
        config.setTableName("test");
        config.setStaticTopic("topic");

        assertThat(MappingRuleLegacyConverter.needsConversion(config)).isTrue();
    }

    @Test
    void needsConversion_falseWhenMappingsPresent() {
        OutboxTableProperties config = new OutboxTableProperties();
        config.setTableName("test");
        config.setStaticTopic("topic");

        MappingRule rule = new MappingRule();
        rule.setSource("payload");
        rule.setTarget("_raw");
        config.setMappings(List.of(rule));

        assertThat(MappingRuleLegacyConverter.needsConversion(config)).isFalse();
    }

    @Test
    void convertIfNeeded_setsRulesOnConfig() {
        OutboxTableProperties config = new OutboxTableProperties();
        config.setTableName("test");
        config.setRowMappingStrategy(RowMappingStrategy.TO_CAMEL_CASE);
        config.setStaticTopic("topic");

        MappingRuleLegacyConverter.convertIfNeeded(config);

        assertThat(config.getMappings()).isNotEmpty();
        assertThat(config.getMappings().get(0).isWildcardSource()).isTrue();
        assertThat(config.getMappings().get(0).isCamelCaseTarget()).isTrue();
    }
}
