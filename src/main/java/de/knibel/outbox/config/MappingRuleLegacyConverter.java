package de.knibel.outbox.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts legacy row-mapping configuration properties ({@code rowMappingStrategy},
 * {@code fieldMappings}, {@code columnPatterns}, {@code listMappings},
 * {@code staticFields}) into the unified {@link MappingRule} list.
 *
 * <p>This converter is invoked at startup when the new {@code mappings} property
 * is empty but legacy properties are present.  A deprecation warning is logged
 * advising the user to migrate to the new syntax.
 *
 * @see MappingRule
 * @see OutboxTableProperties
 */
public final class MappingRuleLegacyConverter {

    private static final Logger log = LoggerFactory.getLogger(MappingRuleLegacyConverter.class);

    private MappingRuleLegacyConverter() {}

    /**
     * Returns {@code true} if the given table config uses legacy row-mapping
     * properties and has no unified {@code mappings}.
     */
    public static boolean needsConversion(OutboxTableProperties config) {
        return (config.getMappings() == null || config.getMappings().isEmpty())
                && usesLegacyConfig(config);
    }

    /**
     * Converts legacy configuration to a {@code List<MappingRule>} and sets
     * it on the config.  Logs a deprecation warning.
     */
    public static void convertIfNeeded(OutboxTableProperties config) {
        if (!needsConversion(config)) {
            return;
        }

        log.warn("Table '{}': Using deprecated row-mapping properties (rowMappingStrategy={}, "
                + "fieldMappings, columnPatterns, listMappings, staticFields). "
                + "Please migrate to the unified 'mappings' syntax.",
                config.getTableName(), config.getRowMappingStrategy());

        List<MappingRule> rules = convert(config);
        config.setMappings(rules);
    }

    /**
     * Performs the actual conversion based on the configured
     * {@link RowMappingStrategy}.
     */
    static List<MappingRule> convert(OutboxTableProperties config) {
        return switch (config.getRowMappingStrategy()) {
            case PAYLOAD_COLUMN -> convertPayloadColumn(config);
            case TO_CAMEL_CASE  -> convertToCamelCase(config);
            case CUSTOM         -> convertCustom(config);
        };
    }

    // ── PAYLOAD_COLUMN ───────────────────────────────────────────────────────

    private static List<MappingRule> convertPayloadColumn(OutboxTableProperties config) {
        List<MappingRule> rules = new ArrayList<>();
        MappingRule raw = new MappingRule();
        raw.setSource(config.getPayloadColumn());
        raw.setTarget(MappingRule.TARGET_RAW);
        rules.add(raw);
        return rules;
    }

    // ── TO_CAMEL_CASE ────────────────────────────────────────────────────────

    private static List<MappingRule> convertToCamelCase(OutboxTableProperties config) {
        List<MappingRule> rules = new ArrayList<>();
        MappingRule wildcard = new MappingRule();
        wildcard.setSource(MappingRule.SOURCE_WILDCARD);
        wildcard.setTarget(MappingRule.TARGET_CAMEL_CASE);
        rules.add(wildcard);
        addStaticFieldRules(rules, config.getStaticFields());
        return rules;
    }

    // ── CUSTOM ───────────────────────────────────────────────────────────────

    private static List<MappingRule> convertCustom(OutboxTableProperties config) {
        List<MappingRule> rules = new ArrayList<>();

        // 1. Explicit fieldMappings
        if (config.getFieldMappings() != null) {
            for (Map.Entry<String, FieldMapping> entry : config.getFieldMappings().entrySet()) {
                MappingRule rule = new MappingRule();
                rule.setSource(entry.getKey());
                FieldMapping fm = entry.getValue();
                rule.setTarget(fm.getName());
                rule.setDataType(fm.getDataType());
                rule.setFormat(fm.getFormat());
                rule.setValueMappings(fm.getValueMappings());
                rules.add(rule);
            }
        }

        // 2. columnPatterns (regex)
        if (config.getColumnPatterns() != null) {
            for (Map.Entry<String, FieldMapping> entry : config.getColumnPatterns().entrySet()) {
                MappingRule rule = new MappingRule();
                rule.setSource("/" + entry.getKey() + "/");
                FieldMapping fm = entry.getValue();
                rule.setTarget(fm.getName());
                rule.setDataType(fm.getDataType());
                rule.setFormat(fm.getFormat());
                rule.setValueMappings(fm.getValueMappings());
                rules.add(rule);
            }
        }

        // 3. listMappings (group rules)
        if (config.getListMappings() != null) {
            for (Map.Entry<String, ListMapping> listEntry : config.getListMappings().entrySet()) {
                String targetPath = listEntry.getKey();
                ListMapping listMapping = listEntry.getValue();

                for (Map.Entry<String, FieldMapping> patEntry : listMapping.getPatterns().entrySet()) {
                    MappingRule rule = new MappingRule();
                    rule.setSource("/" + patEntry.getKey() + "/");
                    rule.setTarget(targetPath);

                    FieldMapping fm = patEntry.getValue();
                    rule.setDataType(fm.getDataType());
                    rule.setFormat(fm.getFormat());
                    rule.setValueMappings(fm.getValueMappings());

                    GroupConfig group = new GroupConfig();
                    group.setBy("$1");
                    group.setKeyProperty(listMapping.getKeyProperty());
                    group.setProperty(fm.getName());
                    rule.setGroup(group);
                    rules.add(rule);
                }
            }
        }

        // 4. staticFields
        addStaticFieldRules(rules, config.getStaticFields());

        return rules;
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static void addStaticFieldRules(List<MappingRule> rules, Map<String, String> staticFields) {
        if (staticFields == null || staticFields.isEmpty()) {
            return;
        }
        for (Map.Entry<String, String> entry : staticFields.entrySet()) {
            MappingRule rule = new MappingRule();
            rule.setTarget(entry.getKey());
            rule.setValue(entry.getValue());
            rules.add(rule);
        }
    }

    private static boolean usesLegacyConfig(OutboxTableProperties config) {
        // Any table with explicit strategy set, or with legacy mapping properties
        RowMappingStrategy strategy = config.getRowMappingStrategy();
        if (strategy == RowMappingStrategy.TO_CAMEL_CASE || strategy == RowMappingStrategy.CUSTOM) {
            return true;
        }
        // PAYLOAD_COLUMN is the default – it's always a legacy config when no mappings are set
        return true;
    }
}
