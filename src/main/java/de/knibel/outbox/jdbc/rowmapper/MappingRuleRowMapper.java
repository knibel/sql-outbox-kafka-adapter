package de.knibel.outbox.jdbc.rowmapper;

import de.knibel.outbox.config.MappingRule;
import de.knibel.outbox.jdbc.RowMapperUtil;
import de.knibel.outbox.repository.OutboxRowMapper;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Unified {@link OutboxRowMapper} that processes the ordered
 * {@link MappingRule} list from configuration.
 *
 * <p>Rules are evaluated top-to-bottom.  Once a column is claimed by a
 * rule, later rules skip it.  This replaces the three separate mappers
 * with a single implementation.
 *
 * <p>Operates entirely on a plain {@code Map<String, Object>} — no JDBC
 * or Jackson dependency at mapping time.
 *
 * <p><b>Special case – raw target:</b> when a rule has target
 * {@code _raw}, the mapper returns a single-entry map whose key is
 * the source column name and whose value is the raw column value.
 * The adapter layer is responsible for extracting the string value.
 *
 * @see MappingRule
 * @see MappingRulePayloadMapper
 */
@SuppressWarnings("removal") // Intentionally implements deprecated PayloadMapper during transition
public class MappingRuleRowMapper implements PayloadMapper, OutboxRowMapper {

    private final List<MappingRule> rules;

    /** Pre-compiled regex patterns cached per rule index. */
    private volatile Map<Integer, Pattern> compiledPatterns;

    /**
     * @param rules ordered list of mapping rules (never {@code null})
     */
    public MappingRuleRowMapper(List<MappingRule> rules) {
        this.rules = rules;
    }

    @Override
    public Map<String, Object> mapRow(Map<String, Object> row) {
        Map<Integer, Pattern> patterns = getCompiledPatterns(rules);

        // Check for raw target first – it short-circuits everything
        for (MappingRule rule : rules) {
            if (rule.isRawTarget() && rule.getSource() != null) {
                Object value = row.get(rule.getSource());
                return Map.of(rule.getSource(), value != null ? value : "");
            }
        }

        Map<String, Object> root = new LinkedHashMap<>();
        Set<String> handledColumns = new HashSet<>();

        // Intermediate group state: targetPath → (capturedKey → {property → value})
        Map<String, Map<String, Map<String, Object>>> groupState = new LinkedHashMap<>();
        Map<String, String> groupKeyProperties = new LinkedHashMap<>();

        for (int ruleIndex = 0; ruleIndex < rules.size(); ruleIndex++) {
            MappingRule rule = rules.get(ruleIndex);

            if (rule.isStaticRule()) {
                RowMapperUtil.setNestedValue(root, rule.getTarget(), rule.getValue());
                continue;
            }

            if (rule.isCamelCaseTarget() && rule.isWildcardSource()) {
                for (Map.Entry<String, Object> entry : row.entrySet()) {
                    String columnName = entry.getKey();
                    if (handledColumns.contains(columnName.toLowerCase(Locale.ROOT))) {
                        continue;
                    }
                    String camelKey = RowMapperUtil.toCamelCase(columnName);
                    root.put(camelKey, entry.getValue());
                    handledColumns.add(columnName.toLowerCase(Locale.ROOT));
                }
                continue;
            }

            if (rule.isRegexSource()) {
                Pattern pattern = patterns.get(ruleIndex);

                if (rule.isGroupRule()) {
                    processGroupRule(row, rule, pattern,
                            handledColumns, groupState, groupKeyProperties);
                } else {
                    processRegexRule(row, rule, pattern, handledColumns, root);
                }
                continue;
            }

            if (rule.getSource() != null && !rule.isWildcardSource()) {
                String sourceColumn = rule.getSource();
                handledColumns.add(sourceColumn.toLowerCase(Locale.ROOT));
                Object value = row.get(sourceColumn);
                Object mapped = RowMapperUtil.applyValueMapping(value, rule.getValueMappings());
                Object converted = RowMapperUtil.convertValue(mapped, rule.getDataType(), rule.getFormat());
                RowMapperUtil.setNestedValue(root, rule.getTarget(), converted);
            }
        }

        // Finalize all group rules: convert grouped entries to arrays
        for (Map.Entry<String, Map<String, Map<String, Object>>> groupEntry : groupState.entrySet()) {
            String targetPath = groupEntry.getKey();
            Map<String, Map<String, Object>> groups = groupEntry.getValue();
            String keyProperty = groupKeyProperties.get(targetPath);

            List<Map<String, Object>> list = new ArrayList<>();
            for (Map.Entry<String, Map<String, Object>> entry : groups.entrySet()) {
                Map<String, Object> element = new LinkedHashMap<>();
                if (keyProperty != null && !keyProperty.isBlank()) {
                    element.put(keyProperty, entry.getKey());
                }
                element.putAll(entry.getValue());
                list.add(element);
            }
            if (!list.isEmpty()) {
                RowMapperUtil.setNestedValue(root, targetPath, list);
            }
        }

        return root;
    }

    // ── Regex pattern mapping ────────────────────────────────────────────────

    private void processRegexRule(Map<String, Object> row, MappingRule rule,
                                  Pattern pattern, Set<String> handledColumns,
                                  Map<String, Object> root) {
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            String columnName = entry.getKey();
            if (handledColumns.contains(columnName.toLowerCase(Locale.ROOT))) {
                continue;
            }
            Matcher matcher = pattern.matcher(columnName);
            if (matcher.matches()) {
                String targetName = matcher.replaceAll(rule.getTarget());
                Object value = entry.getValue();
                Object mapped = RowMapperUtil.applyValueMapping(value, rule.getValueMappings());
                Object converted = RowMapperUtil.convertValue(mapped, rule.getDataType(), rule.getFormat());
                RowMapperUtil.setNestedValue(root, targetName, converted);
                handledColumns.add(columnName.toLowerCase(Locale.ROOT));
            }
        }
    }

    // ── Array grouping ───────────────────────────────────────────────────────

    private void processGroupRule(Map<String, Object> row, MappingRule rule,
                                  Pattern pattern, Set<String> handledColumns,
                                  Map<String, Map<String, Map<String, Object>>> groupState,
                                  Map<String, String> groupKeyProperties) {
        String targetPath = rule.getTarget();
        Map<String, Map<String, Object>> groups =
                groupState.computeIfAbsent(targetPath, _ -> new LinkedHashMap<>());

        groupKeyProperties.putIfAbsent(targetPath, rule.getGroup().getKeyProperty());

        for (Map.Entry<String, Object> entry : row.entrySet()) {
            String columnName = entry.getKey();
            if (handledColumns.contains(columnName.toLowerCase(Locale.ROOT))) {
                continue;
            }
            Matcher matcher = pattern.matcher(columnName);
            if (matcher.matches() && matcher.groupCount() >= 1) {
                String capturedKey = matcher.replaceAll(rule.getGroup().getBy());
                Object value = entry.getValue();
                Object mapped = RowMapperUtil.applyValueMapping(value, rule.getValueMappings());
                Object converted = RowMapperUtil.convertValue(mapped, rule.getDataType(), rule.getFormat());
                groups.computeIfAbsent(capturedKey, _ -> new LinkedHashMap<>())
                      .put(rule.getGroup().getProperty(), converted);
                handledColumns.add(columnName.toLowerCase(Locale.ROOT));
            }
        }
    }

    // ── Pattern compilation cache ────────────────────────────────────────────

    private Map<Integer, Pattern> getCompiledPatterns(List<MappingRule> rules) {
        Map<Integer, Pattern> cached = compiledPatterns;
        if (cached == null) {
            Map<Integer, Pattern> compiled = new LinkedHashMap<>();
            for (int i = 0; i < rules.size(); i++) {
                MappingRule rule = rules.get(i);
                if (rule.isRegexSource()) {
                    compiled.put(i, Pattern.compile(rule.getRegexBody()));
                }
            }
            compiledPatterns = cached = Map.copyOf(compiled);
        }
        return cached;
    }

    // ── PayloadMapper bridge (deprecated path) ───────────────────────────────

    @Override
    public String mapPayload(java.sql.ResultSet rs,
                             de.knibel.outbox.config.OutboxTableProperties config)
            throws java.sql.SQLException {
        try {
            Map<String, Object> row = de.knibel.outbox.jdbc.ResultSetConverter.toMap(rs);
            Map<String, Object> result = mapRow(row);

            // Handle raw target: return the source column value as-is
            for (MappingRule rule : rules) {
                if (rule.isRawTarget() && rule.getSource() != null) {
                    Object rawValue = result.get(rule.getSource());
                    return rawValue != null ? rawValue.toString() : null;
                }
            }

            return new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(result);
        } catch (Exception e) {
            throw new java.sql.SQLException("Failed to build mapping-rule payload", e);
        }
    }
}
