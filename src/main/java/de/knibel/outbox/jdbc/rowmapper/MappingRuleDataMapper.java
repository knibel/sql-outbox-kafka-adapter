package de.knibel.outbox.jdbc.rowmapper;

import de.knibel.outbox.config.MappingRule;
import de.knibel.outbox.config.OutboxTableProperties;
import de.knibel.outbox.jdbc.RowMapperUtil;
import de.knibel.outbox.repository.OutboxDataMapper;
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
 * {@link OutboxDataMapper} implementation that processes the ordered
 * {@link MappingRule} list from configuration.
 *
 * <p>This mapper operates on plain {@link Map} input (column-name → value)
 * and is therefore independent of any specific persistence technology such
 * as JDBC.  It can be used standalone whenever structured row data needs to
 * be transformed according to the unified mappings DSL.
 *
 * <p>Rules are evaluated top-to-bottom.  Once a column is claimed by a
 * rule, later rules skip it.
 *
 * <p><b>Note:</b> this mapper does <em>not</em> handle the
 * {@link MappingRule#TARGET_RAW _raw} target sentinel.  Callers that need
 * raw-payload pass-through should check for it before delegating to this
 * mapper.
 *
 * @see MappingRule
 * @see OutboxDataMapper
 */
public class MappingRuleDataMapper implements OutboxDataMapper {

    /** Pre-compiled regex patterns cached per rule index. */
    private volatile Map<Integer, Pattern> compiledPatterns;

    public MappingRuleDataMapper() {}

    @Override
    public Map<String, Object> map(Map<String, Object> row, OutboxTableProperties config) {
        List<MappingRule> rules = config.getMappings();
        Map<Integer, Pattern> patterns = getCompiledPatterns(rules);

        Map<String, Object> root = new LinkedHashMap<>();
        Set<String> handledColumns = new HashSet<>();

        // Intermediate group state: targetPath → (capturedKey → {property → value})
        Map<String, Map<String, Map<String, Object>>> groupState = new LinkedHashMap<>();
        // Track keyProperty and group configs per target path
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

            if (rule.getSource() != null && !rule.isWildcardSource() && !rule.isRawTarget()) {
                String sourceColumn = rule.getSource();
                handledColumns.add(sourceColumn.toLowerCase(Locale.ROOT));
                Object value = findValue(row, sourceColumn);
                Object mapped = RowMapperUtil.applyValueMapping(value, rule.getValueMappings());
                Object converted = RowMapperUtil.convertValue(mapped, rule.getDataType(), rule.getFormat());
                RowMapperUtil.setNestedValue(root, rule.getTarget(), converted);
            }
        }

        // Finalize all group rules: convert grouped entries to JSON arrays
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

    private void processRegexRule(Map<String, Object> row,
                                  MappingRule rule, Pattern pattern,
                                  Set<String> handledColumns, Map<String, Object> root) {
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

    private void processGroupRule(Map<String, Object> row,
                                  MappingRule rule, Pattern pattern,
                                  Set<String> handledColumns,
                                  Map<String, Map<String, Map<String, Object>>> groupState,
                                  Map<String, String> groupKeyProperties) {
        String targetPath = rule.getTarget();
        Map<String, Map<String, Object>> groups =
                groupState.computeIfAbsent(targetPath, _ -> new LinkedHashMap<>());

        // Record keyProperty – first rule wins if multiple rules target same path
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

    // ── Helpers ──────────────────────────────────────────────────────────────

    /**
     * Case-insensitive column lookup in the row map.
     */
    private static Object findValue(Map<String, Object> row, String columnName) {
        // Try exact match first (most common)
        Object value = row.get(columnName);
        if (value != null || row.containsKey(columnName)) {
            return value;
        }
        // Fall back to case-insensitive lookup
        String lowerKey = columnName.toLowerCase(Locale.ROOT);
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            if (entry.getKey().toLowerCase(Locale.ROOT).equals(lowerKey)) {
                return entry.getValue();
            }
        }
        return null;
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
}
