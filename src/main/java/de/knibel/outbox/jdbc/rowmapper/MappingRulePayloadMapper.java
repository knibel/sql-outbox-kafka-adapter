package de.knibel.outbox.jdbc.rowmapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.knibel.outbox.config.MappingRule;
import de.knibel.outbox.config.OutboxTableProperties;
import de.knibel.outbox.jdbc.RowMapperUtil;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
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
 * {@link PayloadMapper} that processes the ordered
 * {@link MappingRule} list from configuration.
 *
 * <p>Rules are evaluated top-to-bottom.  Once a column is claimed by a
 * rule, later rules skip it.
 *
 * @see MappingRule
 */
public class MappingRulePayloadMapper implements PayloadMapper {

    private final ObjectMapper objectMapper;

    /** Pre-compiled regex patterns cached per rule index. */
    private volatile Map<Integer, Pattern> compiledPatterns;

    public MappingRulePayloadMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public String mapPayload(ResultSet rs, OutboxTableProperties config) throws SQLException {
        List<MappingRule> rules = config.getMappings();
        Map<Integer, Pattern> patterns = getCompiledPatterns(rules);

        // Check for raw target first – it short-circuits everything
        for (MappingRule rule : rules) {
            if (rule.isRawTarget() && rule.getSource() != null) {
                return rs.getString(rule.getSource());
            }
        }

        Map<String, Object> root = new LinkedHashMap<>();
        Set<String> handledColumns = new HashSet<>();

        // Intermediate group state: targetPath → (capturedKey → {property → value})
        Map<String, Map<String, Map<String, Object>>> groupState = new LinkedHashMap<>();
        // Track keyProperty and group configs per target path
        Map<String, String> groupKeyProperties = new LinkedHashMap<>();

        // Lazily load metadata only when needed
        ResultSetMetaData meta = null;
        int columnCount = 0;

        for (int ruleIndex = 0; ruleIndex < rules.size(); ruleIndex++) {
            MappingRule rule = rules.get(ruleIndex);

            if (rule.isStaticRule()) {
                RowMapperUtil.setNestedValue(root, rule.getTarget(), rule.getValue());
                continue;
            }

            if (rule.isCamelCaseTarget() && rule.isWildcardSource()) {
                if (meta == null) {
                    meta = rs.getMetaData();
                    columnCount = meta.getColumnCount();
                }
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = meta.getColumnLabel(i);
                    if (handledColumns.contains(columnName.toLowerCase(Locale.ROOT))) {
                        continue;
                    }
                    String camelKey = RowMapperUtil.toCamelCase(columnName);
                    Object value = rs.getObject(i);
                    root.put(camelKey, value);
                    handledColumns.add(columnName.toLowerCase(Locale.ROOT));
                }
                continue;
            }

            if (rule.isRegexSource()) {
                Pattern pattern = patterns.get(ruleIndex);
                if (meta == null) {
                    meta = rs.getMetaData();
                    columnCount = meta.getColumnCount();
                }

                if (rule.isGroupRule()) {
                    processGroupRule(rs, meta, columnCount, rule, pattern,
                            handledColumns, groupState, groupKeyProperties);
                } else {
                    processRegexRule(rs, meta, columnCount, rule, pattern, handledColumns, root);
                }
                continue;
            }

            if (rule.getSource() != null && !rule.isWildcardSource()) {
                String sourceColumn = rule.getSource();
                handledColumns.add(sourceColumn.toLowerCase(Locale.ROOT));
                Object value = rs.getObject(sourceColumn);
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

        try {
            return objectMapper.writeValueAsString(root);
        } catch (Exception e) {
            throw new SQLException("Failed to serialize payload to JSON", e);
        }
    }

    // ── Regex pattern mapping ────────────────────────────────────────────────

    private void processRegexRule(ResultSet rs, ResultSetMetaData meta, int columnCount,
                                  MappingRule rule, Pattern pattern,
                                  Set<String> handledColumns, Map<String, Object> root)
            throws SQLException {
        for (int i = 1; i <= columnCount; i++) {
            String columnName = meta.getColumnLabel(i);
            if (handledColumns.contains(columnName.toLowerCase(Locale.ROOT))) {
                continue;
            }
            Matcher matcher = pattern.matcher(columnName);
            if (matcher.matches()) {
                String targetName = matcher.replaceAll(rule.getTarget());
                Object value = rs.getObject(i);
                Object mapped = RowMapperUtil.applyValueMapping(value, rule.getValueMappings());
                Object converted = RowMapperUtil.convertValue(mapped, rule.getDataType(), rule.getFormat());
                RowMapperUtil.setNestedValue(root, targetName, converted);
                handledColumns.add(columnName.toLowerCase(Locale.ROOT));
            }
        }
    }

    // ── Array grouping ───────────────────────────────────────────────────────

    private void processGroupRule(ResultSet rs, ResultSetMetaData meta, int columnCount,
                                  MappingRule rule, Pattern pattern,
                                  Set<String> handledColumns,
                                  Map<String, Map<String, Map<String, Object>>> groupState,
                                  Map<String, String> groupKeyProperties)
            throws SQLException {
        String targetPath = rule.getTarget();
        Map<String, Map<String, Object>> groups =
                groupState.computeIfAbsent(targetPath, _ -> new LinkedHashMap<>());

        // Record keyProperty – first rule wins if multiple rules target same path
        groupKeyProperties.putIfAbsent(targetPath, rule.getGroup().getKeyProperty());

        for (int i = 1; i <= columnCount; i++) {
            String columnName = meta.getColumnLabel(i);
            if (handledColumns.contains(columnName.toLowerCase(Locale.ROOT))) {
                continue;
            }
            Matcher matcher = pattern.matcher(columnName);
            if (matcher.matches() && matcher.groupCount() >= 1) {
                String capturedKey = matcher.replaceAll(rule.getGroup().getBy());
                Object value = rs.getObject(i);
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
}
