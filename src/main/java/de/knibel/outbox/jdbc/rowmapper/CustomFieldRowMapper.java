package de.knibel.outbox.jdbc.rowmapper;

import de.knibel.outbox.config.FieldMapping;
import de.knibel.outbox.config.ListMapping;
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
 * Applies explicit {@code fieldMappings}, regex-based
 * {@code columnPatterns}, and list-building {@code listMappings}
 * to map source columns to target JSON paths.
 *
 * <p>Operates entirely on a plain {@code Map<String, Object>} — no JDBC
 * or Jackson dependency at mapping time.
 *
 * @see CustomFieldPayloadMapper
 */
@SuppressWarnings("removal") // Intentionally implements deprecated PayloadMapper during transition
public class CustomFieldRowMapper implements PayloadMapper, OutboxRowMapper {

    private final Map<String, FieldMapping> fieldMappings;
    private final Map<Pattern, FieldMapping> columnPatterns;
    private final Map<String, ListMapping> listMappings;
    private final Map<String, String> staticFields;

    /**
     * @param fieldMappings  explicit source-column → {@link FieldMapping} mapping
     * @param columnPatterns pre-compiled regex-pattern → {@link FieldMapping} template mapping
     * @param listMappings   target-path → {@link ListMapping} for array construction
     * @param staticFields   constant key-value pairs injected last (may be empty)
     */
    public CustomFieldRowMapper(Map<String, FieldMapping> fieldMappings,
                                Map<Pattern, FieldMapping> columnPatterns,
                                Map<String, ListMapping> listMappings,
                                Map<String, String> staticFields) {
        this.fieldMappings = fieldMappings;
        this.columnPatterns = columnPatterns;
        this.listMappings = listMappings;
        this.staticFields = staticFields;
    }

    @Override
    public Map<String, Object> mapRow(Map<String, Object> row) {
        Map<String, Object> root = new LinkedHashMap<>();
        Set<String> handledColumns = new HashSet<>();

        // Step 1: Apply explicit field mappings
        for (Map.Entry<String, FieldMapping> entry : fieldMappings.entrySet()) {
            String sourceColumn = entry.getKey();
            handledColumns.add(sourceColumn.toLowerCase(Locale.ROOT));
            FieldMapping mapping = entry.getValue();
            Object value = row.get(sourceColumn);
            Object mapped = RowMapperUtil.applyValueMapping(value, mapping.getValueMappings());
            Object converted = RowMapperUtil.convertValue(mapped, mapping.getDataType(), mapping.getFormat());
            RowMapperUtil.setNestedValue(root, mapping.getName(), converted);
        }

        // Step 2: Apply pattern-based mappings to columns not already explicitly mapped
        if (columnPatterns != null && !columnPatterns.isEmpty()) {
            for (Map.Entry<String, Object> colEntry : row.entrySet()) {
                String columnName = colEntry.getKey();
                if (handledColumns.contains(columnName.toLowerCase(Locale.ROOT))) {
                    continue;
                }
                for (Map.Entry<Pattern, FieldMapping> compiledEntry : columnPatterns.entrySet()) {
                    Matcher matcher = compiledEntry.getKey().matcher(columnName);
                    if (matcher.matches()) {
                        FieldMapping template = compiledEntry.getValue();
                        String targetName = matcher.replaceAll(template.getName());
                        Object value = colEntry.getValue();
                        Object mapped = RowMapperUtil.applyValueMapping(value, template.getValueMappings());
                        Object converted = RowMapperUtil.convertValue(mapped, template.getDataType(), template.getFormat());
                        RowMapperUtil.setNestedValue(root, targetName, converted);
                        handledColumns.add(columnName.toLowerCase(Locale.ROOT));
                        break; // first matching pattern wins
                    }
                }
            }
        }

        // Step 3: Apply list mappings to columns not handled above
        if (listMappings != null && !listMappings.isEmpty()) {
            for (Map.Entry<String, ListMapping> listEntry : listMappings.entrySet()) {
                String targetPath = listEntry.getKey();
                ListMapping listMapping = listEntry.getValue();
                Map<Pattern, FieldMapping> compiledPatterns = listMapping.getCompiledPatterns();

                // Group by first capturing group: capturedKey → {propertyName → value}
                Map<String, Map<String, Object>> groups = new LinkedHashMap<>();
                for (Map.Entry<String, Object> colEntry : row.entrySet()) {
                    String columnName = colEntry.getKey();
                    if (handledColumns.contains(columnName.toLowerCase(Locale.ROOT))) {
                        continue;
                    }
                    for (Map.Entry<Pattern, FieldMapping> patEntry : compiledPatterns.entrySet()) {
                        Matcher matcher = patEntry.getKey().matcher(columnName);
                        if (matcher.matches() && matcher.groupCount() >= 1) {
                            String capturedKey = matcher.group(1);
                            FieldMapping template = patEntry.getValue();
                            Object value = colEntry.getValue();
                            Object mapped = RowMapperUtil.applyValueMapping(value, template.getValueMappings());
                            Object converted = RowMapperUtil.convertValue(mapped, template.getDataType(), template.getFormat());
                            groups.computeIfAbsent(capturedKey, _ -> new LinkedHashMap<>())
                                  .put(template.getName(), converted);
                            handledColumns.add(columnName.toLowerCase(Locale.ROOT));
                            break; // first matching pattern wins for this column
                        }
                    }
                }

                // Convert grouped entries to an array
                List<Map<String, Object>> list = new ArrayList<>();
                for (Map.Entry<String, Map<String, Object>> groupEntry : groups.entrySet()) {
                    Map<String, Object> element = new LinkedHashMap<>();
                    if (listMapping.getKeyProperty() != null && !listMapping.getKeyProperty().isBlank()) {
                        element.put(listMapping.getKeyProperty(), groupEntry.getKey());
                    }
                    element.putAll(groupEntry.getValue());
                    list.add(element);
                }
                if (!list.isEmpty()) {
                    RowMapperUtil.setNestedValue(root, targetPath, list);
                }
            }
        }

        RowMapperUtil.applyStaticFields(root, staticFields);
        return root;
    }

    // ── PayloadMapper bridge (deprecated path) ───────────────────────────────

    @Override
    public String mapPayload(java.sql.ResultSet rs,
                             de.knibel.outbox.config.OutboxTableProperties config)
            throws java.sql.SQLException {
        try {
            return RowMapperUtil.buildCustomPayload(
                    rs, config.getFieldMappings(), config.getCompiledColumnPatterns(),
                    config.getListMappings(),
                    new com.fasterxml.jackson.databind.ObjectMapper(), config.getStaticFields());
        } catch (Exception e) {
            throw new java.sql.SQLException("Failed to build custom payload", e);
        }
    }
}
