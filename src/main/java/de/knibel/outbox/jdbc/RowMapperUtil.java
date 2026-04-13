package de.knibel.outbox.jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.knibel.outbox.config.FieldDataType;
import de.knibel.outbox.config.FieldMapping;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility methods for mapping SQL result-set rows to JSON payloads.
 *
 * <p>Used by {@link OutboxRepository} when the row-mapping strategy is
 * {@code TO_CAMEL_CASE} or {@code CUSTOM}.
 */
public final class RowMapperUtil {

    private RowMapperUtil() {}

    // ── Snake-case → camelCase conversion ────────────────────────────────────

    /**
     * Converts a {@code snake_case} (or {@code SNAKE_CASE}) identifier to
     * {@code camelCase}.
     *
     * <p>Rules:
     * <ul>
     *   <li>Leading underscores are preserved.</li>
     *   <li>Each underscore followed by a letter causes the letter to be
     *       upper-cased and the underscore removed.</li>
     *   <li>Already-camelCase input is returned unchanged.</li>
     * </ul>
     *
     * Examples:
     * <pre>
     *   order_id          → orderId
     *   customer_first_name → customerFirstName
     *   ID                → id
     *   _internal_flag    → _internalFlag
     * </pre>
     */
    public static String toCamelCase(String snakeCase) {
        if (snakeCase == null || snakeCase.isEmpty()) {
            return snakeCase;
        }

        String lower = snakeCase.toLowerCase();
        StringBuilder sb = new StringBuilder(lower.length());
        boolean capitalizeNext = false;

        for (int i = 0; i < lower.length(); i++) {
            char c = lower.charAt(i);
            if (c == '_') {
                // Preserve leading underscores
                if (sb.isEmpty()) {
                    sb.append(c);
                } else {
                    capitalizeNext = true;
                }
            } else {
                if (capitalizeNext) {
                    sb.append(Character.toUpperCase(c));
                    capitalizeNext = false;
                } else {
                    sb.append(c);
                }
            }
        }
        return sb.toString();
    }

    // ── TO_CAMEL_CASE payload builder ────────────────────────────────────────

    /**
     * Reads all columns from the current result-set row and builds a JSON
     * object with {@code camelCase} keys.
     *
     * @param rs           positioned result-set row
     * @param objectMapper Jackson mapper for serialization
     * @return JSON string, e.g. {@code {"orderId":"123","customerName":"John"}}
     */
    public static String buildCamelCasePayload(ResultSet rs, ObjectMapper objectMapper)
            throws SQLException, JsonProcessingException {
        return buildCamelCasePayload(rs, objectMapper, Map.of());
    }

    /**
     * Reads all columns from the current result-set row and builds a JSON
     * object with {@code camelCase} keys, then applies any static fields.
     *
     * @param rs           positioned result-set row
     * @param objectMapper Jackson mapper for serialization
     * @param staticFields constant key-value pairs to inject (may be empty)
     * @return JSON string, e.g. {@code {"orderId":"123","customerName":"John"}}
     */
    public static String buildCamelCasePayload(ResultSet rs, ObjectMapper objectMapper,
                                                Map<String, String> staticFields)
            throws SQLException, JsonProcessingException {
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();

        Map<String, Object> payload = new LinkedHashMap<>();
        for (int i = 1; i <= columnCount; i++) {
            String columnName = meta.getColumnLabel(i);
            String camelKey = toCamelCase(columnName);
            Object value = rs.getObject(i);
            payload.put(camelKey, value);
        }
        applyStaticFields(payload, staticFields);
        return objectMapper.writeValueAsString(payload);
    }

    // ── CUSTOM payload builder ───────────────────────────────────────────────

    /**
     * Reads the columns specified in {@code fieldMappings} from the current
     * result-set row and builds a (possibly nested) JSON object.
     *
     * <p>Dot-separated target paths produce nested objects.  For example, the
     * mapping {@code customer_name → FieldMapping("customer.name")} with the
     * row value {@code "John"} produces {@code {"customer":{"name":"John"}}}.
     *
     * <p>When a {@link FieldMapping} has a non-null {@code dataType}, the
     * raw database value is converted to the specified type before being
     * placed in the JSON payload.  When {@code dataType} is {@code null},
     * the JDBC driver's default Java mapping is used without conversion.
     *
     * <p>When a {@link FieldMapping} has non-empty {@code valueMappings},
     * the raw value is looked up (as a string) in the map and replaced with
     * the mapped value <em>before</em> any data-type conversion.
     *
     * @param rs            positioned result-set row
     * @param fieldMappings source-column → {@link FieldMapping} mapping
     * @param objectMapper  Jackson mapper for serialization
     * @return JSON string representing the mapped payload
     */
    public static String buildCustomPayload(ResultSet rs,
                                            Map<String, FieldMapping> fieldMappings,
                                            ObjectMapper objectMapper)
            throws SQLException, JsonProcessingException {
        return buildCustomPayload(rs, fieldMappings, objectMapper, Map.of());
    }

    /**
     * Reads the columns specified in {@code fieldMappings} from the current
     * result-set row, builds a (possibly nested) JSON object, and then
     * applies any {@code staticFields}.
     *
     * @param rs            positioned result-set row
     * @param fieldMappings source-column → {@link FieldMapping} mapping
     * @param objectMapper  Jackson mapper for serialization
     * @param staticFields  constant key-value pairs to inject (may be empty)
     * @return JSON string representing the mapped payload
     */
    public static String buildCustomPayload(ResultSet rs,
                                            Map<String, FieldMapping> fieldMappings,
                                            ObjectMapper objectMapper,
                                            Map<String, String> staticFields)
            throws SQLException, JsonProcessingException {
        return buildCustomPayload(rs, fieldMappings, Map.of(), objectMapper, staticFields);
    }

    /**
     * Reads columns from the current result-set row using both explicit
     * {@code fieldMappings} and regex-based {@code columnPatterns}, builds a
     * (possibly nested) JSON object, and then applies any {@code staticFields}.
     *
     * <p>Explicit {@code fieldMappings} are processed first and always take
     * precedence: a column already covered by an explicit mapping is skipped
     * when evaluating {@code columnPatterns}.
     *
     * <p>Each key in {@code columnPatterns} is a Java regular-expression
     * pattern matched against the full column label
     * (via {@link Matcher#matches()}).  The {@code name} in the corresponding
     * {@link FieldMapping} may contain back-references ({@code $1}, {@code $2},
     * …) that are resolved against the capturing groups of the matched column
     * name.
     *
     * <p>Example: pattern {@code "neu_(.*)"} with name {@code "neu.$1"}
     * maps column {@code neu_preis} to JSON path {@code neu.preis}.
     *
     * @param rs             positioned result-set row
     * @param fieldMappings  explicit source-column → {@link FieldMapping} mapping
     * @param columnPatterns regex-pattern → {@link FieldMapping} template mapping
     * @param objectMapper   Jackson mapper for serialization
     * @param staticFields   constant key-value pairs to inject (may be empty)
     * @return JSON string representing the mapped payload
     */
    public static String buildCustomPayload(ResultSet rs,
                                            Map<String, FieldMapping> fieldMappings,
                                            Map<String, FieldMapping> columnPatterns,
                                            ObjectMapper objectMapper,
                                            Map<String, String> staticFields)
            throws SQLException, JsonProcessingException {
        Map<String, Object> root = new LinkedHashMap<>();

        // Step 1: Apply explicit field mappings
        Set<String> explicitColumns = new HashSet<>();
        for (Map.Entry<String, FieldMapping> entry : fieldMappings.entrySet()) {
            String sourceColumn = entry.getKey();
            explicitColumns.add(sourceColumn.toLowerCase(Locale.ROOT));
            FieldMapping mapping = entry.getValue();
            Object value = rs.getObject(sourceColumn);
            Object mapped = applyValueMapping(value, mapping.getValueMappings());
            Object converted = convertValue(mapped, mapping.getDataType(), mapping.getFormat());
            setNestedValue(root, mapping.getName(), converted);
        }

        // Step 2: Apply pattern-based mappings to columns not already explicitly mapped
        if (columnPatterns != null && !columnPatterns.isEmpty()) {
            // Compile patterns once before iterating rows
            Map<Pattern, FieldMapping> compiledPatterns = new LinkedHashMap<>();
            for (Map.Entry<String, FieldMapping> patternEntry : columnPatterns.entrySet()) {
                compiledPatterns.put(Pattern.compile(patternEntry.getKey()), patternEntry.getValue());
            }
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = meta.getColumnLabel(i);
                if (explicitColumns.contains(columnName.toLowerCase(Locale.ROOT))) {
                    continue;
                }
                for (Map.Entry<Pattern, FieldMapping> compiledEntry : compiledPatterns.entrySet()) {
                    Matcher matcher = compiledEntry.getKey().matcher(columnName);
                    if (matcher.matches()) {
                        FieldMapping template = compiledEntry.getValue();
                        String targetName = matcher.replaceAll(template.getName());
                        Object value = rs.getObject(i);
                        Object mapped = applyValueMapping(value, template.getValueMappings());
                        Object converted = convertValue(mapped, template.getDataType(), template.getFormat());
                        setNestedValue(root, targetName, converted);
                        break; // first matching pattern wins
                    }
                }
            }
        }

        applyStaticFields(root, staticFields);
        return objectMapper.writeValueAsString(root);
    }

    // ── Static fields ───────────────────────────────────────────────────────

    /**
     * Injects static key-value pairs into the given map.
     *
     * <p>Keys are target JSON field paths (dot-separated for nesting).
     * Applied <em>after</em> column-based mappings so they can override
     * column-derived values.
     */
    static void applyStaticFields(Map<String, Object> root, Map<String, String> staticFields) {
        if (staticFields == null || staticFields.isEmpty()) {
            return;
        }
        for (Map.Entry<String, String> entry : staticFields.entrySet()) {
            setNestedValue(root, entry.getKey(), entry.getValue());
        }
    }

    // ── Value mapping ────────────────────────────────────────────────────────

    /**
     * Looks up the raw value (as a string) in the given value-mapping table.
     * If a match is found, returns the mapped string value; otherwise returns
     * the original value unchanged.  Returns {@code null} values unchanged.
     */
    static Object applyValueMapping(Object value, Map<String, String> valueMappings) {
        if (value == null || valueMappings == null || valueMappings.isEmpty()) {
            return value;
        }
        String key = String.valueOf(value);
        if (valueMappings.containsKey(key)) {
            return valueMappings.get(key);
        }
        return value;
    }

    // ── Type conversion ──────────────────────────────────────────────────────

    /**
     * Converts the given value to the specified {@link FieldDataType}.
     * Returns the value unchanged when {@code dataType} is {@code null} or
     * when the value itself is {@code null}.
     *
     * <p>For {@link FieldDataType#DATE DATE} and
     * {@link FieldDataType#DATETIME DATETIME}, the {@code format} parameter
     * specifies the {@link DateTimeFormatter} pattern used for formatting.
     */
    static Object convertValue(Object value, FieldDataType dataType, String format) {
        if (value == null || dataType == null) {
            return value;
        }
        return switch (dataType) {
            case STRING  -> String.valueOf(value);
            case INTEGER -> (value instanceof Number n) ? n.intValue()
                    : Integer.parseInt(String.valueOf(value));
            case LONG    -> (value instanceof Number n) ? n.longValue()
                    : Long.parseLong(String.valueOf(value));
            case DOUBLE  -> (value instanceof Number n) ? n.doubleValue()
                    : Double.parseDouble(String.valueOf(value));
            case BOOLEAN -> (value instanceof Boolean b) ? b
                    : Boolean.parseBoolean(String.valueOf(value));
            case DECIMAL -> (value instanceof BigDecimal bd) ? bd
                    : new BigDecimal(String.valueOf(value));
            case DATE     -> formatDate(value, format);
            case DATETIME -> formatDateTime(value, format);
        };
    }

    /**
     * Overload without format – delegates with {@code null} format.
     */
    static Object convertValue(Object value, FieldDataType dataType) {
        return convertValue(value, dataType, null);
    }

    // ── Date/DateTime formatting ─────────────────────────────────────────────

    /**
     * Formats a temporal value as a date string using the given pattern.
     * Supports {@link java.sql.Date}, {@link java.sql.Timestamp},
     * {@link LocalDate}, {@link LocalDateTime}, and {@link java.util.Date}.
     */
    private static String formatDate(Object value, String format) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
        LocalDate localDate = toLocalDate(value);
        return formatter.format(localDate);
    }

    /**
     * Formats a temporal value as a date-time string using the given pattern.
     * Supports {@link java.sql.Timestamp}, {@link LocalDateTime},
     * {@link Instant}, {@link LocalDate}, and {@link java.util.Date}.
     */
    private static String formatDateTime(Object value, String format) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
        LocalDateTime localDateTime = toLocalDateTime(value);
        return formatter.format(localDateTime);
    }

    /** Converts various temporal types to {@link LocalDate}. */
    private static LocalDate toLocalDate(Object value) {
        if (value instanceof LocalDate ld)           return ld;
        if (value instanceof java.sql.Date sd)       return sd.toLocalDate();
        if (value instanceof java.sql.Timestamp ts)  return ts.toLocalDateTime().toLocalDate();
        if (value instanceof LocalDateTime ldt)      return ldt.toLocalDate();
        if (value instanceof java.util.Date d)       return d.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        throw new IllegalArgumentException("Cannot convert " + value.getClass().getName() + " to LocalDate");
    }

    /** Converts various temporal types to {@link LocalDateTime}. */
    private static LocalDateTime toLocalDateTime(Object value) {
        if (value instanceof LocalDateTime ldt)      return ldt;
        if (value instanceof java.sql.Timestamp ts)  return ts.toLocalDateTime();
        if (value instanceof Instant inst)           return inst.atZone(ZoneId.systemDefault()).toLocalDateTime();
        if (value instanceof java.sql.Date sd)       return sd.toLocalDate().atStartOfDay();
        if (value instanceof LocalDate ld)           return ld.atStartOfDay();
        if (value instanceof java.util.Date d)       return d.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
        throw new IllegalArgumentException("Cannot convert " + value.getClass().getName() + " to LocalDateTime");
    }

    /**
     * Sets a value at a dot-separated path in a nested map structure.
     * Intermediate maps are created as needed.
     *
     * <p>Example: {@code setNestedValue(root, "customer.address.city", "NY")}
     * produces {@code {"customer":{"address":{"city":"NY"}}}}.
     */
    @SuppressWarnings("unchecked")
    static void setNestedValue(Map<String, Object> root, String path, Object value) {
        String[] segments = path.split("\\.");
        Map<String, Object> current = root;
        for (int i = 0; i < segments.length - 1; i++) {
            current = (Map<String, Object>) current.computeIfAbsent(
                    segments[i], _ -> new LinkedHashMap<String, Object>());
        }
        current.put(segments[segments.length - 1], value);
    }
}
