package com.github.knibel.outbox.jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.knibel.outbox.config.FieldDataType;
import com.github.knibel.outbox.config.FieldMapping;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

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
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();

        Map<String, Object> payload = new LinkedHashMap<>();
        for (int i = 1; i <= columnCount; i++) {
            String columnName = meta.getColumnLabel(i);
            String camelKey = toCamelCase(columnName);
            Object value = rs.getObject(i);
            payload.put(camelKey, value);
        }
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
     * @param rs            positioned result-set row
     * @param fieldMappings source-column → {@link FieldMapping} mapping
     * @param objectMapper  Jackson mapper for serialization
     * @return JSON string representing the mapped payload
     */
    public static String buildCustomPayload(ResultSet rs,
                                            Map<String, FieldMapping> fieldMappings,
                                            ObjectMapper objectMapper)
            throws SQLException, JsonProcessingException {
        Map<String, Object> root = new LinkedHashMap<>();
        for (Map.Entry<String, FieldMapping> entry : fieldMappings.entrySet()) {
            String sourceColumn = entry.getKey();
            FieldMapping mapping = entry.getValue();
            Object value = rs.getObject(sourceColumn);
            Object converted = convertValue(value, mapping.getDataType());
            setNestedValue(root, mapping.getName(), converted);
        }
        return objectMapper.writeValueAsString(root);
    }

    // ── Type conversion ──────────────────────────────────────────────────────

    /**
     * Converts the given value to the specified {@link FieldDataType}.
     * Returns the value unchanged when {@code dataType} is {@code null} or
     * when the value itself is {@code null}.
     */
    static Object convertValue(Object value, FieldDataType dataType) {
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
        };
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
