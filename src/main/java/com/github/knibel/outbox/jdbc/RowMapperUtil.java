package com.github.knibel.outbox.jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
     * mapping {@code {"customer_name": "customer.name"}} with the row value
     * {@code "John"} produces {@code {"customer":{"name":"John"}}}.
     *
     * @param rs            positioned result-set row
     * @param fieldMappings source-column → target-JSON-path mapping
     * @param objectMapper  Jackson mapper for serialization
     * @return JSON string representing the mapped payload
     */
    public static String buildCustomPayload(ResultSet rs,
                                            Map<String, String> fieldMappings,
                                            ObjectMapper objectMapper)
            throws SQLException, JsonProcessingException {
        Map<String, Object> root = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : fieldMappings.entrySet()) {
            String sourceColumn = entry.getKey();
            String targetPath = entry.getValue();
            Object value = rs.getObject(sourceColumn);
            setNestedValue(root, targetPath, value);
        }
        return objectMapper.writeValueAsString(root);
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
