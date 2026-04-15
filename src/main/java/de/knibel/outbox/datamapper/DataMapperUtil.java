package de.knibel.outbox.datamapper;

import de.knibel.outbox.config.FieldDataType;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Utility methods for mapping data rows to structured output maps.
 *
 * <p>Provides helpers for snake→camelCase conversion, value mapping,
 * data type conversion, date/datetime formatting, and nested map
 * path handling.  Used by {@link MappingRuleDataMapper}.
 */
public final class DataMapperUtil {

    private DataMapperUtil() {}

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

    // ── Value mapping ────────────────────────────────────────────────────────

    /**
     * Looks up the raw value (as a string) in the given value-mapping table.
     * If a match is found, returns the mapped string value; otherwise returns
     * the original value unchanged.  Returns {@code null} values unchanged.
     */
    public static Object applyValueMapping(Object value, Map<String, String> valueMappings) {
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
    public static Object convertValue(Object value, FieldDataType dataType, String format) {
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
    public static Object convertValue(Object value, FieldDataType dataType) {
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
    public static void setNestedValue(Map<String, Object> root, String path, Object value) {
        String[] segments = path.split("\\.");
        Map<String, Object> current = root;
        for (int i = 0; i < segments.length - 1; i++) {
            current = (Map<String, Object>) current.computeIfAbsent(
                    segments[i], _ -> new LinkedHashMap<String, Object>());
        }
        current.put(segments[segments.length - 1], value);
    }
}
