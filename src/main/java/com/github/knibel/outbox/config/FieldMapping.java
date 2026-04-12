package com.github.knibel.outbox.config;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Describes how a single source SQL column is mapped to a target JSON field
 * in the {@link RowMappingStrategy#CUSTOM CUSTOM} row-mapping strategy.
 *
 * <ul>
 *   <li>{@link #name} (required) – the target JSON field path.
 *       Dot-separated paths produce nested JSON objects
 *       (e.g. {@code "customer.address.city"}).</li>
 *   <li>{@link #dataType} (optional) – when set, the raw database value is
 *       converted to the specified type before being placed in the JSON
 *       payload.  When {@code null}, the JDBC driver's default Java mapping
 *       is used without conversion.</li>
 *   <li>{@link #format} (optional, required for {@code DATE}/{@code DATETIME}) –
 *       a {@link java.time.format.DateTimeFormatter} pattern used to format
 *       temporal values (e.g. {@code "yyyy-MM-dd"} or
 *       {@code "yyyy-MM-dd'T'HH:mm:ss"}).</li>
 *   <li>{@link #valueMappings} (optional) – maps raw database values (as
 *       strings) to replacement output values.  Applied <em>before</em> any
 *       {@code dataType} conversion.  Useful for translating integer codes
 *       to human-readable enum strings
 *       (e.g. {@code "1" → "ACTIVE"}, {@code "2" → "INACTIVE"}).</li>
 * </ul>
 *
 * <p>Example YAML:
 * <pre>
 * fieldMappings:
 *   order_id:
 *     name: orderId
 *     dataType: STRING
 *   total_amount:
 *     name: totalAmount
 *     dataType: DOUBLE
 *   created_at:
 *     name: createdAt
 *     dataType: DATETIME
 *     format: "yyyy-MM-dd'T'HH:mm:ss"
 *   status_code:
 *     name: status
 *     valueMappings:
 *       "1": ACTIVE
 *       "2": INACTIVE
 *       "3": DELETED
 *   customer_name:
 *     name: customer.name
 *     # no dataType → uses default database type
 * </pre>
 */
public class FieldMapping {

    /** Target JSON field path (supports dot-separated nesting). Required. */
    private String name;

    /**
     * Optional target data type.  When set, the raw database value is
     * converted to this type.  When {@code null}, the value is used as-is.
     */
    private FieldDataType dataType;

    /**
     * Date/datetime format pattern (e.g. {@code "yyyy-MM-dd"}).
     * Required when {@code dataType} is {@link FieldDataType#DATE DATE} or
     * {@link FieldDataType#DATETIME DATETIME}; ignored for other types.
     */
    private String format;

    /**
     * Optional value mapping table.  Keys are the string representation of
     * raw database values; values are the replacement output values.
     *
     * <p>Applied <em>before</em> any {@code dataType} conversion.  If the
     * raw value (as a string) matches a key, the corresponding map value is
     * used instead.  If no match is found, the original value is kept.
     *
     * <p>Example: mapping integer status codes to enum names:
     * <pre>
     * valueMappings:
     *   "1": ACTIVE
     *   "2": INACTIVE
     * </pre>
     */
    private Map<String, String> valueMappings = new LinkedHashMap<>();

    public FieldMapping() {}

    public FieldMapping(String name, FieldDataType dataType) {
        this.name = name;
        this.dataType = dataType;
    }

    public FieldMapping(String name, FieldDataType dataType, String format) {
        this.name = name;
        this.dataType = dataType;
        this.format = format;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public FieldDataType getDataType() { return dataType; }
    public void setDataType(FieldDataType dataType) { this.dataType = dataType; }

    public String getFormat() { return format; }
    public void setFormat(String format) { this.format = format; }

    public Map<String, String> getValueMappings() { return valueMappings; }
    public void setValueMappings(Map<String, String> valueMappings) { this.valueMappings = valueMappings; }
}
