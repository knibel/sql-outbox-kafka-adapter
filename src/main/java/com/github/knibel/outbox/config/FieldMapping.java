package com.github.knibel.outbox.config;

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

    public FieldMapping() {}

    public FieldMapping(String name, FieldDataType dataType) {
        this.name = name;
        this.dataType = dataType;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public FieldDataType getDataType() { return dataType; }
    public void setDataType(FieldDataType dataType) { this.dataType = dataType; }
}
