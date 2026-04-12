package com.github.knibel.outbox.config;

/**
 * Defines how SQL row columns are mapped to the Kafka record value (payload).
 *
 * <ul>
 *   <li>{@link #PAYLOAD_COLUMN} (default) – reads the pre-serialized JSON
 *       payload from a single configured {@code payloadColumn}.
 *       This is the original behaviour.</li>
 *   <li>{@link #TO_CAMEL_CASE} – selects <em>all</em> columns from the row
 *       and builds a flat JSON object where each {@code snake_case} column
 *       name is converted to {@code camelCase}.
 *       Example: {@code order_id → orderId}.</li>
 *   <li>{@link #CUSTOM} – requires an explicit {@code fieldMappings}
 *       configuration that maps source column names to target JSON paths.
 *       Dot-separated paths produce nested JSON objects.
 *       Example mapping: {@code customer_name → customer.name} produces
 *       {@code {"customer":{"name":"…"}}}.</li>
 * </ul>
 */
public enum RowMappingStrategy {

    /**
     * Read the Kafka record value from a single pre-serialized JSON column
     * ({@code payloadColumn}).  This is the default and preserves backward
     * compatibility.
     */
    PAYLOAD_COLUMN,

    /**
     * Select all columns and convert every {@code snake_case} column name
     * to {@code camelCase} in the resulting JSON payload.
     */
    TO_CAMEL_CASE,

    /**
     * Use an explicit column-to-JSON-path mapping provided via
     * {@code fieldMappings}.  Supports nested objects through
     * dot-separated target paths.
     */
    CUSTOM
}
