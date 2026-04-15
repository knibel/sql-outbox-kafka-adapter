package de.knibel.outbox.config;

/**
 * Supported data types for converting database column values to JSON field
 * values in the {@code mappings} DSL.
 *
 * <p>When a {@link MappingRule#getDataType() dataType} is configured, the
 * raw database value is converted to the specified type before being written
 * to the JSON payload.  When no {@code dataType} is set, the value is used
 * as-is (i.e. the JDBC driver's default Java mapping applies).
 */
public enum FieldDataType {

    /** Convert the value to {@link String} via {@code String.valueOf()}. */
    STRING,

    /** Convert the value to {@link Integer} via {@code Number.intValue()}. */
    INTEGER,

    /** Convert the value to {@link Long} via {@code Number.longValue()}. */
    LONG,

    /** Convert the value to {@link Double} via {@code Number.doubleValue()}. */
    DOUBLE,

    /** Convert the value to {@link Boolean}. */
    BOOLEAN,

    /** Convert the value to {@link java.math.BigDecimal}. */
    DECIMAL,

    /**
     * Format the value as a date string.
     * Requires {@link MappingRule#getFormat() format} to be set
     * (e.g. {@code "yyyy-MM-dd"}).
     *
     * <p>Accepts {@link java.sql.Date}, {@link java.sql.Timestamp},
     * {@link java.time.LocalDate}, {@link java.time.LocalDateTime},
     * and {@link java.util.Date}.
     */
    DATE,

    /**
     * Format the value as a date-time string.
     * Requires {@link MappingRule#getFormat() format} to be set
     * (e.g. {@code "yyyy-MM-dd'T'HH:mm:ss"}).
     *
     * <p>Accepts {@link java.sql.Timestamp},
     * {@link java.time.LocalDateTime}, {@link java.time.Instant},
     * and {@link java.util.Date}.
     */
    DATETIME
}
