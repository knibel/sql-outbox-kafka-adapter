package de.knibel.outbox.config;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A single mapping rule within the unified {@code mappings} DSL.
 *
 * <p>Each rule describes how one or more SQL result-set columns are mapped
 * to fields in the output JSON payload.  Rules are evaluated top-to-bottom;
 * once a column is claimed by a rule, later rules skip it.
 *
 * <h3>Rule types</h3>
 * <table>
 *   <tr><th>source</th><th>target</th><th>Behaviour</th></tr>
 *   <tr><td>literal column name</td><td>JSON path</td><td>Explicit column→field mapping (replaces {@code fieldMappings})</td></tr>
 *   <tr><td>{@code /regex/}</td><td>JSON path with back-refs</td><td>Regex pattern mapping (replaces {@code columnPatterns})</td></tr>
 *   <tr><td>{@code /regex/}</td><td>JSON path</td><td>Array grouping when {@code group} is set (replaces {@code listMappings})</td></tr>
 *   <tr><td>{@code *}</td><td>{@code _camelCase}</td><td>Auto-discover all remaining columns, snake→camel (replaces {@code TO_CAMEL_CASE})</td></tr>
 *   <tr><td>literal column name</td><td>{@code _raw}</td><td>Use column value verbatim as entire payload (replaces {@code PAYLOAD_COLUMN})</td></tr>
 *   <tr><td><em>absent</em></td><td>JSON path</td><td>Static constant injection (replaces {@code staticFields})</td></tr>
 * </table>
 *
 * <h3>Example YAML</h3>
 * <pre>
 * mappings:
 *   - source: order_id
 *     target: orderId
 *     dataType: STRING
 *
 *   - source: /new_(.*)/
 *     target: modifications
 *     group:
 *       by: $1
 *       keyProperty: attribute
 *       property: after
 *
 *   - value: "SomeType"
 *     target: eventType
 *
 *   - source: "*"
 *     target: _camelCase
 * </pre>
 *
 * @see GroupConfig
 * @see OutboxTableProperties#getMappings()
 */
public class MappingRule {

    /** Sentinel target value: use the source column value as the entire raw JSON payload. */
    public static final String TARGET_RAW = "_raw";

    /** Sentinel target value: auto-discover all unhandled columns and camelCase their names. */
    public static final String TARGET_CAMEL_CASE = "_camelCase";

    /** Sentinel source value: wildcard matching all remaining unhandled columns. */
    public static final String SOURCE_WILDCARD = "*";

    /**
     * The source column specifier.  Can be:
     * <ul>
     *   <li>A literal column name (exact match, case-insensitive)</li>
     *   <li>A regex wrapped in {@code /…/} (e.g. {@code "/new_(.*)/"})</li>
     *   <li>{@code "*"} – wildcard for all remaining unhandled columns</li>
     *   <li>{@code null} – when {@link #value} is used instead (static rule)</li>
     * </ul>
     */
    private String source;

    /**
     * The target JSON field path.  Dot-separated paths produce nested
     * JSON objects (e.g. {@code "customer.address.city"}).
     *
     * <p>Special sentinel values:
     * <ul>
     *   <li>{@code "_raw"} – the column value is the entire payload</li>
     *   <li>{@code "_camelCase"} – auto-discover and camelCase remaining columns</li>
     * </ul>
     */
    private String target;

    /**
     * Static constant value to inject.  Mutually exclusive with
     * {@link #source}: when {@code value} is set, {@code source} must
     * be {@code null}.
     */
    private String value;

    /**
     * Optional target data type.  When set, the raw database value is
     * converted to the specified type.  When {@code null}, the value
     * is used as-is.
     */
    private FieldDataType dataType;

    /**
     * Date/datetime format pattern (e.g. {@code "yyyy-MM-dd"}).
     * Required when {@code dataType} is {@link FieldDataType#DATE} or
     * {@link FieldDataType#DATETIME}; ignored for other types.
     */
    private String format;

    /**
     * Optional value mapping table.  Keys are the string representation
     * of raw database values; values are the replacement output values.
     * Applied <em>before</em> any {@code dataType} conversion.
     */
    private Map<String, String> valueMappings = new LinkedHashMap<>();

    /**
     * Optional group configuration for array aggregation.  When present,
     * the rule collects matching columns into a JSON array at the
     * {@code target} path.  Requires a regex {@code source}.
     */
    private GroupConfig group;

    public MappingRule() {}

    // ── Convenience query methods ────────────────────────────────────────────

    /**
     * Returns {@code true} if this rule's source is a regex pattern
     * (wrapped in {@code /…/}).
     */
    public boolean isRegexSource() {
        return source != null && source.startsWith("/") && source.endsWith("/") && source.length() > 2;
    }

    /**
     * Returns the regex body without the enclosing slashes.
     *
     * @throws IllegalStateException if the source is not a regex
     */
    public String getRegexBody() {
        if (!isRegexSource()) {
            throw new IllegalStateException("Source is not a regex: " + source);
        }
        return source.substring(1, source.length() - 1);
    }

    /**
     * Returns {@code true} if this rule's source is the wildcard ({@code "*"}).
     */
    public boolean isWildcardSource() {
        return SOURCE_WILDCARD.equals(source);
    }

    /**
     * Returns {@code true} if this is a static value rule
     * (no source, value present).
     */
    public boolean isStaticRule() {
        return source == null && value != null;
    }

    /**
     * Returns {@code true} if this rule targets the raw payload sentinel.
     */
    public boolean isRawTarget() {
        return TARGET_RAW.equals(target);
    }

    /**
     * Returns {@code true} if this rule targets the camelCase sentinel.
     */
    public boolean isCamelCaseTarget() {
        return TARGET_CAMEL_CASE.equals(target);
    }

    /**
     * Returns {@code true} if this rule has a group configuration
     * (array aggregation).
     */
    public boolean isGroupRule() {
        return group != null;
    }

    // ── Getters / setters ────────────────────────────────────────────────────

    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }

    public String getTarget() { return target; }
    public void setTarget(String target) { this.target = target; }

    public String getValue() { return value; }
    public void setValue(String value) { this.value = value; }

    public FieldDataType getDataType() { return dataType; }
    public void setDataType(FieldDataType dataType) { this.dataType = dataType; }

    public String getFormat() { return format; }
    public void setFormat(String format) { this.format = format; }

    public Map<String, String> getValueMappings() { return valueMappings; }
    public void setValueMappings(Map<String, String> valueMappings) { this.valueMappings = valueMappings; }

    public GroupConfig getGroup() { return group; }
    public void setGroup(GroupConfig group) { this.group = group; }
}
