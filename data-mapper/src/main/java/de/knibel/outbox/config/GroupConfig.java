package de.knibel.outbox.config;

/**
 * Configuration for grouping behaviour within a {@link MappingRule}.
 *
 * <p>When present on a mapping rule, columns matching the rule's regex
 * {@code source} are collected at the rule's {@code target} path.
 * The output structure is controlled by {@link #type}:
 *
 * <ul>
 *   <li>{@link GroupType#LIST} (default) – produces a JSON array where
 *       columns sharing the same captured-group value (specified by
 *       {@link #by}) are merged into a single array element.</li>
 *   <li>{@link GroupType#MAP} – produces a JSON object/map where each
 *       unique captured-group value becomes a key.  When {@link #property}
 *       is set, each key maps to a nested object; when it is {@code null},
 *       each key maps directly to the column value.</li>
 * </ul>
 *
 * <h3>Example YAML – list (default)</h3>
 * <pre>
 * mappings:
 *   - source: /new_(.*)/
 *     target: modifications
 *     group:
 *       by: $1
 *       keyProperty: attribute
 *       property: after
 * </pre>
 *
 * <h3>Example YAML – map (simple)</h3>
 * <pre>
 * mappings:
 *   - source: /new_(.*)/
 *     target: modifications
 *     group:
 *       by: $1
 *       type: MAP
 * </pre>
 *
 * <h3>Example YAML – map (with property)</h3>
 * <pre>
 * mappings:
 *   - source: /new_(.*)/
 *     target: modifications
 *     group:
 *       by: $1
 *       type: MAP
 *       property: after
 * </pre>
 *
 * @see MappingRule
 * @see GroupType
 */
public class GroupConfig {

    /**
     * Capture-group expression (e.g. {@code "$1"}) used to correlate
     * columns into the same element or map key.  Required.
     */
    private String by;

    /**
     * Output structure type.  Defaults to {@link GroupType#LIST} when
     * not set, preserving backward compatibility.
     */
    private GroupType type;

    /**
     * Optional property name for injecting the captured key into each
     * array element.  Only meaningful when {@link #type} is
     * {@link GroupType#LIST}.
     *
     * <p>Example: with {@code keyProperty = "attribute"} and
     * {@code by = "$1"} matching column {@code new_price}, the element
     * will contain {@code "attribute": "price"}.
     */
    private String keyProperty;

    /**
     * The property name within each element where the column value is
     * placed.
     *
     * <ul>
     *   <li>For {@link GroupType#LIST}: required.  Sets the property in
     *       each array element (e.g. {@code {"after": <value>}}).</li>
     *   <li>For {@link GroupType#MAP} with property: each key maps to an
     *       object containing this property.</li>
     *   <li>For {@link GroupType#MAP} without property: each key maps
     *       directly to the column value.</li>
     * </ul>
     */
    private String property;

    public GroupConfig() {}

    /**
     * Returns the effective group type, defaulting to {@link GroupType#LIST}
     * when not explicitly set.
     */
    public GroupType getEffectiveType() {
        return type != null ? type : GroupType.LIST;
    }

    public String getBy() { return by; }
    public void setBy(String by) { this.by = by; }

    public GroupType getType() { return type; }
    public void setType(GroupType type) { this.type = type; }

    public String getKeyProperty() { return keyProperty; }
    public void setKeyProperty(String keyProperty) { this.keyProperty = keyProperty; }

    public String getProperty() { return property; }
    public void setProperty(String property) { this.property = property; }
}
