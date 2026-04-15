package de.knibel.outbox.config;

/**
 * Configuration for array-grouping behaviour within a {@link MappingRule}.
 *
 * <p>When present on a mapping rule, columns matching the rule's regex
 * {@code source} are collected into a JSON array at the rule's
 * {@code target} path.  Columns sharing the same captured-group value
 * (specified by {@link #by}) are merged into a single array element.
 *
 * <h3>Example YAML</h3>
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
 * @see MappingRule
 */
public class GroupConfig {

    /**
     * Capture-group expression (e.g. {@code "$1"}) used to correlate
     * columns into the same array element.  Required.
     */
    private String by;

    /**
     * Optional property name for injecting the captured key into each
     * array element.  When set, every element includes a property with
     * this name whose value is the resolved capture group.
     *
     * <p>Example: with {@code keyProperty = "attribute"} and
     * {@code by = "$1"} matching column {@code new_price}, the element
     * will contain {@code "attribute": "price"}.
     */
    private String keyProperty;

    /**
     * The property name within each array element where the column
     * value is placed.  Required.
     *
     * <p>Example: with {@code property = "after"}, the matched column
     * value is set as {@code {"after": <value>}} in the element.
     */
    private String property;

    public GroupConfig() {}

    public String getBy() { return by; }
    public void setBy(String by) { this.by = by; }

    public String getKeyProperty() { return keyProperty; }
    public void setKeyProperty(String keyProperty) { this.keyProperty = keyProperty; }

    public String getProperty() { return property; }
    public void setProperty(String property) { this.property = property; }
}
