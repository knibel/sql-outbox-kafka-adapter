package de.knibel.outbox.config;

/**
 * Determines the output structure for a group rule in the {@code mappings} DSL.
 *
 * <p>When a {@link MappingRule} has a {@link GroupConfig}, the captured
 * columns are aggregated at the rule's target path.  The {@code GroupType}
 * controls whether the result is a JSON array ({@link #LIST}) or a JSON
 * object/map ({@link #MAP}).
 *
 * @see GroupConfig#getType()
 */
public enum GroupType {

    /**
     * Aggregate captured columns into a JSON array (default).
     *
     * <p>Each unique captured-group value produces one array element.
     * When {@link GroupConfig#getKeyProperty() keyProperty} is set, it is
     * injected as a property in each element.
     *
     * <p>Example output:
     * <pre>
     * "modifications": [
     *   {"attribute": "price", "after": 10.0, "before": 8.0},
     *   {"attribute": "stock", "after": 100,  "before": 50}
     * ]
     * </pre>
     */
    LIST,

    /**
     * Aggregate captured columns into a JSON object (map).
     *
     * <p>Each unique captured-group value becomes a key in the map.
     *
     * <p>When {@link GroupConfig#getProperty() property} is set, each
     * key maps to an object containing the property:
     * <pre>
     * "modifications": {
     *   "price": {"after": 10.0, "before": 8.0},
     *   "stock": {"after": 100,  "before": 50}
     * }
     * </pre>
     *
     * <p>When {@code property} is <em>not</em> set, each key maps
     * directly to the column value:
     * <pre>
     * "modifications": {
     *   "price": 10.0,
     *   "stock": 100
     * }
     * </pre>
     */
    MAP
}
