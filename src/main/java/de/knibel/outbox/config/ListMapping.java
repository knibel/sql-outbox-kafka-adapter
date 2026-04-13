package de.knibel.outbox.config;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Describes how a group of SQL columns matching regex patterns are
 * collected into a <em>JSON array</em> within the
 * {@link RowMappingStrategy#CUSTOM CUSTOM} row-mapping strategy.
 *
 * <p>Columns whose labels match one of the configured {@link #patterns}
 * are grouped by their <em>first capturing group</em> value.  Each unique
 * group value produces one JSON object (array element).  The
 * {@link FieldMapping#getName() name} of the matching pattern becomes the
 * property name within that element, and the column value becomes its value.
 *
 * <p>If {@link #keyProperty} is set, each array element also includes a
 * property with that name whose value is the captured group string.
 *
 * <h3>Example</h3>
 * <pre>
 * listMappings:
 *   modifications:                # target JSON path
 *     keyProperty: attribute      # $1 → property "attribute"
 *     patterns:
 *       "new_(.*)":
 *         name: after
 *       "old_(.*)":
 *         name: before
 * </pre>
 *
 * Given columns {@code new_price=10.0}, {@code old_price=8.0},
 * {@code new_stock=100}, {@code old_stock=50} this produces:
 * <pre>
 * "modifications": [
 *   {"attribute": "price", "after": 10.0, "before": 8.0},
 *   {"attribute": "stock", "after": 100,  "before": 50}
 * ]
 * </pre>
 *
 * @see OutboxTableProperties#getListMappings()
 */
public class ListMapping {

    /**
     * Optional property name for the captured group key in each list
     * element.  When set, every array element includes a property with
     * this name whose value is the first capturing-group match of the
     * column label (e.g. the field name extracted from the prefix).
     *
     * <p>Example: with {@code keyProperty = "attribute"} and pattern
     * {@code "new_(.*)"} matching column {@code new_price}, the element
     * will contain {@code "attribute": "price"}.
     */
    private String keyProperty;

    /**
     * Regex-pattern-to-property mappings.  Keys are Java regular
     * expressions matched against each column label (full-string match
     * via {@link java.util.regex.Matcher#matches()}).  The pattern
     * <em>must</em> contain at least one capturing group; the first
     * group value is used to correlate columns into the same list
     * element.
     *
     * <p>Values are {@link FieldMapping} objects whose
     * {@link FieldMapping#getName() name} specifies the property name
     * within the list element.  {@code dataType}, {@code format}, and
     * {@code valueMappings} are supported and behave identically to
     * their use in {@code fieldMappings} / {@code columnPatterns}.
     */
    private Map<String, FieldMapping> patterns = new LinkedHashMap<>();

    /** Lazily compiled version of {@link #patterns}; invalidated when the map is replaced. */
    private transient volatile Map<Pattern, FieldMapping> compiledPatterns;

    public ListMapping() {}

    public String getKeyProperty() { return keyProperty; }
    public void setKeyProperty(String keyProperty) { this.keyProperty = keyProperty; }

    public Map<String, FieldMapping> getPatterns() { return patterns; }
    public void setPatterns(Map<String, FieldMapping> patterns) {
        this.patterns = patterns;
        this.compiledPatterns = null; // invalidate cache
    }

    /**
     * Returns the {@link #patterns} compiled as {@link Pattern} objects.
     * Patterns are compiled lazily on the first call and cached for reuse.
     */
    public Map<Pattern, FieldMapping> getCompiledPatterns() {
        Map<Pattern, FieldMapping> cached = compiledPatterns;
        if (cached == null) {
            Map<Pattern, FieldMapping> compiled = new LinkedHashMap<>();
            for (Map.Entry<String, FieldMapping> entry : patterns.entrySet()) {
                compiled.put(Pattern.compile(entry.getKey()), entry.getValue());
            }
            compiledPatterns = cached = Collections.unmodifiableMap(compiled);
        }
        return cached;
    }
}
