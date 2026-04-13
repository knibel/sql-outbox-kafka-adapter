package de.knibel.outbox.config;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Configuration for a single outbox table.
 *
 * <p>After a row is successfully published to Kafka the adapter acknowledges
 * it according to the configured {@link AcknowledgementStrategy}:
 * <ul>
 *   <li>{@link AcknowledgementStrategy#STATUS STATUS} (default) – updates
 *       {@code statusColumn} from {@code pendingValue} to {@code doneValue}.
 *   <li>{@link AcknowledgementStrategy#DELETE DELETE} – deletes the row.
 *   <li>{@link AcknowledgementStrategy#TIMESTAMP TIMESTAMP} – writes
 *       {@code NOW()} into {@code processedAtColumn}.
 * </ul>
 *
 * <p><b>Topic routing</b>
 * <ul>
 *   <li>If {@code topicColumn} is set, the Kafka topic is read from that column
 *       (allows per-row topic routing).
 *   <li>Otherwise {@code staticTopic} is used for all rows from this table.
 * </ul>
 *
 * <p><b>SQL identifier safety</b><br>
 * All column and table names are validated against {@code ^[a-zA-Z_][a-zA-Z0-9_]*$}
 * at startup and double-quoted in generated SQL to prevent injection.
 */
public class OutboxTableProperties {

    // ── Table / polling ──────────────────────────────────────────────────────

    /** Name of the outbox table. Required. */
    private String tableName;

    /** How often to poll for new rows (milliseconds). Default: 1000. */
    private long pollIntervalMs = 1_000;

    /** Maximum number of rows to process in one poll cycle. Default: 100. */
    private int batchSize = 100;

    /**
     * When {@code true}, the poller skips the configured polling delay and
     * immediately starts the next poll cycle whenever the previous cycle
     * returned a full batch (i.e. {@code records.size() == batchSize}).
     *
     * <p>This allows the poller to drain a large backlog of outbox rows
     * as quickly as possible without waiting between cycles.  The delay is
     * only applied once the poller fetches fewer rows than {@code batchSize},
     * which indicates the backlog has been caught up.
     *
     * <p>Default: {@code false} (always apply the configured delay).
     */
    private boolean skipDelayOnFullBatch = false;

    // ── Custom SQL query ────────────────────────────────────────────────────

    /**
     * Optional custom SQL SELECT query used to fetch pending rows.
     *
     * <p>When set, this query is executed as-is (no additional
     * {@code FOR UPDATE SKIP LOCKED} or row-limiting is appended).
     * The query must return all columns required by the configured
     * row-mapping strategy (e.g. {@code idColumn}, {@code keyColumn},
     * {@code payloadColumn}, field-mapping source columns, etc.).
     *
     * <p>When {@code null} (the default), the adapter auto-generates
     * the SELECT statement based on the other configuration properties.
     */
    private String customQuery;

    // ── Column mapping ───────────────────────────────────────────────────────

    /** Primary-key column (also used as Kafka record key when {@code keyColumn} is absent). Required. */
    private String idColumn = "id";

    /**
     * Column containing the Kafka record key.
     * If null, the value of {@code idColumn} is used as the Kafka key.
     */
    private String keyColumn;

    /** Column containing the JSON payload sent as the Kafka record value. Required. */
    private String payloadColumn = "payload";

    /**
     * Column containing a JSON object whose entries become Kafka record headers.
     * Optional – omit or set to null to send no headers.
     */
    private String headersColumn;

    /**
     * Column used to derive the Kafka topic per row.
     * Optional – when null, {@code staticTopic} is used for all rows.
     */
    private String topicColumn;

    /** Static Kafka topic used when {@code topicColumn} is not set. */
    private String staticTopic;

    // ── Row mapping strategy ─────────────────────────────────────────────────

    /**
     * How SQL row columns are mapped to the Kafka record value (payload).
     * Default: {@link RowMappingStrategy#PAYLOAD_COLUMN}.
     *
     * <ul>
     *   <li>{@code PAYLOAD_COLUMN} – reads the pre-serialized JSON payload from
     *       a single configured {@code payloadColumn} (original behaviour).
     *   <li>{@code TO_CAMEL_CASE} – selects all columns and converts every
     *       {@code snake_case} column name to {@code camelCase} in the resulting
     *       JSON payload.
     *   <li>{@code CUSTOM} – uses the explicit {@code fieldMappings} to map
     *       source columns to target JSON paths (supports nested objects via
     *       dot-separated paths).
     * </ul>
     */
    private RowMappingStrategy rowMappingStrategy = RowMappingStrategy.PAYLOAD_COLUMN;

    /**
     * Explicit column-to-JSON-field mappings, used only when
     * {@code rowMappingStrategy} is {@link RowMappingStrategy#CUSTOM}.
     *
     * <p>Keys are source SQL column names; values are {@link FieldMapping}
     * objects specifying the target JSON field path ({@code name}) and an
     * optional data type ({@code dataType}) for type conversion.
     * Dot-separated paths produce nested JSON objects.
     *
     * <p>Example:
     * <pre>
     *   fieldMappings:
     *     order_id:
     *       name: orderId
     *       dataType: STRING
     *     total_amount:
     *       name: totalAmount
     *       dataType: DOUBLE
     *     customer_name:
     *       name: customer.name
     * </pre>
     * Produces: {@code {"orderId":"…","totalAmount":99.95,"customer":{"name":"…"}}}
     */
    private Map<String, FieldMapping> fieldMappings = new LinkedHashMap<>();

    /**
     * Regex-pattern-to-JSON-field mappings, used only when
     * {@code rowMappingStrategy} is {@link RowMappingStrategy#CUSTOM}.
     *
     * <p>Keys are Java regular-expression patterns applied against every
     * column label in the result set using full-string matching
     * (equivalent to {@link java.util.regex.Matcher#matches()}).
     * Values are {@link FieldMapping} objects whose {@code name} may contain
     * back-references ({@code $1}, {@code $2}, …) that are resolved against
     * the capturing groups of the matched column name.
     *
     * <p>Columns already listed in {@code fieldMappings} are excluded from
     * pattern matching so that explicit mappings always take precedence.
     *
     * <p>Example:
     * <pre>
     *   columnPatterns:
     *     "neu_(.*)":
     *       name: "neu.$1"
     *     "alt_(.*)":
     *       name: "alt.$1"
     * </pre>
     * A row containing the columns {@code neu_preis=10.0} and
     * {@code alt_preis=8.0} produces:
     * {@code {"neu":{"preis":10.0},"alt":{"preis":8.0}}}
     */
    private Map<String, FieldMapping> columnPatterns = new LinkedHashMap<>();

    /**
     * Static key-value pairs injected into every JSON payload.
     *
     * <p>Used together with {@link RowMappingStrategy#CUSTOM CUSTOM} or
     * {@link RowMappingStrategy#TO_CAMEL_CASE TO_CAMEL_CASE} to add
     * constant values that do not come from a database column (e.g. a
     * type discriminator).
     *
     * <p>Keys are target JSON field paths (dot-separated for nesting,
     * just like {@link FieldMapping#getName()}).  Values are the constant
     * string values written to the payload.  Static fields are applied
     * <em>after</em> column-based mappings, so they can override a
     * column-derived value if the same path is used.
     *
     * <p>Ignored when {@code rowMappingStrategy} is
     * {@link RowMappingStrategy#PAYLOAD_COLUMN PAYLOAD_COLUMN}.
     *
     * <p>Example:
     * <pre>
     *   staticFields:
     *     ereignistyp: "BatchlaufStatus.BatchlaufinfoBereitgestellt"
     *     meta.source: "outbox-adapter"
     * </pre>
     * Produces: {@code {"ereignistyp":"BatchlaufStatus.BatchlaufinfoBereitgestellt","meta":{"source":"outbox-adapter"}}}
     */
    private Map<String, String> staticFields = new LinkedHashMap<>();

    // ── Acknowledgement strategy ─────────────────────────────────────────────

    /**
     * How to acknowledge a row after it has been successfully published.
     * Default: {@link AcknowledgementStrategy#STATUS}.
     */
    private AcknowledgementStrategy acknowledgementStrategy = AcknowledgementStrategy.STATUS;

    // ── Status strategy settings ─────────────────────────────────────────────
    // Used only when acknowledgementStrategy = STATUS.

    /** Column that tracks processing status. Required for STATUS strategy. */
    private String statusColumn = "status";

    /**
     * Value in {@code statusColumn} that marks a row as not yet processed.
     * Default: {@code "PENDING"}.  Used only for STATUS strategy.
     */
    private String pendingValue = "PENDING";

    /**
     * Value in {@code statusColumn} that marks a row as successfully processed.
     * Default: {@code "DONE"}.  Used only for STATUS strategy.
     */
    private String doneValue = "DONE";

    // ── Timestamp strategy settings ───────────────────────────────────────────
    // Used only when acknowledgementStrategy = TIMESTAMP.

    /**
     * Column into which {@code NOW()} is written after a row has been
     * successfully published.  Rows with a {@code NULL} value are treated as
     * pending.  Required for TIMESTAMP strategy.
     */
    private String processedAtColumn;

    // ── Custom acknowledgement query ─────────────────────────────────────────

    /**
     * Optional custom SQL statement executed to acknowledge each row after
     * successful Kafka publishing.
     *
     * <p>Used only when {@code acknowledgementStrategy} is
     * {@link AcknowledgementStrategy#CUSTOM CUSTOM}.  The statement is
     * executed once per processed row with a single bind parameter
     * ({@code ?}) that receives the row's {@code idColumn} value.
     *
     * <p>Example:
     * <pre>
     * customAcknowledgementQuery: >
     *   UPDATE OUTBOX
     *   SET DATEN_ABGEHOLT_JN = 1, DATEN_ABHOLUNG_DAT = CURRENT_TIMESTAMP
     *   WHERE STICHTAG_DAT = ?
     * </pre>
     *
     * <p><b>Note:</b> This query receives the row's {@code idColumn} value
     * as the sole bind parameter.  If your acknowledgement needs multiple
     * columns, consider using a single synthetic key or embedding the logic
     * in the SQL (e.g. a stored procedure call).
     */
    private String customAcknowledgementQuery;

    // ── Transient DB error tolerance ─────────────────────────────────────────

    /**
     * Minimum idle time in milliseconds (time A) – i.e. the duration since
     * the last poll cycle that actually returned records – before transient
     * DB errors are silently suppressed.
     *
     * <p>A value of {@code 0} (the default) disables silent suppression:
     * every transient DB error is always logged.
     */
    private long transientDbErrorSilenceAfterIdleMs = 0;

    /**
     * Duration in milliseconds (time B) for which transient DB errors are
     * silently ignored once the idle threshold
     * ({@link #transientDbErrorSilenceAfterIdleMs}) has been exceeded.
     *
     * <p>A value of {@code 0} (the default) disables silent suppression.
     * After this window has elapsed the errors are logged again.
     */
    private long transientDbErrorSilenceDurationMs = 0;

    // ── Getters / setters ─────────────────────────────────────────────────────

    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }

    public long getPollIntervalMs() { return pollIntervalMs; }
    public void setPollIntervalMs(long pollIntervalMs) { this.pollIntervalMs = pollIntervalMs; }

    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }

    public boolean isSkipDelayOnFullBatch() { return skipDelayOnFullBatch; }
    public void setSkipDelayOnFullBatch(boolean skipDelayOnFullBatch) { this.skipDelayOnFullBatch = skipDelayOnFullBatch; }

    public String getIdColumn() { return idColumn; }
    public void setIdColumn(String idColumn) { this.idColumn = idColumn; }

    public String getKeyColumn() { return keyColumn; }
    public void setKeyColumn(String keyColumn) { this.keyColumn = keyColumn; }

    public String getPayloadColumn() { return payloadColumn; }
    public void setPayloadColumn(String payloadColumn) { this.payloadColumn = payloadColumn; }

    public String getHeadersColumn() { return headersColumn; }
    public void setHeadersColumn(String headersColumn) { this.headersColumn = headersColumn; }

    public String getTopicColumn() { return topicColumn; }
    public void setTopicColumn(String topicColumn) { this.topicColumn = topicColumn; }

    public String getStaticTopic() { return staticTopic; }
    public void setStaticTopic(String staticTopic) { this.staticTopic = staticTopic; }

    public String getStatusColumn() { return statusColumn; }
    public void setStatusColumn(String statusColumn) { this.statusColumn = statusColumn; }

    public String getPendingValue() { return pendingValue; }
    public void setPendingValue(String pendingValue) { this.pendingValue = pendingValue; }

    public String getDoneValue() { return doneValue; }
    public void setDoneValue(String doneValue) { this.doneValue = doneValue; }

    public AcknowledgementStrategy getAcknowledgementStrategy() { return acknowledgementStrategy; }
    public void setAcknowledgementStrategy(AcknowledgementStrategy acknowledgementStrategy) { this.acknowledgementStrategy = acknowledgementStrategy; }

    public RowMappingStrategy getRowMappingStrategy() { return rowMappingStrategy; }
    public void setRowMappingStrategy(RowMappingStrategy rowMappingStrategy) { this.rowMappingStrategy = rowMappingStrategy; }

    public Map<String, FieldMapping> getFieldMappings() { return fieldMappings; }
    public void setFieldMappings(Map<String, FieldMapping> fieldMappings) { this.fieldMappings = fieldMappings; }

    public Map<String, FieldMapping> getColumnPatterns() { return columnPatterns; }
    public void setColumnPatterns(Map<String, FieldMapping> columnPatterns) { this.columnPatterns = columnPatterns; }

    public String getProcessedAtColumn() { return processedAtColumn; }
    public void setProcessedAtColumn(String processedAtColumn) { this.processedAtColumn = processedAtColumn; }

    public long getTransientDbErrorSilenceAfterIdleMs() { return transientDbErrorSilenceAfterIdleMs; }
    public void setTransientDbErrorSilenceAfterIdleMs(long transientDbErrorSilenceAfterIdleMs) { this.transientDbErrorSilenceAfterIdleMs = transientDbErrorSilenceAfterIdleMs; }

    public long getTransientDbErrorSilenceDurationMs() { return transientDbErrorSilenceDurationMs; }
    public void setTransientDbErrorSilenceDurationMs(long transientDbErrorSilenceDurationMs) { this.transientDbErrorSilenceDurationMs = transientDbErrorSilenceDurationMs; }

    public String getCustomQuery() { return customQuery; }
    public void setCustomQuery(String customQuery) { this.customQuery = customQuery; }

    public Map<String, String> getStaticFields() { return staticFields; }
    public void setStaticFields(Map<String, String> staticFields) { this.staticFields = staticFields; }

    public String getCustomAcknowledgementQuery() { return customAcknowledgementQuery; }
    public void setCustomAcknowledgementQuery(String customAcknowledgementQuery) { this.customAcknowledgementQuery = customAcknowledgementQuery; }
}
