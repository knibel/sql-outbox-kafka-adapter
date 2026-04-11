package com.github.knibel.outbox.config;

/**
 * Configuration for a single outbox table.
 *
 * <p>Each row transitions from {@code pendingValue} to {@code doneValue} in
 * the {@code statusColumn} (when using the {@code MARK_DONE} strategy), or is
 * deleted outright (when using the {@code DELETE} strategy).  If any error
 * occurs during publishing the application shuts down to preserve record
 * ordering.
 *
 * <p><b>Topic routing</b>
 * <ul>
 *   <li>If {@code topicColumn} is set, the Kafka topic is read from that column
 *       (allows per-row topic routing).
 *   <li>Otherwise {@code staticTopic} is used for all rows from this table.
 * </ul>
 *
 * <p><b>Post-publish strategy</b>
 * <ul>
 *   <li>{@link ProcessingStrategy#MARK_DONE} (default) – updates
 *       {@code statusColumn} to {@code doneValue}; rows are kept for auditing.
 *   <li>{@link ProcessingStrategy#DELETE} – removes the row immediately after
 *       Kafka acknowledgement; keeps the table small without a cleanup job.
 * </ul>
 *
 * <p><b>SQL identifier safety</b><br>
 * All column and table names are validated against {@code ^[a-zA-Z_][a-zA-Z0-9_]*$}
 * at startup and double-quoted in generated SQL to prevent injection.
 */
public class OutboxTableProperties {

    /** Available post-publish processing strategies. */
    public enum ProcessingStrategy {
        /**
         * Update the status column to {@code doneValue} after successful
         * Kafka delivery. Rows are retained for auditing / replay.
         */
        MARK_DONE,

        /**
         * Delete the row immediately after successful Kafka delivery.
         * Keeps the outbox table lean without a separate cleanup job.
         */
        DELETE
    }

    // ── Table / polling ──────────────────────────────────────────────────────

    /** Name of the outbox table. Required. */
    private String tableName;

    /** How often to poll for new rows (milliseconds). Default: 1000. */
    private long pollIntervalMs = 1_000;

    /** Maximum number of rows to process in one poll cycle. Default: 100. */
    private int batchSize = 100;

    /**
     * What to do with a row after it has been successfully published to Kafka.
     * Default: {@link ProcessingStrategy#MARK_DONE}.
     */
    private ProcessingStrategy processingStrategy = ProcessingStrategy.MARK_DONE;

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

    // ── Status / marking ─────────────────────────────────────────────────────

    /** Column that tracks processing status. Required. */
    private String statusColumn = "status";

    /**
     * Value in {@code statusColumn} that marks a row as not yet processed.
     * Default: {@code "PENDING"}.
     */
    private String pendingValue = "PENDING";

    /**
     * Value in {@code statusColumn} that marks a row as successfully processed.
     * Default: {@code "DONE"}.
     */
    private String doneValue = "DONE";

    // ── Getters / setters ─────────────────────────────────────────────────────

    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }

    public long getPollIntervalMs() { return pollIntervalMs; }
    public void setPollIntervalMs(long pollIntervalMs) { this.pollIntervalMs = pollIntervalMs; }

    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }

    public ProcessingStrategy getProcessingStrategy() { return processingStrategy; }
    public void setProcessingStrategy(ProcessingStrategy processingStrategy) { this.processingStrategy = processingStrategy; }

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
}
