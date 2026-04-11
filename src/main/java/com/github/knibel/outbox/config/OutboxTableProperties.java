package com.github.knibel.outbox.config;

import com.github.knibel.outbox.domain.StatusStrategyType;

/**
 * Configuration for a single outbox table.
 *
 * <p><b>Status strategies</b>
 * <ul>
 *   <li>{@code ENUM} – uses string values for PENDING / IN_PROGRESS / DONE.
 *       Provides effectively-once semantics: rows are claimed atomically to
 *       IN_PROGRESS before Kafka publishing; stuck-row recovery resets them.
 *   <li>{@code TIMESTAMP} – a nullable timestamp column: {@code NULL} means
 *       pending, {@code CURRENT_TIMESTAMP} means done. At-least-once semantics
 *       (no in-progress state; relies on {@code FOR UPDATE SKIP LOCKED}).
 *   <li>{@code BOOLEAN} – a boolean column: {@code false} means pending,
 *       {@code true} means done. At-least-once semantics.
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

    // ── Column mapping ───────────────────────────────────────────────────────

    /** Primary-key column (also used as Kafka idempotency key base). Required. */
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
     * Strategy used to interpret / update the status column.
     * Default: {@code ENUM}.
     */
    private StatusStrategyType statusStrategy = StatusStrategyType.ENUM;

    /**
     * Value (or SQL expression) representing a row that has not yet been
     * processed. Used by {@code ENUM} strategy. Default: {@code "PENDING"}.
     */
    private String pendingValue = "PENDING";

    /**
     * Value representing a row currently being processed.
     * Used by {@code ENUM} strategy only. Default: {@code "IN_PROGRESS"}.
     */
    private String inProgressValue = "IN_PROGRESS";

    /**
     * Value representing a successfully processed row.
     * Used by {@code ENUM} strategy only. Default: {@code "DONE"}.
     */
    private String doneValue = "DONE";

    /**
     * Value representing a permanently failed row.
     * Used by {@code ENUM} strategy only. Default: {@code "FAILED"}.
     */
    private String failedValue = "FAILED";

    // ── Stuck-row recovery (ENUM strategy only) ───────────────────────────────

    /**
     * Column used to detect stuck rows (rows that were claimed to IN_PROGRESS
     * but never completed, typically due to a JVM crash).
     * Must be a timestamp column that is updated whenever the row is modified.
     * If null, stuck-row detection is disabled for this table.
     */
    private String updatedAtColumn;

    /**
     * Number of seconds after which an IN_PROGRESS row is considered stuck
     * and reset to PENDING. Default: 300 (5 minutes).
     */
    private long stuckTtlSeconds = 300;

    // ── Getters / setters ─────────────────────────────────────────────────────

    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }

    public long getPollIntervalMs() { return pollIntervalMs; }
    public void setPollIntervalMs(long pollIntervalMs) { this.pollIntervalMs = pollIntervalMs; }

    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }

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

    public StatusStrategyType getStatusStrategy() { return statusStrategy; }
    public void setStatusStrategy(StatusStrategyType statusStrategy) { this.statusStrategy = statusStrategy; }

    public String getPendingValue() { return pendingValue; }
    public void setPendingValue(String pendingValue) { this.pendingValue = pendingValue; }

    public String getInProgressValue() { return inProgressValue; }
    public void setInProgressValue(String inProgressValue) { this.inProgressValue = inProgressValue; }

    public String getDoneValue() { return doneValue; }
    public void setDoneValue(String doneValue) { this.doneValue = doneValue; }

    public String getFailedValue() { return failedValue; }
    public void setFailedValue(String failedValue) { this.failedValue = failedValue; }

    public String getUpdatedAtColumn() { return updatedAtColumn; }
    public void setUpdatedAtColumn(String updatedAtColumn) { this.updatedAtColumn = updatedAtColumn; }

    public long getStuckTtlSeconds() { return stuckTtlSeconds; }
    public void setStuckTtlSeconds(long stuckTtlSeconds) { this.stuckTtlSeconds = stuckTtlSeconds; }
}
