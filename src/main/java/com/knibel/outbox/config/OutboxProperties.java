package com.knibel.outbox.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for the SQL outbox Kafka adapter.
 *
 * <p>All properties are prefixed with {@code outbox} in {@code application.properties} /
 * {@code application.yml}.
 *
 * <p>Example configuration:
 * <pre>{@code
 * outbox:
 *   polling-interval-ms: 1000
 *   batch-size: 50
 *   processing-strategy: delete
 * }</pre>
 */
@ConfigurationProperties(prefix = "outbox")
public class OutboxProperties {

    /**
     * How often (in milliseconds) the poller checks for new outbox messages.
     * Defaults to 1000 ms.
     */
    private long pollingIntervalMs = 1_000;

    /**
     * Maximum number of outbox messages fetched and published per polling cycle.
     * Defaults to 100.
     */
    private int batchSize = 100;

    /**
     * Strategy to apply after a message is successfully published to Kafka.
     * Accepted values: {@code delete} (default) or {@code mark-as-processed}.
     */
    private ProcessingStrategy processingStrategy = ProcessingStrategy.DELETE;

    public long getPollingIntervalMs() {
        return pollingIntervalMs;
    }

    public void setPollingIntervalMs(long pollingIntervalMs) {
        this.pollingIntervalMs = pollingIntervalMs;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public ProcessingStrategy getProcessingStrategy() {
        return processingStrategy;
    }

    public void setProcessingStrategy(ProcessingStrategy processingStrategy) {
        this.processingStrategy = processingStrategy;
    }

    /** Available post-publish processing strategies. */
    public enum ProcessingStrategy {
        /** Delete the outbox row immediately after successful Kafka publication. */
        DELETE,
        /** Mark the outbox row as processed (retains the row for auditing). */
        MARK_AS_PROCESSED
    }
}
