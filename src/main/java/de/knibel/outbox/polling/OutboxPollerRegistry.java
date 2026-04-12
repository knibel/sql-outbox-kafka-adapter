package de.knibel.outbox.polling;

import de.knibel.outbox.config.AcknowledgementStrategy;
import de.knibel.outbox.config.FieldDataType;
import de.knibel.outbox.config.FieldMapping;
import de.knibel.outbox.config.OutboxProperties;
import de.knibel.outbox.config.OutboxTableProperties;
import de.knibel.outbox.config.RowMappingStrategy;
import de.knibel.outbox.jdbc.SqlIdentifier;
import de.knibel.outbox.repository.OutboxRepository;
import de.knibel.outbox.transport.MessageSender;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;

/**
 * Creates and manages one {@link OutboxPoller} per configured outbox table.
 *
 * <p>Implements {@link SmartLifecycle} so that Spring Boot's graceful shutdown
 * mechanism can stop all pollers cleanly.  Each poller runs in its own
 * <em>virtual thread</em> (Java 21+) so JDBC blocking calls never hold up
 * platform threads.
 *
 * <p>On {@link #start()}, one virtual thread per table is launched.  Each
 * thread loops: poll → sleep → repeat until interrupted.
 *
 * <p>On {@link #stop()}, all virtual threads are interrupted and the registry
 * waits up to 10 seconds for them to finish before returning.
 *
 * <p><b>Error handling:</b> {@link DataAccessException} thrown during a poll
 * cycle is treated as a transient DB problem – the error is logged and the
 * poller continues with the next scheduled cycle.  If the poller has been idle
 * (no records read) for at least
 * {@link OutboxTableProperties#getTransientDbErrorSilenceAfterIdleMs()} (time A)
 * the error log is suppressed for up to
 * {@link OutboxTableProperties#getTransientDbErrorSilenceDurationMs()} (time B)
 * to avoid noisy logs during quiet periods.  Any other exception is still
 * treated as fatal and shuts the application down.
 */
@Component
public class OutboxPollerRegistry implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(OutboxPollerRegistry.class);
    private static final long SHUTDOWN_WAIT_MS = 10_000;

    private final OutboxProperties outboxProperties;
    private final OutboxRepository repository;
    private final MessageSender messageSender;
    private final MeterRegistry meterRegistry;
    private final ApplicationContext applicationContext;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final List<Thread> pollerThreads = new ArrayList<>();

    public OutboxPollerRegistry(OutboxProperties outboxProperties,
                                OutboxRepository repository,
                                MessageSender messageSender,
                                MeterRegistry meterRegistry,
                                ApplicationContext applicationContext) {
        this.outboxProperties = outboxProperties;
        this.repository = repository;
        this.messageSender = messageSender;
        this.meterRegistry = meterRegistry;
        this.applicationContext = applicationContext;
    }

    // ── SmartLifecycle ───────────────────────────────────────────────────────

    @Override
    public void start() {
        List<OutboxTableProperties> tables = outboxProperties.getTables();
        if (tables.isEmpty()) {
            log.warn("No outbox tables configured – no pollers will start");
            running.set(true);
            return;
        }

        validateConfigs(tables);

        for (OutboxTableProperties tableConfig : tables) {
            OutboxPoller poller = new OutboxPoller(tableConfig, repository, messageSender, meterRegistry);
            Thread thread = Thread.ofVirtual()
                    .name("outbox-poller-" + tableConfig.getTableName())
                    .start(() -> runPollerLoop(poller, tableConfig));
            pollerThreads.add(thread);
            log.info("Started outbox poller for table '{}' (interval={}ms, batch={}, skipDelayOnFullBatch={})",
                    tableConfig.getTableName(),
                    tableConfig.getPollIntervalMs(),
                    tableConfig.getBatchSize(),
                    tableConfig.isSkipDelayOnFullBatch());
        }
        running.set(true);
    }

    @Override
    public void stop() {
        log.info("Stopping {} outbox poller(s)…", pollerThreads.size());
        pollerThreads.forEach(Thread::interrupt);

        for (Thread t : pollerThreads) {
            try {
                t.join(SHUTDOWN_WAIT_MS);
                if (t.isAlive()) {
                    log.warn("Poller thread '{}' did not stop within {}ms",
                            t.getName(), SHUTDOWN_WAIT_MS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        pollerThreads.clear();
        running.set(false);
        log.info("All outbox pollers stopped");
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    /** Higher phase value = starts last in startup order; stops first in shutdown order.
     *  Using {@code MAX_VALUE} ensures pollers start after all infrastructure beans
     *  (DataSource, KafkaProducer) are ready, and are stopped before them during
     *  graceful shutdown so in-flight batches can complete cleanly. */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    private void runPollerLoop(OutboxPoller poller, OutboxTableProperties config) {
        log.debug("Poller loop started for table '{}'", config.getTableName());
        long pollerStartTimeMs = System.currentTimeMillis();
        long lastRecordReadTimeMs = 0L;      // 0 = no records read yet
        Long transientErrorFirstSeenMs = null;

        while (!Thread.currentThread().isInterrupted()) {
            try {
                int processed = poller.poll();
                if (processed > 0) {
                    lastRecordReadTimeMs = System.currentTimeMillis();
                }
                transientErrorFirstSeenMs = null; // reset whenever poll() completes without throwing
                boolean fullBatch = processed == config.getBatchSize();
                if (config.isSkipDelayOnFullBatch() && fullBatch) {
                    log.debug("Full batch of {} processed for table '{}' – skipping delay",
                            processed, config.getTableName());
                } else {
                    Thread.sleep(config.getPollIntervalMs());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (DataAccessException e) {
                // Transient DB error: log (unless silenced) and retry on next schedule.
                long now = System.currentTimeMillis();
                if (transientErrorFirstSeenMs == null) {
                    transientErrorFirstSeenMs = now;
                }
                if (!shouldSuppressTransientError(config, pollerStartTimeMs,
                        lastRecordReadTimeMs, transientErrorFirstSeenMs, now)) {
                    log.error("Transient DB error in outbox poller for table '{}', will retry: {}",
                            config.getTableName(), e.getMessage(), e);
                }
                try {
                    Thread.sleep(config.getPollIntervalMs());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            } catch (Exception e) {
                log.error("Fatal error in outbox poller for table '{}' – shutting down application: {}",
                        config.getTableName(), e.getMessage(), e);
                // Shut the whole application down to avoid skipping records and
                // breaking message ordering. The exit code is 1 to signal an error.
                SpringApplication.exit(applicationContext, () -> 1);
                return;
            }
        }
        log.debug("Poller loop stopped for table '{}'", config.getTableName());
    }

    /**
     * Returns {@code true} when the transient-error log should be suppressed.
     *
     * <p>Suppression is active when <em>all</em> of the following hold:
     * <ol>
     *   <li>Both A and B are configured (both > 0).
     *   <li>The poller has been idle (no records read) for at least A milliseconds.
     *   <li>The current run of transient errors started no more than B milliseconds ago.
     * </ol>
     */
    static boolean shouldSuppressTransientError(OutboxTableProperties config,
                                                long pollerStartTimeMs,
                                                long lastRecordReadTimeMs,
                                                long transientErrorFirstSeenMs,
                                                long nowMs) {
        long silenceAfterIdleMs = config.getTransientDbErrorSilenceAfterIdleMs();
        long silenceDurationMs  = config.getTransientDbErrorSilenceDurationMs();

        if (silenceAfterIdleMs <= 0 || silenceDurationMs <= 0) {
            return false; // suppression disabled
        }

        // Use the later of pollerStartTimeMs and lastRecordReadTimeMs as the
        // idle baseline so "never read any records" is tracked from startup.
        long idleBaseline = lastRecordReadTimeMs > 0 ? lastRecordReadTimeMs : pollerStartTimeMs;
        long idleDurationMs = nowMs - idleBaseline;

        if (idleDurationMs < silenceAfterIdleMs) {
            return false; // not idle long enough to trigger suppression
        }

        // Still within the silence window B?
        long errorDurationMs = nowMs - transientErrorFirstSeenMs;
        return errorDurationMs <= silenceDurationMs;
    }

    /**
     * Validates all required fields and SQL identifiers in each table config.
     * Fails fast at startup rather than encountering issues later at runtime.
     */
    private void validateConfigs(List<OutboxTableProperties> tables) {
        for (OutboxTableProperties cfg : tables) {
            String name = cfg.getTableName();
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("outbox table config is missing 'tableName'");
            }

            boolean hasCustomQuery = cfg.getCustomQuery() != null && !cfg.getCustomQuery().isBlank();

            // When no custom query is used, all identifiers must be safe SQL identifiers
            if (!hasCustomQuery) {
                SqlIdentifier.quote(cfg.getTableName());
                if (cfg.getAcknowledgementStrategy() == AcknowledgementStrategy.STATUS) {
                    SqlIdentifier.quote(cfg.getStatusColumn());
                }
                if (cfg.getKeyColumn() != null)     SqlIdentifier.quote(cfg.getKeyColumn());
                if (cfg.getTopicColumn() != null)   SqlIdentifier.quote(cfg.getTopicColumn());
                if (cfg.getHeadersColumn() != null) SqlIdentifier.quote(cfg.getHeadersColumn());
            }

            // idColumn is always validated because it's used for Kafka key fallback and ack
            SqlIdentifier.quote(cfg.getIdColumn());

            // Validate payloadColumn only when the PAYLOAD_COLUMN strategy is used
            RowMappingStrategy rowMapping = cfg.getRowMappingStrategy();
            if (rowMapping == RowMappingStrategy.PAYLOAD_COLUMN && !hasCustomQuery) {
                SqlIdentifier.quote(cfg.getPayloadColumn());
            }

            // Validate CUSTOM strategy: fieldMappings must not be empty and
            // all source column names must be safe SQL identifiers
            if (rowMapping == RowMappingStrategy.CUSTOM) {
                if (cfg.getFieldMappings() == null || cfg.getFieldMappings().isEmpty()) {
                    throw new IllegalArgumentException(
                            "Table '" + name + "': rowMappingStrategy=CUSTOM requires non-empty 'fieldMappings'");
                }
                for (var entry : cfg.getFieldMappings().entrySet()) {
                    if (!hasCustomQuery) {
                        SqlIdentifier.quote(entry.getKey());
                    }
                    FieldMapping mapping = entry.getValue();
                    if (mapping == null || mapping.getName() == null || mapping.getName().isBlank()) {
                        throw new IllegalArgumentException(
                                "Table '" + name + "': fieldMappings entry for column '"
                                + entry.getKey() + "' must have a non-blank 'name'");
                    }
                    // DATE and DATETIME require a format pattern
                    FieldDataType dt = mapping.getDataType();
                    if ((dt == FieldDataType.DATE || dt == FieldDataType.DATETIME)
                            && (mapping.getFormat() == null || mapping.getFormat().isBlank())) {
                        throw new IllegalArgumentException(
                                "Table '" + name + "': fieldMappings entry for column '"
                                + entry.getKey() + "' with dataType=" + dt
                                + " requires a non-blank 'format' pattern");
                    }
                }
            }

            // Validate staticFields: paths must be non-blank
            if (cfg.getStaticFields() != null) {
                for (var entry : cfg.getStaticFields().entrySet()) {
                    if (entry.getKey() == null || entry.getKey().isBlank()) {
                        throw new IllegalArgumentException(
                                "Table '" + name + "': staticFields key must not be blank");
                    }
                }
            }

            // Validate CUSTOM acknowledgement strategy
            if (cfg.getAcknowledgementStrategy() == AcknowledgementStrategy.CUSTOM) {
                if (cfg.getCustomAcknowledgementQuery() == null || cfg.getCustomAcknowledgementQuery().isBlank()) {
                    throw new IllegalArgumentException(
                            "Table '" + name + "': acknowledgementStrategy=CUSTOM requires non-blank 'customAcknowledgementQuery'");
                }
            }

            if (cfg.getTopicColumn() == null
                    && (cfg.getStaticTopic() == null || cfg.getStaticTopic().isBlank())) {
                throw new IllegalArgumentException(
                        "Table '" + name + "': either 'topicColumn' or 'staticTopic' must be configured");
            }
            if (cfg.getPollIntervalMs() <= 0) {
                throw new IllegalArgumentException(
                        "Table '" + name + "': 'pollIntervalMs' must be > 0");
            }
            if (cfg.getBatchSize() <= 0) {
                throw new IllegalArgumentException(
                        "Table '" + name + "': 'batchSize' must be > 0");
            }
            if (cfg.getTransientDbErrorSilenceAfterIdleMs() < 0) {
                throw new IllegalArgumentException(
                        "Table '" + name + "': 'transientDbErrorSilenceAfterIdleMs' must be >= 0");
            }
            if (cfg.getTransientDbErrorSilenceDurationMs() < 0) {
                throw new IllegalArgumentException(
                        "Table '" + name + "': 'transientDbErrorSilenceDurationMs' must be >= 0");
            }
        }
    }
}
