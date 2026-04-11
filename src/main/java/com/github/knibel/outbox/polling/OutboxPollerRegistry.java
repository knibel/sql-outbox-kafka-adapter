package com.github.knibel.outbox.polling;

import com.github.knibel.outbox.config.OutboxProperties;
import com.github.knibel.outbox.config.OutboxTableProperties;
import com.github.knibel.outbox.jdbc.OutboxRepository;
import com.github.knibel.outbox.jdbc.SqlIdentifier;
import com.github.knibel.outbox.kafka.OutboxKafkaProducer;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
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
 * thread loops:
 * <ol>
 *   <li>Call {@link OutboxPoller#poll()}.
 *   <li>Sleep for {@link OutboxTableProperties#getPollIntervalMs()} ms.
 *   <li>Repeat until interrupted.
 * </ol>
 *
 * <p>On {@link #stop()}, all virtual threads are interrupted and the registry
 * waits up to 10 seconds for them to finish their current poll cycle before
 * returning.
 */
@Component
public class OutboxPollerRegistry implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(OutboxPollerRegistry.class);
    private static final long SHUTDOWN_WAIT_MS = 10_000;

    private final OutboxProperties outboxProperties;
    private final OutboxRepository repository;
    private final OutboxKafkaProducer kafkaProducer;
    private final MeterRegistry meterRegistry;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final List<Thread> pollerThreads = new ArrayList<>();

    public OutboxPollerRegistry(OutboxProperties outboxProperties,
                                OutboxRepository repository,
                                OutboxKafkaProducer kafkaProducer,
                                MeterRegistry meterRegistry) {
        this.outboxProperties = outboxProperties;
        this.repository = repository;
        this.kafkaProducer = kafkaProducer;
        this.meterRegistry = meterRegistry;
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
            OutboxPoller poller = new OutboxPoller(tableConfig, repository, kafkaProducer, meterRegistry);
            Thread thread = Thread.ofVirtual()
                    .name("outbox-poller-" + tableConfig.getTableName())
                    .start(() -> runPollerLoop(poller, tableConfig));
            pollerThreads.add(thread);
            log.info("Started outbox poller for table '{}' (interval={}ms, batch={})",
                    tableConfig.getTableName(),
                    tableConfig.getPollIntervalMs(),
                    tableConfig.getBatchSize());
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
        while (!Thread.currentThread().isInterrupted()) {
            try {
                poller.poll();
                Thread.sleep(config.getPollIntervalMs());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                // poll() already logs individual errors; catch any unexpected ones here
                log.error("Unexpected error in poller loop for table '{}': {}",
                        config.getTableName(), e.getMessage(), e);
                // Brief back-off to avoid tight error loops
                try {
                    Thread.sleep(config.getPollIntervalMs());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        log.debug("Poller loop stopped for table '{}'", config.getTableName());
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
            // Validate all identifier fields – SqlIdentifier.quote() throws on invalid input
            SqlIdentifier.quote(cfg.getTableName());
            SqlIdentifier.quote(cfg.getIdColumn());
            SqlIdentifier.quote(cfg.getStatusColumn());
            SqlIdentifier.quote(cfg.getPayloadColumn());
            if (cfg.getKeyColumn() != null)      SqlIdentifier.quote(cfg.getKeyColumn());
            if (cfg.getTopicColumn() != null)    SqlIdentifier.quote(cfg.getTopicColumn());
            if (cfg.getHeadersColumn() != null)  SqlIdentifier.quote(cfg.getHeadersColumn());
            if (cfg.getUpdatedAtColumn() != null) SqlIdentifier.quote(cfg.getUpdatedAtColumn());

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
        }
    }
}
