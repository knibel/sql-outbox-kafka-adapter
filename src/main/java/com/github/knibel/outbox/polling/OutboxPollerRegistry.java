package com.github.knibel.outbox.polling;

import com.github.knibel.outbox.config.OutboxProperties;
import com.github.knibel.outbox.config.OutboxTableProperties;
import com.github.knibel.outbox.jdbc.OutboxRepository;
import com.github.knibel.outbox.jdbc.SqlIdentifier;
import com.github.knibel.outbox.kafka.OutboxKafkaProducer;
import com.github.knibel.outbox.strategy.DeleteAfterPublishStrategy;
import com.github.knibel.outbox.strategy.MarkDoneStrategy;
import com.github.knibel.outbox.strategy.PostPublishStrategy;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
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
 * thread loops: poll → sleep → repeat until interrupted.
 *
 * <p>On {@link #stop()}, all virtual threads are interrupted and the registry
 * waits up to 10 seconds for them to finish before returning.
 *
 * <p><b>Error handling:</b> if any error occurs during a poll cycle the
 * application is shut down immediately.  Continuing after an error is not
 * permitted because it would silently skip records and break message ordering.
 */
@Component
public class OutboxPollerRegistry implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(OutboxPollerRegistry.class);
    private static final long SHUTDOWN_WAIT_MS = 10_000;

    private final OutboxProperties outboxProperties;
    private final OutboxRepository repository;
    private final OutboxKafkaProducer kafkaProducer;
    private final MeterRegistry meterRegistry;
    private final ApplicationContext applicationContext;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final List<Thread> pollerThreads = new ArrayList<>();

    public OutboxPollerRegistry(OutboxProperties outboxProperties,
                                OutboxRepository repository,
                                OutboxKafkaProducer kafkaProducer,
                                MeterRegistry meterRegistry,
                                ApplicationContext applicationContext) {
        this.outboxProperties = outboxProperties;
        this.repository = repository;
        this.kafkaProducer = kafkaProducer;
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
            PostPublishStrategy strategy = resolveStrategy(tableConfig);
            OutboxPoller poller = new OutboxPoller(tableConfig, repository, kafkaProducer, strategy, meterRegistry);
            Thread thread = Thread.ofVirtual()
                    .name("outbox-poller-" + tableConfig.getTableName())
                    .start(() -> runPollerLoop(poller, tableConfig));
            pollerThreads.add(thread);
            log.info("Started outbox poller for table '{}' (interval={}ms, batch={}, strategy={})",
                    tableConfig.getTableName(),
                    tableConfig.getPollIntervalMs(),
                    tableConfig.getBatchSize(),
                    tableConfig.getProcessingStrategy());
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

    private PostPublishStrategy resolveStrategy(OutboxTableProperties config) {
        return switch (config.getProcessingStrategy()) {
            case DELETE -> new DeleteAfterPublishStrategy();
            case MARK_DONE -> new MarkDoneStrategy();
        };
    }

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
     * Validates all required fields and SQL identifiers in each table config.
     * Fails fast at startup rather than encountering issues later at runtime.
     */
    private void validateConfigs(List<OutboxTableProperties> tables) {
        for (OutboxTableProperties cfg : tables) {
            String name = cfg.getTableName();
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("outbox table config is missing 'tableName'");
            }
            SqlIdentifier.quote(cfg.getTableName());
            SqlIdentifier.quote(cfg.getIdColumn());
            SqlIdentifier.quote(cfg.getStatusColumn());
            SqlIdentifier.quote(cfg.getPayloadColumn());
            if (cfg.getKeyColumn() != null)     SqlIdentifier.quote(cfg.getKeyColumn());
            if (cfg.getTopicColumn() != null)   SqlIdentifier.quote(cfg.getTopicColumn());
            if (cfg.getHeadersColumn() != null) SqlIdentifier.quote(cfg.getHeadersColumn());

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
