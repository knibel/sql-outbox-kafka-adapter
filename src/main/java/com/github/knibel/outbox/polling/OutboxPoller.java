package com.github.knibel.outbox.polling;

import com.github.knibel.outbox.config.OutboxTableProperties;
import com.github.knibel.outbox.domain.OutboxRecord;
import com.github.knibel.outbox.jdbc.OutboxRepository;
import com.github.knibel.outbox.kafka.OutboxKafkaProducer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes one poll cycle for a single outbox table.
 *
 * <p>The cycle follows the two-phase commit pattern:
 * <ol>
 *   <li><b>Reset stuck rows</b> – rows that were claimed to IN_PROGRESS but
 *       never completed (JVM crash) are reset to PENDING (ENUM strategy only).
 *   <li><b>Claim</b> – atomically claim a batch of PENDING rows.
 *       For ENUM strategy, rows are marked IN_PROGRESS; for TIMESTAMP/BOOLEAN,
 *       they are selected with {@code FOR UPDATE SKIP LOCKED}.
 *   <li><b>Publish</b> – each claimed row is sent to Kafka.
 *       After {@code producer.flush()} confirms all acknowledgements, …
 *   <li><b>Mark done</b> – all rows in the batch are marked DONE/PROCESSED.
 *       If publishing fails, rows are marked FAILED instead.
 * </ol>
 *
 * <p>This class is intentionally <em>not</em> a Spring bean; instances are
 * created and managed by {@link OutboxPollerRegistry}.
 */
public class OutboxPoller {

    private static final Logger log = LoggerFactory.getLogger(OutboxPoller.class);

    private final OutboxTableProperties config;
    private final OutboxRepository repository;
    private final OutboxKafkaProducer kafkaProducer;
    private final Counter claimedCounter;
    private final Counter doneCounter;
    private final Counter failedCounter;

    OutboxPoller(OutboxTableProperties config,
                 OutboxRepository repository,
                 OutboxKafkaProducer kafkaProducer,
                 MeterRegistry meterRegistry) {
        this.config = config;
        this.repository = repository;
        this.kafkaProducer = kafkaProducer;

        String table = config.getTableName();
        this.claimedCounter = Counter.builder("outbox.rows.claimed")
                .description("Number of outbox rows claimed for processing")
                .tag("table", table)
                .register(meterRegistry);
        this.doneCounter = Counter.builder("outbox.rows.done")
                .description("Number of outbox rows successfully published to Kafka")
                .tag("table", table)
                .register(meterRegistry);
        this.failedCounter = Counter.builder("outbox.rows.failed")
                .description("Number of outbox rows that failed to publish to Kafka")
                .tag("table", table)
                .register(meterRegistry);
    }

    /**
     * Executes one poll cycle.  All exceptions are caught and logged; the
     * caller (virtual thread loop in {@link OutboxPollerRegistry}) will retry
     * on the next interval.
     */
    public void poll() {
        try {
            resetStuck();
        } catch (Exception e) {
            log.warn("Stuck-row reset failed for table '{}': {}", config.getTableName(), e.getMessage());
        }

        List<OutboxRecord> records;
        try {
            records = repository.claimBatch(config);
        } catch (Exception e) {
            log.error("Failed to claim batch from table '{}': {}", config.getTableName(), e.getMessage(), e);
            return;
        }

        if (records.isEmpty()) {
            return;
        }

        claimedCounter.increment(records.size());
        log.debug("Claimed {} row(s) from table '{}'", records.size(), config.getTableName());

        List<String> ids = records.stream().map(OutboxRecord::id).toList();
        try {
            kafkaProducer.sendBatch(records);
            repository.markDone(config, ids);
            doneCounter.increment(ids.size());
            log.debug("Marked {} row(s) DONE in table '{}'", ids.size(), config.getTableName());
        } catch (Exception e) {
            failedCounter.increment(ids.size());
            log.error("Failed to publish batch from table '{}'; marking {} row(s) as failed: {}",
                    config.getTableName(), ids.size(), e.getMessage(), e);
            try {
                repository.markFailed(config, ids);
            } catch (Exception dbEx) {
                log.error("Could not mark rows as failed in table '{}'; rows may remain stuck in IN_PROGRESS: {}",
                        config.getTableName(), dbEx.getMessage(), dbEx);
            }
        }
    }

    private void resetStuck() {
        int count = repository.resetStuck(config);
        if (count > 0) {
            log.warn("Recovered {} stuck row(s) in table '{}'", count, config.getTableName());
        }
    }

    OutboxTableProperties getConfig() {
        return config;
    }
}
