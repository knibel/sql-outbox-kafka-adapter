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
 * <p>The cycle:
 * <ol>
 *   <li><b>Claim</b> – select a batch of pending rows with
 *       {@code FOR UPDATE SKIP LOCKED}.
 *   <li><b>Publish</b> – send each row to Kafka and flush.
 *   <li><b>Mark done</b> – update all rows in the batch to the done value.
 * </ol>
 *
 * <p>Any exception from any step is propagated to the caller
 * ({@link OutboxPollerRegistry}), which will shut the application down.
 * Continuing after a failure is not permitted because it would break
 * message ordering.
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
    }

    /**
     * Executes one poll cycle.
     *
     * <p>Any exception is propagated to the caller without being swallowed.
     * This ensures that an error stops the application rather than silently
     * skipping records and breaking ordering.
     */
    public void poll() {
        List<OutboxRecord> records = repository.claimBatch(config);

        if (records.isEmpty()) {
            return;
        }

        claimedCounter.increment(records.size());
        log.debug("Claimed {} row(s) from table '{}'", records.size(), config.getTableName());

        List<String> ids = records.stream().map(OutboxRecord::id).toList();
        kafkaProducer.sendBatch(records);
        if (config.isDeleteAfterPublish()) {
            repository.deleteByIds(config, ids);
            doneCounter.increment(ids.size());
            log.debug("Deleted {} row(s) from table '{}'", ids.size(), config.getTableName());
        } else {
            repository.markDone(config, ids);
            doneCounter.increment(ids.size());
            log.debug("Marked {} row(s) DONE in table '{}'", ids.size(), config.getTableName());
        }
    }

    OutboxTableProperties getConfig() {
        return config;
    }
}
