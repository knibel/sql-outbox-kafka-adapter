package de.knibel.outbox.polling;

import de.knibel.outbox.config.OutboxTableProperties;
import de.knibel.outbox.domain.OutboxRecord;
import de.knibel.outbox.repository.OutboxRepository;
import de.knibel.outbox.transport.MessageSender;
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
 *   <li><b>Publish</b> – send each row via the configured
 *       {@link MessageSender} and flush.
 *   <li><b>Acknowledge</b> – apply the configured
 *       {@link AcknowledgementStrategy}: update a status column, delete the
 *       rows, or write a processed-at timestamp.
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
    private final MessageSender messageSender;
    private final Counter claimedCounter;
    private final Counter processedCounter;

    OutboxPoller(OutboxTableProperties config,
                 OutboxRepository repository,
                 MessageSender messageSender,
                 MeterRegistry meterRegistry) {
        this.config = config;
        this.repository = repository;
        this.messageSender = messageSender;

        String table = config.getTableName();
        this.claimedCounter = Counter.builder("outbox.rows.claimed")
                .description("Number of outbox rows claimed for processing")
                .tag("table", table)
                .register(meterRegistry);
        this.processedCounter = Counter.builder("outbox.rows.processed")
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
     *
     * @return the number of records processed in this cycle (0 if there were
     *         no pending rows)
     */
    public int poll() {
        List<OutboxRecord> records = repository.claimBatch(config);

        if (records.isEmpty()) {
            return 0;
        }

        claimedCounter.increment(records.size());
        log.debug("Claimed {} row(s) from table '{}'", records.size(), config.getTableName());

        List<String> ids = records.stream().map(OutboxRecord::id).toList();
        messageSender.sendBatch(records);
        repository.acknowledge(config, ids);
        log.debug("Acknowledged {} row(s) in table '{}'", ids.size(), config.getTableName());
        processedCounter.increment(ids.size());
        return ids.size();
    }

    OutboxTableProperties getConfig() {
        return config;
    }
}
