package de.knibel.outbox.repository;

import de.knibel.outbox.config.OutboxTableProperties;
import de.knibel.outbox.domain.OutboxRecord;
import java.util.List;

/**
 * Abstraction for outbox table operations.
 *
 * <p>This interface is <em>not</em> dependent on JDBC or any specific
 * persistence technology.  It returns the domain model
 * ({@link OutboxRecord}) and accepts configuration via
 * {@link OutboxTableProperties}.
 */
public interface OutboxRepository {

    /**
     * Claims a batch of pending outbox rows for processing.
     *
     * @param config table-specific configuration
     * @return the pending rows mapped to {@link OutboxRecord}s; empty if none
     */
    List<OutboxRecord> claimBatch(OutboxTableProperties config);

    /**
     * Marks rows as done (STATUS acknowledgement strategy).
     */
    void markDone(OutboxTableProperties config, List<String> ids);

    /**
     * Deletes rows (DELETE acknowledgement strategy).
     */
    void deleteByIds(OutboxTableProperties config, List<String> ids);

    /**
     * Records the current timestamp (TIMESTAMP acknowledgement strategy).
     */
    void markProcessedAt(OutboxTableProperties config, List<String> ids);

    /**
     * Executes user-provided acknowledgement SQL (CUSTOM acknowledgement strategy).
     */
    void executeCustomAcknowledgement(OutboxTableProperties config, List<String> ids);
}
