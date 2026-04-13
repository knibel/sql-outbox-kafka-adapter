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
     * Acknowledges that the given rows have been successfully published,
     * delegating to the appropriate {@link AcknowledgementHandler} based
     * on the configured {@link de.knibel.outbox.config.AcknowledgementStrategy}.
     *
     * @param config table-specific configuration
     * @param ids    primary-key values of the rows to acknowledge
     */
    void acknowledge(OutboxTableProperties config, List<String> ids);
}
