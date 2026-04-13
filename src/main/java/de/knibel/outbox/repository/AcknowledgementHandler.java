package de.knibel.outbox.repository;

import de.knibel.outbox.config.OutboxTableProperties;
import java.util.List;

/**
 * Strategy for acknowledging that outbox rows have been successfully
 * published to the message broker.
 *
 * <p>Each implementation corresponds to a
 * {@link de.knibel.outbox.config.AcknowledgementStrategy} configuration
 * option and encapsulates the specific acknowledgement behaviour (e.g.
 * updating a status column, deleting rows, or writing a timestamp).
 *
 * <p>This interface is <em>not</em> dependent on JDBC or any specific
 * persistence technology.
 */
public interface AcknowledgementHandler {

    /**
     * Acknowledges that the given rows have been successfully published.
     *
     * @param config table-specific configuration
     * @param ids    primary-key values of the rows to acknowledge
     */
    void acknowledge(OutboxTableProperties config, List<String> ids);
}
