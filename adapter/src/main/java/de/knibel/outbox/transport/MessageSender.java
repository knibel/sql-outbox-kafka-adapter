package de.knibel.outbox.transport;

import de.knibel.outbox.domain.OutboxRecord;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Component responsible for sending outbox records to a message broker
 * via a configured {@link MessageTransport} implementation.
 *
 * <p>This is the primary entry point used by the polling infrastructure.
 * The actual transport mechanism (Kafka, RabbitMQ, etc.) is determined by
 * the injected {@link MessageTransport} bean.
 */
@Component
public class MessageSender {

    private static final Logger log = LoggerFactory.getLogger(MessageSender.class);

    private final MessageTransport transport;

    public MessageSender(MessageTransport transport) {
        this.transport = transport;
    }

    /**
     * Publishes all records in the batch via the configured transport.
     *
     * @param records non-empty list of outbox records to publish
     */
    public void sendBatch(List<OutboxRecord> records) {
        if (records.isEmpty()) return;
        log.debug("Sending {} record(s) via {}", records.size(),
                transport.getClass().getSimpleName());
        transport.sendBatch(records);
    }

    /**
     * Closes the underlying transport resources gracefully.
     */
    public void close() {
        transport.close();
    }
}
