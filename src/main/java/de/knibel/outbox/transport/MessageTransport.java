package de.knibel.outbox.transport;

import de.knibel.outbox.domain.OutboxRecord;
import java.util.List;

/**
 * Abstraction for delivering outbox records to a message broker.
 *
 * <p>Implementations provide the actual transport mechanism (e.g. Kafka,
 * RabbitMQ, etc.).  The adapter ships with a Kafka implementation
 * ({@link de.knibel.outbox.transport.kafka.KafkaMessageTransport}).
 */
public interface MessageTransport {

    /**
     * Publishes all records in the batch and blocks until the broker has
     * acknowledged every record.
     *
     * @param records non-empty list of outbox records to publish
     * @throws RuntimeException if one or more records could not be delivered
     */
    void sendBatch(List<OutboxRecord> records);

    /**
     * Closes the underlying transport resources gracefully.
     */
    void close();
}
