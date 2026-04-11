package com.github.knibel.outbox.kafka;

import com.github.knibel.outbox.domain.OutboxRecord;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Thin wrapper around {@link KafkaProducer} that publishes a batch of
 * {@link OutboxRecord}s and waits for broker acknowledgement.
 *
 * <p><b>Delivery guarantee:</b>
 * <ul>
 *   <li>Each record is sent asynchronously (non-blocking send callback).
 *   <li>After all records are enqueued, {@link KafkaProducer#flush()} is
 *       called once to wait for all in-flight sends to complete.
 *   <li>Per-record errors are collected and rethrown as a single
 *       {@link KafkaBatchException} so the caller can react uniformly.
 * </ul>
 *
 * <p><b>Idempotence:</b> the underlying producer is configured with
 * {@code enable.idempotence=true} (see {@link
 * com.github.knibel.outbox.config.KafkaOutboxConfig}), which prevents
 * duplicate messages on producer retries within a session.
 */
@Component
public class OutboxKafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(OutboxKafkaProducer.class);

    private final KafkaProducer<String, String> producer;

    public OutboxKafkaProducer(KafkaProducer<String, String> producer) {
        this.producer = producer;
    }

    /**
     * Publishes all records in the batch and blocks until the broker has
     * acknowledged every record.
     *
     * @param records non-empty list of outbox records to publish.
     * @throws KafkaBatchException if one or more records could not be delivered.
     * @throws IllegalArgumentException if a record has a null or blank topic.
     */
    public void sendBatch(List<OutboxRecord> records) {
        if (records.isEmpty()) return;

        List<CompletableFuture<Void>> futures = new ArrayList<>(records.size());

        for (OutboxRecord record : records) {
            validateTopic(record);
            ProducerRecord<String, String> pr = toProducerRecord(record);
            CompletableFuture<Void> future = new CompletableFuture<>();

            producer.send(pr, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Failed to send outbox record id={} to topic={}: {}",
                            record.id(), record.topic(), exception.getMessage(), exception);
                    future.completeExceptionally(exception);
                } else {
                    log.debug("Sent outbox record id={} to {}@{}/{}",
                            record.id(), record.topic(),
                            metadata.partition(), metadata.offset());
                    future.complete(null);
                }
            });
            futures.add(future);
        }

        // Wait for all in-flight sends to reach the broker (or fail).
        producer.flush();

        // Collect any failures after flush completes.
        List<Throwable> failures = futures.stream()
                .filter(CompletableFuture::isCompletedExceptionally)
                .map(f -> {
                    try {
                        f.join();
                        return null;
                    } catch (Exception e) {
                        return (Throwable) e.getCause() != null ? e.getCause() : e;
                    }
                })
                .toList();

        if (!failures.isEmpty()) {
            throw new KafkaBatchException(failures.size() + " of " + records.size()
                    + " record(s) failed to publish", failures);
        }
    }

    /** Closes the underlying producer gracefully. */
    public void close() {
        producer.close();
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private ProducerRecord<String, String> toProducerRecord(OutboxRecord record) {
        ProducerRecord<String, String> pr =
                new ProducerRecord<>(record.topic(), record.kafkaKey(), record.payload());

        Map<String, String> headers = record.headers();
        if (headers != null) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                pr.headers().add(new RecordHeader(
                        entry.getKey(),
                        entry.getValue() != null
                                ? entry.getValue().getBytes(StandardCharsets.UTF_8)
                                : new byte[0]));
            }
        }
        return pr;
    }

    private void validateTopic(OutboxRecord record) {
        if (record.topic() == null || record.topic().isBlank()) {
            throw new IllegalArgumentException(
                    "Outbox record id=" + record.id() + " has no Kafka topic configured. "
                    + "Set 'topicColumn' or 'staticTopic' in the table configuration.");
        }
    }

    /**
     * Thrown when one or more records in a batch could not be delivered to
     * Kafka.
     */
    public static class KafkaBatchException extends RuntimeException {
        private final List<Throwable> causes;

        public KafkaBatchException(String message, List<Throwable> causes) {
            super(message, causes.isEmpty() ? null : causes.get(0));
            this.causes = List.copyOf(causes);
        }

        public List<Throwable> getCauses() {
            return causes;
        }
    }
}
