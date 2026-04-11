package com.knibel.outbox;

import com.knibel.outbox.config.OutboxProperties;
import com.knibel.outbox.strategy.OutboxProcessingStrategy;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.annotation.Transactional;

/**
 * Periodically polls the outbox table, publishes pending messages to Kafka, and delegates
 * post-publish handling to the configured {@link OutboxProcessingStrategy}.
 *
 * <p>Messages are fetched in batches (size configured via {@code outbox.batch-size}) and
 * published synchronously. After each successful publish the strategy decides whether to
 * delete the row or mark it as processed.
 *
 * <p>If publishing a message fails the exception is logged and the remaining messages in
 * the current batch are still attempted, so a single bad message does not block the queue.
 */
@EnableScheduling
public class OutboxPoller {

    private static final Logger log = LoggerFactory.getLogger(OutboxPoller.class);

    private final OutboxRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final OutboxProcessingStrategy processingStrategy;
    private final OutboxProperties properties;

    public OutboxPoller(
            OutboxRepository repository,
            KafkaTemplate<String, String> kafkaTemplate,
            OutboxProcessingStrategy processingStrategy,
            OutboxProperties properties) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.processingStrategy = processingStrategy;
        this.properties = properties;
    }

    /**
     * Polling loop. The fixed-delay is set to 1 ms here; the actual interval is driven by
     * {@code outbox.polling-interval-ms} via a SpEL expression so that it can be changed
     * in configuration without recompiling.
     */
    @Scheduled(fixedDelayString = "#{outboxProperties.pollingIntervalMs}")
    @Transactional
    public void poll() {
        List<OutboxMessage> messages = repository.findPendingMessages(properties.getBatchSize());
        if (messages.isEmpty()) {
            return;
        }

        log.debug("Outbox poller found {} pending message(s)", messages.size());

        for (OutboxMessage message : messages) {
            publishMessage(message);
        }
    }

    private void publishMessage(OutboxMessage message) {
        try {
            SendResult<String, String> result = kafkaTemplate
                    .send(message.getTopic(), message.getMessageKey(), message.getPayload())
                    .get();

            log.debug(
                    "Published outbox message id={} to topic={} partition={} offset={}",
                    message.getId(),
                    result.getRecordMetadata().topic(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());

            processingStrategy.onSuccess(message, repository);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while publishing outbox message id={}", message.getId(), e);
        } catch (Exception e) {
            log.error("Failed to publish outbox message id={}", message.getId(), e);
        }
    }
}
