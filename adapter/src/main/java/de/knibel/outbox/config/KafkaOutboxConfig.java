package de.knibel.outbox.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Creates the shared {@link KafkaProducer} bean used by all table pollers.
 *
 * <p>The producer is configured for idempotent delivery by default:
 * <ul>
 *   <li>{@code enable.idempotence=true} – broker-side deduplication within a
 *       producer session (prevents duplicates on retry after a transient error).
 *   <li>{@code acks=all} – waits for all in-sync replicas to acknowledge.
 *   <li>{@code retries=MAX_INT} – retries indefinitely on transient failures.
 *   <li>{@code max.in.flight.requests.per.connection=5} – maximum in-flight
 *       requests while keeping idempotence guarantees.
 * </ul>
 *
 * <p>If {@code outbox.kafka.transactionalIdPrefix} is non-empty, the producer
 * is additionally configured for <em>exactly-once</em> semantics via Kafka
 * transactions (requires Kafka 2.5+).
 */
@Configuration
public class KafkaOutboxConfig {

    @Bean
    @ConditionalOnMissingBean(ObjectMapper.class)
    public ObjectMapper objectMapper() {
        // Use JsonMapper.builder() for a properly immutable, thread-safe instance.
        // Respects user-provided ObjectMapper beans if present in the context.
        return JsonMapper.builder().findAndAddModules().build();
    }

    @Bean
    public KafkaProducer<String, String> kafkaProducer(OutboxProperties outboxProperties) {
        OutboxProperties.KafkaProperties kafkaCfg = outboxProperties.getKafka();

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCfg.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Idempotent producer settings
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        // Optional transactional mode
        String txPrefix = kafkaCfg.getTransactionalIdPrefix();
        if (txPrefix != null && !txPrefix.isBlank()) {
            props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txPrefix);
        }

        // Allow caller-provided overrides (e.g. compression, linger.ms)
        props.putAll(kafkaCfg.getProducerProperties());

        return new KafkaProducer<>(props);
    }
}
