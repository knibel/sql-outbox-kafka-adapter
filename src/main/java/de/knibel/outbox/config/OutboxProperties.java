package de.knibel.outbox.config;

import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Top-level configuration for the SQL → Kafka outbox adapter.
 * Bound from the {@code outbox} prefix in {@code application.yml}.
 *
 * <p>Example:
 * <pre>
 * outbox:
 *   kafka:
 *     bootstrapServers: localhost:9092
 *   tables:
 *     - tableName: orders_outbox
 *       ...
 * </pre>
 */
@ConfigurationProperties(prefix = "outbox")
public class OutboxProperties {

    private KafkaProperties kafka = new KafkaProperties();
    private List<OutboxTableProperties> tables = new ArrayList<>();

    public KafkaProperties getKafka() {
        return kafka;
    }

    public void setKafka(KafkaProperties kafka) {
        this.kafka = kafka;
    }

    public List<OutboxTableProperties> getTables() {
        return tables;
    }

    public void setTables(List<OutboxTableProperties> tables) {
        this.tables = tables;
    }

    /** Kafka producer connection settings. */
    public static class KafkaProperties {

        /** Comma-separated list of broker host:port pairs. */
        private String bootstrapServers = "localhost:9092";

        /**
         * Transactional ID prefix for the Kafka producer.
         * When set, the producer runs in transactional mode (exactly-once across
         * JVM restarts). Each table gets its own transactional ID derived from this
         * prefix plus the table name. Leave empty to use the idempotent-only mode.
         */
        private String transactionalIdPrefix = "";

        /** Additional producer properties forwarded verbatim to KafkaProducer. */
        private java.util.Map<String, String> producerProperties = new java.util.HashMap<>();

        public String getBootstrapServers() {
            return bootstrapServers;
        }

        public void setBootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        public String getTransactionalIdPrefix() {
            return transactionalIdPrefix;
        }

        public void setTransactionalIdPrefix(String transactionalIdPrefix) {
            this.transactionalIdPrefix = transactionalIdPrefix;
        }

        public java.util.Map<String, String> getProducerProperties() {
            return producerProperties;
        }

        public void setProducerProperties(java.util.Map<String, String> producerProperties) {
            this.producerProperties = producerProperties;
        }
    }
}
