package com.knibel.outbox.config;

import com.knibel.outbox.OutboxPoller;
import com.knibel.outbox.OutboxRepository;
import com.knibel.outbox.strategy.DeleteAfterProcessingStrategy;
import com.knibel.outbox.strategy.MarkAsProcessedStrategy;
import com.knibel.outbox.strategy.OutboxProcessingStrategy;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Spring Boot auto-configuration for the SQL outbox Kafka adapter.
 *
 * <p>Automatically registers an {@link OutboxPoller} bean and the configured
 * {@link OutboxProcessingStrategy}. All beans can be overridden by declaring your own
 * beans of the same type in your application context.
 */
@AutoConfiguration
@EnableConfigurationProperties(OutboxProperties.class)
public class OutboxAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public OutboxProcessingStrategy outboxProcessingStrategy(OutboxProperties properties) {
        return switch (properties.getProcessingStrategy()) {
            case DELETE -> new DeleteAfterProcessingStrategy();
            case MARK_AS_PROCESSED -> new MarkAsProcessedStrategy();
        };
    }

    @Bean
    @ConditionalOnMissingBean
    public OutboxPoller outboxPoller(
            OutboxRepository repository,
            KafkaTemplate<String, String> kafkaTemplate,
            OutboxProcessingStrategy strategy,
            OutboxProperties properties) {
        return new OutboxPoller(repository, kafkaTemplate, strategy, properties);
    }
}
