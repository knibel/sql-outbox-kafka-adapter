package de.knibel.outbox;

import de.knibel.outbox.config.OutboxProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * Entry point for the SQL → Kafka outbox adapter.
 *
 * <p>On startup, {@link de.knibel.outbox.polling.OutboxPollerRegistry}
 * launches one virtual-thread poller per configured outbox table.  Each poller
 * continuously claims pending rows, publishes them to Kafka, and marks them
 * done in the database.
 *
 * <p>Configure tables in {@code application.yml} under the {@code outbox} key.
 * See {@link OutboxProperties} and
 * {@link de.knibel.outbox.config.OutboxTableProperties} for all
 * available options.
 */
@SpringBootApplication
@EnableConfigurationProperties(OutboxProperties.class)
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
