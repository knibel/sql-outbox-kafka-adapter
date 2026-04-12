package com.github.knibel.outbox.integration;

import com.github.knibel.outbox.Application;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for the {@code CUSTOM} acknowledgement strategy.
 *
 * <p>Verifies that after publishing a row to Kafka, the adapter executes
 * the user-provided {@code customAcknowledgementQuery} which updates both
 * a flag column and a timestamp column simultaneously.
 */
@SpringBootTest(
        classes = Application.class,
        properties = {
                "outbox.tables[0].tableName=custom_ack_outbox",
                "outbox.tables[0].idColumn=id",
                "outbox.tables[0].payloadColumn=payload",
                "outbox.tables[0].staticTopic=custom-ack-topic",
                "outbox.tables[0].acknowledgementStrategy=CUSTOM",
                "outbox.tables[0].customAcknowledgementQuery="
                        + "UPDATE custom_ack_outbox SET ack_flag = 1, ack_timestamp = CURRENT_TIMESTAMP WHERE id = ?",
                // Use a custom query to only pick rows where ack_flag = 0
                "outbox.tables[0].customQuery="
                        + "SELECT id, payload FROM custom_ack_outbox WHERE ack_flag = 0",
                "outbox.tables[0].pollIntervalMs=200",
                "outbox.tables[0].batchSize=10",
        }
)
@Testcontainers
class OutboxCustomAcknowledgementIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("outbox_test")
                    .withUsername("test")
                    .withPassword("test");

    @Container
    static final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("outbox.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @BeforeAll
    static void createKafkaTopic() throws Exception {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(adminProps)) {
            admin.createTopics(List.of(new NewTopic("custom-ack-topic", 1, (short) 1)))
                    .all()
                    .get(10, TimeUnit.SECONDS);
        }
    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    KafkaConsumer<String, String> consumer;

    @BeforeEach
    void setUp() {
        jdbcTemplate.execute("TRUNCATE TABLE custom_ack_outbox");

        consumer = createConsumer(kafka.getBootstrapServers());
        consumer.subscribe(Collections.singletonList("custom-ack-topic"));
        consumer.poll(Duration.ofMillis(500));
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Test
    void shouldExecuteCustomAcknowledgementAfterPublishing() {
        int rowCount = 3;
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            String id = UUID.randomUUID().toString();
            ids.add(id);
            jdbcTemplate.update(
                    "INSERT INTO custom_ack_outbox (id, payload) VALUES (?, ?)",
                    id, "{\"event\":\"TEST_" + i + "\"}");
        }

        // Wait until all rows have ack_flag = 1
        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    int ackedCount = jdbcTemplate.queryForObject(
                            "SELECT COUNT(*) FROM custom_ack_outbox WHERE ack_flag = 1",
                            Integer.class);
                    assertThat(ackedCount).isEqualTo(rowCount);
                });

        // Verify ack_timestamp was set for all rows
        int withTimestamp = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM custom_ack_outbox WHERE ack_timestamp IS NOT NULL",
                Integer.class);
        assertThat(withTimestamp).isEqualTo(rowCount);

        // Rows should still exist (not deleted)
        int totalRows = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM custom_ack_outbox",
                Integer.class);
        assertThat(totalRows).isEqualTo(rowCount);

        // Verify the corresponding Kafka messages arrived.
        List<ConsumerRecord<String, String>> received = pollAllMessages(rowCount);
        assertThat(received).hasSize(rowCount);

        List<String> receivedKeys = received.stream().map(ConsumerRecord::key).toList();
        assertThat(receivedKeys).containsExactlyInAnyOrderElementsOf(ids);
    }

    @Test
    void shouldNotReprocessAcknowledgedRows() {
        String id = UUID.randomUUID().toString();
        jdbcTemplate.update(
                "INSERT INTO custom_ack_outbox (id, payload) VALUES (?, ?)",
                id, "{\"event\":\"ONCE\"}");

        // Wait until row is acknowledged
        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    int ackedCount = jdbcTemplate.queryForObject(
                            "SELECT ack_flag FROM custom_ack_outbox WHERE id = ?",
                            Integer.class, id);
                    assertThat(ackedCount).isEqualTo(1);
                });

        // Consume the message
        List<ConsumerRecord<String, String>> received = pollAllMessages(1);
        assertThat(received).hasSize(1);

        // Wait a bit more and verify no duplicate messages appear
        List<ConsumerRecord<String, String>> extra = pollAllMessages(1);
        // Should not get more than we already got
        assertThat(extra).isEmpty();
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private List<ConsumerRecord<String, String>> pollAllMessages(int expectedCount) {
        List<ConsumerRecord<String, String>> all = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 5_000;
        while (all.size() < expectedCount && System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            records.forEach(all::add);
        }
        return all;
    }

    private static KafkaConsumer<String, String> createConsumer(String bootstrapServers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "custom-ack-test-consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(props);
    }
}
