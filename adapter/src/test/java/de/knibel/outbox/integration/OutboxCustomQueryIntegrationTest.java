package de.knibel.outbox.integration;

import de.knibel.outbox.Application;
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
 * Integration test for the {@code customQuery} feature.
 *
 * <p>A user-provided SQL query with a cross-table EXISTS join is used
 * instead of the adapter's auto-generated SELECT.  The test inserts rows
 * into two tables and verifies that only rows matching the JOIN condition
 * are published to Kafka.
 */
@SpringBootTest(
        classes = Application.class,
        properties = {
                "outbox.tables[0].tableName=custom_query_outbox",
                "outbox.tables[0].idColumn=id",
                "outbox.tables[0].payloadColumn=payload",
                "outbox.tables[0].staticTopic=custom-query-topic",
                "outbox.tables[0].acknowledgementStrategy=DELETE",
                "outbox.tables[0].pollIntervalMs=200",
                "outbox.tables[0].batchSize=10",
                "outbox.tables[0].customQuery=SELECT o.id, o.payload FROM custom_query_outbox o "
                        + "WHERE EXISTS(SELECT 1 FROM custom_query_status s WHERE s.batch_id = o.id AND s.ready = TRUE)",
        }
)
@Testcontainers
class OutboxCustomQueryIntegrationTest {

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
            admin.createTopics(List.of(new NewTopic("custom-query-topic", 1, (short) 1)))
                    .all()
                    .get(10, TimeUnit.SECONDS);
        }
    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    KafkaConsumer<String, String> consumer;

    @BeforeEach
    void setUp() {
        jdbcTemplate.execute("TRUNCATE TABLE custom_query_outbox");
        jdbcTemplate.execute("TRUNCATE TABLE custom_query_status");

        consumer = createConsumer(kafka.getBootstrapServers());
        consumer.subscribe(Collections.singletonList("custom-query-topic"));
        consumer.poll(Duration.ofMillis(500));
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Test
    void shouldOnlyPublishRowsMatchingCrossTableCondition() {
        // Insert 3 rows into the outbox
        String id1 = UUID.randomUUID().toString();
        String id2 = UUID.randomUUID().toString();
        String id3 = UUID.randomUUID().toString();

        jdbcTemplate.update(
                "INSERT INTO custom_query_outbox (id, payload, status) VALUES (?, ?, 'PENDING')",
                id1, "{\"event\":\"READY_1\"}");
        jdbcTemplate.update(
                "INSERT INTO custom_query_outbox (id, payload, status) VALUES (?, ?, 'PENDING')",
                id2, "{\"event\":\"NOT_READY\"}");
        jdbcTemplate.update(
                "INSERT INTO custom_query_outbox (id, payload, status) VALUES (?, ?, 'PENDING')",
                id3, "{\"event\":\"READY_3\"}");

        // Only id1 and id3 have a matching status row with ready=TRUE
        jdbcTemplate.update(
                "INSERT INTO custom_query_status (batch_id, ready) VALUES (?, TRUE)", id1);
        jdbcTemplate.update(
                "INSERT INTO custom_query_status (batch_id, ready) VALUES (?, FALSE)", id2);
        jdbcTemplate.update(
                "INSERT INTO custom_query_status (batch_id, ready) VALUES (?, TRUE)", id3);

        // Wait until the two "ready" rows are deleted (DELETE strategy)
        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    int remaining = jdbcTemplate.queryForObject(
                            "SELECT COUNT(*) FROM custom_query_outbox WHERE id IN (?, ?)",
                            Integer.class, id1, id3);
                    assertThat(remaining).isZero();
                });

        // id2 should still be in the table (not matched by the custom query)
        int id2Count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM custom_query_outbox WHERE id = ?",
                Integer.class, id2);
        assertThat(id2Count).isEqualTo(1);

        // Verify Kafka received exactly 2 messages
        List<ConsumerRecord<String, String>> received = pollAllMessages(2);
        assertThat(received).hasSize(2);

        List<String> receivedKeys = received.stream().map(ConsumerRecord::key).toList();
        assertThat(receivedKeys).containsExactlyInAnyOrder(id1, id3);
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private List<ConsumerRecord<String, String>> pollAllMessages(int expectedCount) {
        List<ConsumerRecord<String, String>> all = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 10_000;
        while (all.size() < expectedCount && System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            records.forEach(all::add);
        }
        return all;
    }

    private static KafkaConsumer<String, String> createConsumer(String bootstrapServers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "custom-query-test-consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(props);
    }
}
