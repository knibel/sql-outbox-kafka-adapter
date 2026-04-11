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
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
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
 * Integration test for the {@code DELETE} post-publish strategy.
 *
 * <p>Verifies that rows are:
 * <ol>
 *   <li>Picked up from the outbox table and published to Kafka.
 *   <li><b>Deleted</b> from the outbox table after successful publication
 *       (rather than marked DONE).
 * </ol>
 */
@SpringBootTest(
        classes = Application.class,
        properties = {
                "outbox.tables[0].tableName=test_outbox",
                "outbox.tables[0].idColumn=id",
                "outbox.tables[0].keyColumn=aggregate_id",
                "outbox.tables[0].payloadColumn=payload",
                "outbox.tables[0].headersColumn=headers_json",
                "outbox.tables[0].staticTopic=delete-strategy-topic",
                "outbox.tables[0].statusColumn=status",
                "outbox.tables[0].pendingValue=PENDING",
                "outbox.tables[0].doneValue=DONE",
                "outbox.tables[0].pollIntervalMs=200",
                "outbox.tables[0].batchSize=10",
                "outbox.tables[0].processingStrategy=DELETE",
        }
)
@Testcontainers
class DeleteStrategyIntegrationTest {

    // ── Containers ─────────────────────────────────────────────────────────

    @Container
    static final PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("outbox_test")
                    .withUsername("test")
                    .withPassword("test");

    @Container
    static final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    // ── Dynamic properties ─────────────────────────────────────────────────

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",      postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("outbox.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @BeforeAll
    static void createKafkaTopic() throws Exception {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(adminProps)) {
            admin.createTopics(List.of(new NewTopic("delete-strategy-topic", 1, (short) 1)))
                    .all()
                    .get(10, TimeUnit.SECONDS);
        }
    }

    // ── Test fixtures ───────────────────────────────────────────────────────

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    NamedParameterJdbcTemplate namedJdbc;

    KafkaConsumer<String, String> consumer;

    @BeforeEach
    void setUp() {
        jdbcTemplate.execute("TRUNCATE TABLE test_outbox");

        consumer = createConsumer(kafka.getBootstrapServers());
        consumer.subscribe(Collections.singletonList("delete-strategy-topic"));
        consumer.poll(Duration.ofMillis(500));
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    // ── Tests ───────────────────────────────────────────────────────────────

    @Test
    void shouldDeleteRowsFromOutboxAfterPublishingToKafka() {
        int rowCount = 5;
        List<String> ids = insertPendingRows(rowCount);

        // Wait until all rows are removed from the outbox table.
        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    int remaining = namedJdbc.queryForObject(
                            "SELECT COUNT(*) FROM test_outbox WHERE id IN (:ids)",
                            new MapSqlParameterSource("ids", ids),
                            Integer.class);
                    assertThat(remaining).isZero();
                });

        // Verify the messages were still published to Kafka before being deleted.
        List<ConsumerRecord<String, String>> received = pollAllMessages(rowCount);
        assertThat(received).hasSize(rowCount);

        List<String> receivedKeys = received.stream().map(ConsumerRecord::key).toList();
        assertThat(receivedKeys).containsExactlyInAnyOrderElementsOf(ids);

        for (ConsumerRecord<String, String> record : received) {
            assertThat(record.value()).contains("\"event\":\"ORDER_CREATED\"");
        }
    }

    @Test
    void shouldNotLeaveAnyDoneRowsWhenDeleteStrategyIsUsed() {
        int rowCount = 8;
        insertPendingRows(rowCount);

        // Wait until the table is empty (all rows deleted after publication).
        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    int total = jdbcTemplate.queryForObject(
                            "SELECT COUNT(*) FROM test_outbox",
                            Integer.class);
                    assertThat(total).isZero();
                });

        // Verify messages were published.
        List<ConsumerRecord<String, String>> received = pollAllMessages(rowCount);
        assertThat(received).hasSize(rowCount);
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private List<String> insertPendingRows(int count) {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String id = UUID.randomUUID().toString();
            ids.add(id);
            jdbcTemplate.update(
                    "INSERT INTO test_outbox (id, aggregate_id, payload, headers_json, status) "
                    + "VALUES (?, ?, ?, ?, 'PENDING')",
                    id,
                    id,
                    "{\"event\":\"ORDER_CREATED\",\"orderId\":\"" + id + "\"}",
                    "{\"source\":\"delete-strategy-test\"}");
        }
        return ids;
    }

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
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "delete-strategy-test-consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(props);
    }
}
