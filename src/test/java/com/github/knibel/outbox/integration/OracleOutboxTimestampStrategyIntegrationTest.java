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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test verifying the {@code TIMESTAMP} acknowledgement strategy on Oracle.
 *
 * <ol>
 *   <li>Rows are inserted into the outbox table with {@code processed_at = NULL}.
 *   <li>The poller picks them up, publishes to Kafka, and writes {@code CURRENT_TIMESTAMP}
 *       into the {@code processed_at} column.
 *   <li>A test Kafka consumer confirms the expected messages arrived.
 *   <li>The database rows are verified to have a non-null {@code processed_at}.
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
                "outbox.tables[0].staticTopic=oracle-timestamp-topic",
                "outbox.tables[0].acknowledgementStrategy=TIMESTAMP",
                "outbox.tables[0].processedAtColumn=processed_at",
                "outbox.tables[0].pollIntervalMs=200",
                "outbox.tables[0].batchSize=10",
        }
)
@Testcontainers
class OracleOutboxTimestampStrategyIntegrationTest {

    // ── Containers ─────────────────────────────────────────────────────────

    @Container
    static final OracleContainer oracle =
            new OracleContainer(DockerImageName.parse("gvenzl/oracle-free:23-slim"))
                    .withDatabaseName("testdb")
                    .withUsername("test")
                    .withPassword("test");

    @Container
    static final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    // ── Dynamic properties ─────────────────────────────────────────────────

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",      oracle::getJdbcUrl);
        registry.add("spring.datasource.username", oracle::getUsername);
        registry.add("spring.datasource.password", oracle::getPassword);
        registry.add("spring.sql.init.schema-locations", () -> "classpath:schema-oracle.sql");
        registry.add("outbox.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    // ── Topic setup ────────────────────────────────────────────────────────

    @BeforeAll
    static void createKafkaTopic() throws Exception {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(adminProps)) {
            admin.createTopics(List.of(new NewTopic("oracle-timestamp-topic", 1, (short) 1)))
                    .all()
                    .get(10, TimeUnit.SECONDS);
        }
    }

    // ── Test fixtures ───────────────────────────────────────────────────────

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    NamedParameterJdbcTemplate namedJdbc;

    @Value("${outbox.kafka.bootstrap-servers}")
    String kafkaBootstrapServers;

    KafkaConsumer<String, String> consumer;

    @BeforeEach
    void setUp() {
        jdbcTemplate.execute("DELETE FROM \"test_outbox\"");

        consumer = createConsumer(kafkaBootstrapServers);
        consumer.subscribe(Collections.singletonList("oracle-timestamp-topic"));
        consumer.poll(Duration.ofMillis(500));
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    // ── Tests ───────────────────────────────────────────────────────────────

    @Test
    void shouldPublishToKafkaAndWriteProcessedAtTimestamp() {
        int rowCount = 5;
        List<String> ids = insertRows(rowCount);

        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    int processedCount = namedJdbc.queryForObject(
                            "SELECT COUNT(*) FROM \"test_outbox\""
                            + " WHERE \"id\" IN (:ids) AND \"processed_at\" IS NOT NULL",
                            new MapSqlParameterSource("ids", ids),
                            Integer.class);
                    assertThat(processedCount).isEqualTo(rowCount);
                });

        // Rows should still exist in the table.
        int remaining = namedJdbc.queryForObject(
                "SELECT COUNT(*) FROM \"test_outbox\" WHERE \"id\" IN (:ids)",
                new MapSqlParameterSource("ids", ids),
                Integer.class);
        assertThat(remaining).isEqualTo(rowCount);

        List<ConsumerRecord<String, String>> received = pollAllMessages(rowCount);
        assertThat(received).hasSize(rowCount);

        List<String> receivedKeys = received.stream().map(ConsumerRecord::key).toList();
        assertThat(receivedKeys).containsExactlyInAnyOrderElementsOf(ids);

        for (ConsumerRecord<String, String> record : received) {
            assertThat(record.value()).contains("\"event\":\"ORDER_CREATED\"");
        }
    }

    @Test
    void shouldTimestampLargeBatchInMultipleCycles() {
        int rowCount = 25;
        insertRows(rowCount);

        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(300))
                .untilAsserted(() -> {
                    int processedCount = jdbcTemplate.queryForObject(
                            "SELECT COUNT(*) FROM \"test_outbox\" WHERE \"processed_at\" IS NOT NULL",
                            Integer.class);
                    assertThat(processedCount).isEqualTo(rowCount);
                });

        List<ConsumerRecord<String, String>> received = pollAllMessages(rowCount);
        assertThat(received).hasSize(rowCount);
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private List<String> insertRows(int count) {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String id = UUID.randomUUID().toString();
            ids.add(id);
            jdbcTemplate.update(
                    "INSERT INTO \"test_outbox\" (\"id\", \"aggregate_id\", \"payload\", \"headers_json\") "
                    + "VALUES (?, ?, ?, ?)",
                    id,
                    id,
                    "{\"event\":\"ORDER_CREATED\",\"orderId\":\"" + id + "\"}",
                    "{\"source\":\"integration-test\"}");
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
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "oracle-timestamp-test-consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(props);
    }
}
