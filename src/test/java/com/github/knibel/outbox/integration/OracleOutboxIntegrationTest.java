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
 * Integration test verifying the {@code STATUS} acknowledgement strategy on Oracle.
 *
 * <ol>
 *   <li>Rows are inserted with {@code status='PENDING'}.
 *   <li>The poller picks them up, publishes to Kafka, and marks them {@code DONE}.
 *   <li>A test Kafka consumer confirms the expected messages arrived.
 *   <li>The database rows are verified to have {@code status='DONE'}.
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
                "outbox.tables[0].staticTopic=oracle-status-topic",
                "outbox.tables[0].statusColumn=status",
                "outbox.tables[0].pendingValue=PENDING",
                "outbox.tables[0].doneValue=DONE",
                "outbox.tables[0].pollIntervalMs=200",
                "outbox.tables[0].batchSize=10",
        }
)
@Testcontainers
class OracleOutboxIntegrationTest {

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
            admin.createTopics(List.of(new NewTopic("oracle-status-topic", 1, (short) 1)))
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
        jdbcTemplate.execute("TRUNCATE TABLE test_outbox");

        consumer = createConsumer(kafkaBootstrapServers);
        consumer.subscribe(Collections.singletonList("oracle-status-topic"));
        consumer.poll(Duration.ofMillis(500));
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    // ── Tests ───────────────────────────────────────────────────────────────

    @Test
    void shouldPublishPendingRowsToKafkaAndMarkThem_done() {
        int rowCount = 5;
        List<String> ids = insertPendingRows(rowCount);

        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    List<String> statuses = namedJdbc.queryForList(
                            "SELECT status FROM test_outbox WHERE id IN (:ids)",
                            new MapSqlParameterSource("ids", ids),
                            String.class);
                    assertThat(statuses)
                            .hasSize(rowCount)
                            .allMatch("DONE"::equals);
                });

        List<ConsumerRecord<String, String>> received = pollAllMessages(rowCount);
        assertThat(received).hasSize(rowCount);

        List<String> receivedKeys = received.stream().map(ConsumerRecord::key).toList();
        assertThat(receivedKeys).containsExactlyInAnyOrderElementsOf(ids);

        for (ConsumerRecord<String, String> record : received) {
            assertThat(record.value()).contains("\"event\":\"ORDER_CREATED\"");
        }
    }

    @Test
    void shouldProcessLargeBatchInMultipleCycles() {
        int rowCount = 25;
        insertPendingRows(rowCount);

        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(300))
                .untilAsserted(() -> {
                    int doneCount = jdbcTemplate.queryForObject(
                            "SELECT COUNT(*) FROM test_outbox WHERE status = 'DONE'",
                            Integer.class);
                    assertThat(doneCount).isEqualTo(rowCount);
                });

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
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "oracle-status-test-consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(props);
    }
}
