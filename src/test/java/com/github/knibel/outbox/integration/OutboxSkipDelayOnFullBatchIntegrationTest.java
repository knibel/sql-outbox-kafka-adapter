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
 * Integration test verifying the {@code skipDelayOnFullBatch} behaviour.
 *
 * <p>The poller is configured with a very long polling interval (10 s) but
 * {@code skipDelayOnFullBatch=true} and a small batch size (5).  A backlog
 * of rows larger than one batch is inserted before the test assertion runs.
 *
 * <p>Expected behaviour: because every cycle until the last one returns a
 * full batch, the poller skips the inter-cycle delay and drains the entire
 * backlog far faster than the configured 10 s interval.  The assertion
 * window is deliberately tighter than the interval to prove the skip is
 * actually happening.
 */
@SpringBootTest(
        classes = Application.class,
        properties = {
                "outbox.tables[0].tableName=skip_delay_outbox",
                "outbox.tables[0].idColumn=id",
                "outbox.tables[0].payloadColumn=payload",
                "outbox.tables[0].staticTopic=skip-delay-topic",
                "outbox.tables[0].statusColumn=status",
                "outbox.tables[0].pendingValue=PENDING",
                "outbox.tables[0].doneValue=DONE",
                "outbox.tables[0].pollIntervalMs=10000",
                "outbox.tables[0].batchSize=5",
                "outbox.tables[0].skipDelayOnFullBatch=true",
        }
)
@Testcontainers
class OutboxSkipDelayOnFullBatchIntegrationTest {

    // ── Containers ─────────────────────────────────────────────────────────

    @Container
    static final PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("outbox_skip_delay_test")
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

    // ── Topic setup ────────────────────────────────────────────────────────

    @BeforeAll
    static void createKafkaTopic() throws Exception {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(adminProps)) {
            admin.createTopics(List.of(new NewTopic("skip-delay-topic", 1, (short) 1)))
                    .all()
                    .get(10, TimeUnit.SECONDS);
        }
    }

    // ── Test fixtures ───────────────────────────────────────────────────────

    @Autowired
    JdbcTemplate jdbcTemplate;

    KafkaConsumer<String, String> consumer;

    @BeforeEach
    void setUp() {
        jdbcTemplate.execute("TRUNCATE TABLE skip_delay_outbox");

        consumer = createConsumer(kafka.getBootstrapServers());
        consumer.subscribe(Collections.singletonList("skip-delay-topic"));
        // Seek past any leftover messages from previous test runs.
        consumer.poll(Duration.ofMillis(500));
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    // ── Tests ───────────────────────────────────────────────────────────────

    /**
     * Inserts a backlog that spans several batches (25 rows, batchSize=5 → 5 cycles).
     * With {@code skipDelayOnFullBatch=true} and a 10 s poll interval the entire
     * backlog must be drained well within the 10 s window; without the skip it
     * would require at least 4 × 10 s = 40 s for the intermediate full batches.
     */
    @Test
    void shouldDrainMultipleBatchesWithoutWaitingForPollInterval() {
        int rowCount = 25;
        insertPendingRows(rowCount);

        // All rows must be processed in far less than the 10 s poll interval,
        // which demonstrates that the inter-cycle delay is being skipped.
        await().atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    int doneCount = jdbcTemplate.queryForObject(
                            "SELECT COUNT(*) FROM skip_delay_outbox WHERE status = 'DONE'",
                            Integer.class);
                    assertThat(doneCount).isEqualTo(rowCount);
                });

        List<ConsumerRecord<String, String>> received = pollAllMessages(rowCount);
        assertThat(received).hasSize(rowCount);
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private void insertPendingRows(int count) {
        for (int i = 0; i < count; i++) {
            String id = UUID.randomUUID().toString();
            jdbcTemplate.update(
                    "INSERT INTO skip_delay_outbox (id, payload, status) "
                    + "VALUES (?, ?, 'PENDING')",
                    id,
                    "{\"event\":\"SKIP_DELAY_TEST\",\"id\":\"" + id + "\"}");
        }
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
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "skip-delay-test-consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(props);
    }
}
