package de.knibel.outbox.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.knibel.outbox.Application;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
 * Integration test for the cross-join two-table pattern.
 *
 * <p>Two outbox table configurations are exercised simultaneously:
 *
 * <ol>
 *   <li><b>data_records</b> – rows are selected via a cross-table EXISTS join
 *       with {@code batch_status}, published as {@code DataRequest.ItemsRequested}
 *       events to the {@code data-request} topic, then deleted
 *       (CUSTOM acknowledgement).
 *   <li><b>batch_status</b> – rows with {@code fetched_flag = 0} and no
 *       remaining {@code data_records} rows for the same batch are published
 *       as {@code BatchStatus.BatchInfoProvided} events to the
 *       {@code batch-status} topic, then acknowledged by setting
 *       {@code fetched_flag = 1} and writing {@code fetched_at = now}
 *       (CUSTOM acknowledgement).
 * </ol>
 *
 * <p>The combined end-to-end flow:
 * <pre>
 *   INSERT batch_status  (batch registered, not yet fetched)
 *   INSERT data_records  (individual records for the batch)
 *       ↓
 *   Poller 1 picks up data_records rows (EXISTS match)
 *   → publishes DataRequest.ItemsRequested → DELETE data_records rows
 *       ↓
 *   Poller 2 picks up batch_status (NOT EXISTS satisfied)
 *   → publishes BatchStatus.BatchInfoProvided → fetched_flag = 1
 * </pre>
 *
 * <p>See {@code docs/examples/cross-join-two-tables/application.yml} for the
 * equivalent production configuration (Oracle column names in uppercase).
 */
@SpringBootTest(
        classes = Application.class,
        properties = {
                // ── Table 0: data_records ────────────────────────────────────────────
                "outbox.tables[0].tableName=data_records",
                "outbox.tables[0].idColumn=load_id",
                "outbox.tables[0].keyColumn=entity_id",
                "outbox.tables[0].staticTopic=data-request",
                "outbox.tables[0].acknowledgementStrategy=CUSTOM",
                "outbox.tables[0].customAcknowledgementQuery="
                        + "DELETE FROM data_records WHERE load_id = ?",
                "outbox.tables[0].customQuery="
                        + "SELECT * FROM data_records t1 WHERE EXISTS ("
                        + "SELECT 1 FROM batch_status t2 "
                        + "WHERE t2.reference_date = t1.reference_date "
                        + "AND t2.batch_id = t1.batch_ref_id)",
                "outbox.tables[0].rowMappingStrategy=CUSTOM",
                "outbox.tables[0].fieldMappings.entity_id.name=eventData.entityId",
                "outbox.tables[0].fieldMappings.entity_id.dataType=LONG",
                "outbox.tables[0].fieldMappings.reference_date.name=eventData.items.referenceDate",
                "outbox.tables[0].fieldMappings.reference_date.dataType=DATE",
                "outbox.tables[0].fieldMappings.reference_date.format=yyyy-MM-dd",
                "outbox.tables[0].fieldMappings.batch_ref_id.name=eventData.items.batchId",
                "outbox.tables[0].fieldMappings.batch_ref_id.dataType=LONG",
                "outbox.tables[0].fieldMappings.default_flag.name=eventData.items.defaultFlag",
                "outbox.tables[0].fieldMappings.default_flag.dataType=BOOLEAN",
                "outbox.tables[0].fieldMappings.duplicate_flag.name=eventData.items.duplicateFlag",
                "outbox.tables[0].fieldMappings.duplicate_flag.dataType=BOOLEAN",
                "outbox.tables[0].fieldMappings.amount.name=eventData.items.amount",
                "outbox.tables[0].fieldMappings.amount.dataType=DECIMAL",
                "outbox.tables[0].fieldMappings.liability_amount.name=eventData.items.liabilityAmount",
                "outbox.tables[0].fieldMappings.liability_amount.dataType=DECIMAL",
                "outbox.tables[0].fieldMappings.liability_ts.name=eventData.items.liabilityTs",
                "outbox.tables[0].fieldMappings.liability_ts.dataType=DATETIME",
                "outbox.tables[0].fieldMappings.liability_ts.format=yyyy-MM-dd'T'HH:mm:ss",
                "outbox.tables[0].staticFields.eventType=DataRequest.ItemsRequested",
                "outbox.tables[0].staticFields.eventData.companyId=1",
                "outbox.tables[0].pollIntervalMs=200",
                "outbox.tables[0].batchSize=10",

                // ── Table 1: batch_status ────────────────────────────────────────────
                "outbox.tables[1].tableName=batch_status",
                "outbox.tables[1].idColumn=batch_id",
                "outbox.tables[1].keyColumn=batch_id",
                "outbox.tables[1].staticTopic=batch-status",
                "outbox.tables[1].acknowledgementStrategy=CUSTOM",
                "outbox.tables[1].customAcknowledgementQuery="
                        + "UPDATE batch_status "
                        + "SET fetched_flag = 1, fetched_at = CURRENT_TIMESTAMP "
                        + "WHERE batch_id = ?",
                "outbox.tables[1].customQuery="
                        + "SELECT * FROM batch_status t1 "
                        + "WHERE t1.fetched_flag = 0 "
                        + "AND NOT EXISTS ("
                        + "SELECT 1 FROM data_records t2 "
                        + "WHERE t2.reference_date = t1.reference_date "
                        + "AND t2.batch_ref_id = t1.batch_id)",
                "outbox.tables[1].rowMappingStrategy=CUSTOM",
                "outbox.tables[1].fieldMappings.record_count.name=eventData.recordCount",
                "outbox.tables[1].fieldMappings.record_count.dataType=LONG",
                "outbox.tables[1].fieldMappings.reference_date.name=eventData.referenceDate",
                "outbox.tables[1].fieldMappings.reference_date.dataType=DATE",
                "outbox.tables[1].fieldMappings.reference_date.format=yyyy-MM-dd",
                "outbox.tables[1].fieldMappings.batch_id.name=eventData.batchId",
                "outbox.tables[1].fieldMappings.batch_id.dataType=LONG",
                "outbox.tables[1].staticFields.eventType=BatchStatus.BatchInfoProvided",
                "outbox.tables[1].pollIntervalMs=200",
                "outbox.tables[1].batchSize=10",
        }
)
@Testcontainers
class CrossJoinTwoTablesIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("outbox_test")
                    .withUsername("test")
                    .withPassword("test");

    @Container
    static final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    /** Generates unique IDs per test run to avoid cross-test interference. */
    private static final AtomicLong ID_SEQUENCE = new AtomicLong(1_000_000);

    /** Returns a unique string ID backed by an incrementing counter. */
    private static String nextId() {
        return String.valueOf(ID_SEQUENCE.getAndIncrement());
    }

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",      postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("outbox.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @BeforeAll
    static void createKafkaTopics() throws Exception {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(adminProps)) {
            admin.createTopics(List.of(
                    new NewTopic("data-request", 1, (short) 1),
                    new NewTopic("batch-status", 1, (short) 1)
            )).all().get(10, TimeUnit.SECONDS);
        }
    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    ObjectMapper objectMapper;

    KafkaConsumer<String, String> dataRequestConsumer;
    KafkaConsumer<String, String> batchStatusConsumer;

    @BeforeEach
    void setUp() {
        jdbcTemplate.execute("TRUNCATE TABLE data_records");
        jdbcTemplate.execute("TRUNCATE TABLE batch_status");

        dataRequestConsumer = createConsumer(kafka.getBootstrapServers(), "data-request-consumer");
        dataRequestConsumer.subscribe(Collections.singletonList("data-request"));
        dataRequestConsumer.poll(Duration.ofMillis(500));

        batchStatusConsumer = createConsumer(kafka.getBootstrapServers(), "batch-status-consumer");
        batchStatusConsumer.subscribe(Collections.singletonList("batch-status"));
        batchStatusConsumer.poll(Duration.ofMillis(500));
    }

    @AfterEach
    void tearDown() {
        dataRequestConsumer.close();
        batchStatusConsumer.close();
    }

    /**
     * Full end-to-end scenario:
     * <ol>
     *   <li>Insert one {@code batch_status} row and two {@code data_records}
     *       rows for the same batch.
     *   <li>Wait for both {@code data_records} rows to be published and deleted.
     *   <li>Wait for the {@code batch_status} row to be published and
     *       acknowledged.
     * </ol>
     */
    @Test
    void shouldProcessDataRowsThenPublishBatchStatus() {
        String batchId   = nextId();
        String recordId1 = nextId();
        String recordId2 = nextId();

        jdbcTemplate.update(
                "INSERT INTO batch_status (batch_id, reference_date, record_count, fetched_flag) "
                + "VALUES (?, DATE '2024-03-01', 2, 0)",
                batchId);

        jdbcTemplate.update(
                "INSERT INTO data_records (load_id, reference_date, batch_ref_id, entity_id, "
                + "amount, liability_amount, liability_ts) "
                + "VALUES (?, DATE '2024-03-01', ?, 100001, 1500.00, 200.00, "
                + "TIMESTAMP '2024-03-01 08:00:00')",
                recordId1, batchId);
        jdbcTemplate.update(
                "INSERT INTO data_records (load_id, reference_date, batch_ref_id, entity_id, "
                + "amount, liability_amount, liability_ts) "
                + "VALUES (?, DATE '2024-03-01', ?, 100002, 2500.00, 300.00, "
                + "TIMESTAMP '2024-03-01 09:00:00')",
                recordId2, batchId);

        // Both data_records rows must be deleted (published + acknowledged)
        await().atMost(Duration.ofSeconds(20))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    int remaining = jdbcTemplate.queryForObject(
                            "SELECT COUNT(*) FROM data_records WHERE batch_ref_id = ?",
                            Integer.class, batchId);
                    assertThat(remaining).isZero();
                });

        List<ConsumerRecord<String, String>> dataRequestMessages = pollAllMessages(dataRequestConsumer, 2);
        assertThat(dataRequestMessages).hasSize(2);

        // The batch_status row must be acknowledged
        await().atMost(Duration.ofSeconds(20))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    int fetched = jdbcTemplate.queryForObject(
                            "SELECT fetched_flag FROM batch_status WHERE batch_id = ?",
                            Integer.class, batchId);
                    assertThat(fetched).isEqualTo(1);
                });

        // fetched_at must have been written
        int withTimestamp = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM batch_status "
                + "WHERE batch_id = ? AND fetched_at IS NOT NULL",
                Integer.class, batchId);
        assertThat(withTimestamp).isEqualTo(1);

        List<ConsumerRecord<String, String>> batchStatusMessages = pollAllMessages(batchStatusConsumer, 1);
        assertThat(batchStatusMessages).hasSize(1);
    }

    /**
     * Verifies the JSON payload structure of a {@code DataRequest.ItemsRequested}
     * Kafka message.
     */
    @Test
    @SuppressWarnings("unchecked")
    void shouldProduceDataRequestPayloadWithCorrectStructure() throws Exception {
        String batchId  = nextId();
        String recordId = nextId();

        jdbcTemplate.update(
                "INSERT INTO batch_status (batch_id, reference_date, record_count, fetched_flag) "
                + "VALUES (?, DATE '2024-06-15', 1, 0)",
                batchId);
        jdbcTemplate.update(
                "INSERT INTO data_records (load_id, reference_date, batch_ref_id, entity_id, "
                + "default_flag, duplicate_flag, amount, liability_amount, liability_ts) "
                + "VALUES (?, DATE '2024-06-15', ?, 999001, TRUE, FALSE, 4200.00, 500.00, "
                + "TIMESTAMP '2024-06-15 12:30:00')",
                recordId, batchId);

        // Wait until the data_records row is deleted
        await().atMost(Duration.ofSeconds(20))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    int remaining = jdbcTemplate.queryForObject(
                            "SELECT COUNT(*) FROM data_records WHERE load_id = ?",
                            Integer.class, recordId);
                    assertThat(remaining).isZero();
                });

        List<ConsumerRecord<String, String>> received = pollAllMessages(dataRequestConsumer, 1);
        assertThat(received).hasSize(1);

        // Kafka message key is the entity ID
        assertThat(received.get(0).key()).isEqualTo("999001");

        Map<String, Object> payload = objectMapper.readValue(received.get(0).value(), Map.class);

        assertThat(payload.get("eventType")).isEqualTo("DataRequest.ItemsRequested");

        Map<String, Object> eventData = (Map<String, Object>) payload.get("eventData");
        assertThat(eventData).isNotNull();

        // Static field: company identifier (always serialised as JSON string by the adapter)
        assertThat(eventData.get("companyId")).isEqualTo("1");

        assertThat(((Number) eventData.get("entityId")).longValue()).isEqualTo(999001L);

        Map<String, Object> items = (Map<String, Object>) eventData.get("items");
        assertThat(items).isNotNull();

        assertThat(items.get("referenceDate")).isEqualTo("2024-06-15");
        // batch_ref_id is VARCHAR in the test schema; LONG dataType converts it to a number
        assertThat(((Number) items.get("batchId")).longValue()).isEqualTo(Long.parseLong(batchId));
        assertThat(items.get("defaultFlag")).isEqualTo(true);
        assertThat(items.get("duplicateFlag")).isEqualTo(false);
        assertThat(((Number) items.get("amount")).doubleValue()).isEqualTo(4200.00);
        assertThat(((Number) items.get("liabilityAmount")).doubleValue()).isEqualTo(500.00);
        assertThat(items.get("liabilityTs")).isEqualTo("2024-06-15T12:30:00");
    }

    /**
     * Verifies the JSON payload structure of a {@code BatchStatus.BatchInfoProvided}
     * Kafka message.
     */
    @Test
    @SuppressWarnings("unchecked")
    void shouldProduceBatchStatusPayloadWithCorrectStructure() throws Exception {
        String batchId = nextId();

        // Insert a batch_status row with no data_records – immediately visible to poller 2
        jdbcTemplate.update(
                "INSERT INTO batch_status (batch_id, reference_date, record_count, fetched_flag) "
                + "VALUES (?, DATE '2024-09-30', 42, 0)",
                batchId);

        await().atMost(Duration.ofSeconds(20))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    int fetched = jdbcTemplate.queryForObject(
                            "SELECT fetched_flag FROM batch_status WHERE batch_id = ?",
                            Integer.class, batchId);
                    assertThat(fetched).isEqualTo(1);
                });

        List<ConsumerRecord<String, String>> received = pollAllMessages(batchStatusConsumer, 1);
        assertThat(received).hasSize(1);

        assertThat(received.get(0).key()).isEqualTo(batchId);

        Map<String, Object> payload = objectMapper.readValue(received.get(0).value(), Map.class);

        assertThat(payload.get("eventType")).isEqualTo("BatchStatus.BatchInfoProvided");

        Map<String, Object> eventData = (Map<String, Object>) payload.get("eventData");
        assertThat(eventData).isNotNull();

        assertThat(((Number) eventData.get("recordCount")).longValue()).isEqualTo(42L);
        assertThat(eventData.get("referenceDate")).isEqualTo("2024-09-30");
        // batch_id is VARCHAR in the test schema; LONG dataType converts it to a number
        assertThat(((Number) eventData.get("batchId")).longValue()).isEqualTo(Long.parseLong(batchId));
    }

    /**
     * Verifies that {@code data_records} rows without a matching
     * {@code batch_status} entry are NOT picked up by the poller.
     */
    @Test
    void shouldNotPublishDataRowsWithoutMatchingBatchStatus() {
        String batchId  = nextId();
        String recordId = nextId();

        jdbcTemplate.update(
                "INSERT INTO data_records (load_id, reference_date, batch_ref_id, entity_id) "
                + "VALUES (?, DATE '2024-01-01', ?, 777001)",
                recordId, batchId);

        // Wait a few poll cycles; the row must not be processed
        await().during(Duration.ofSeconds(2))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    int remaining = jdbcTemplate.queryForObject(
                            "SELECT COUNT(*) FROM data_records WHERE load_id = ?",
                            Integer.class, recordId);
                    assertThat(remaining).isEqualTo(1);
                });

        List<ConsumerRecord<String, String>> messages = pollAllMessages(dataRequestConsumer, 1);
        assertThat(messages).isEmpty();
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private List<ConsumerRecord<String, String>> pollAllMessages(
            KafkaConsumer<String, String> consumer, int expectedCount) {
        List<ConsumerRecord<String, String>> all = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 10_000;
        while (all.size() < expectedCount && System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            records.forEach(all::add);
        }
        return all;
    }

    private static KafkaConsumer<String, String> createConsumer(
            String bootstrapServers, String groupIdSuffix) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupIdSuffix + "-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(props);
    }
}
