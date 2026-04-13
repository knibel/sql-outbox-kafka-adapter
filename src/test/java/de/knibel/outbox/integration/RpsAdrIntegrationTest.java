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
 * Integration test that replaces the {@code RpsAdrRepository} /
 * {@code RpsAdrStatusRepository} polling logic with the sql-outbox-kafka-adapter.
 *
 * <p>Two outbox table configurations are exercised simultaneously:
 *
 * <ol>
 *   <li><b>rps_adr</b> – rows are selected via a cross-table EXISTS join with
 *       {@code rps_adr_status}, published as
 *       {@code BerechnungAnfrage.BestandsKunden_anfragen} events to the
 *       {@code berechnung-anfrage} topic, then deleted (CUSTOM acknowledgement).
 *   <li><b>rps_adr_status</b> – rows with {@code daten_abgeholt_jn = 0} and no
 *       remaining {@code rps_adr} rows for the same batch are published as
 *       {@code BatchlaufStatus.BatchlaufinfoBereitgestellt} events to the
 *       {@code batchlauf-status} topic, then acknowledged by setting
 *       {@code daten_abgeholt_jn = 1} and {@code daten_abholung_dat = now}
 *       (CUSTOM acknowledgement).
 * </ol>
 *
 * <p>The combined end-to-end flow:
 * <pre>
 *   INSERT rps_adr_status (batch ready, no data fetched yet)
 *   INSERT rps_adr rows   (data for the batch)
 *       ↓
 *   Poller 1 picks up rps_adr rows (EXISTS match) → publishes BerechnungAnfrage
 *   → DELETE rps_adr rows
 *       ↓
 *   Poller 2 picks up rps_adr_status (NOT EXISTS satisfied) → publishes
 *   BatchlaufinfoBereitgestellt → DATEN_ABGEHOLT_JN = 1
 * </pre>
 *
 * <p>See {@code docs/examples/rps-adr/application.yml} for the equivalent
 * production configuration (Oracle column names in uppercase).
 */
@SpringBootTest(
        classes = Application.class,
        properties = {
                // ── Table 0: rps_adr ─────────────────────────────────────────────────
                "outbox.tables[0].tableName=rps_adr",
                "outbox.tables[0].idColumn=ext_load_id",
                "outbox.tables[0].keyColumn=bk_adr_nummer",
                "outbox.tables[0].staticTopic=berechnung-anfrage",
                "outbox.tables[0].acknowledgementStrategy=CUSTOM",
                "outbox.tables[0].customAcknowledgementQuery="
                        + "DELETE FROM rps_adr WHERE ext_load_id = ?",
                "outbox.tables[0].customQuery="
                        + "SELECT * FROM rps_adr t1 WHERE EXISTS ("
                        + "SELECT 1 FROM rps_adr_status t2 "
                        + "WHERE t2.stichtag_dat = t1.bk_stichtag_dat "
                        + "AND t2.dwh_job_id_batch = t1.ext_job_id)",
                "outbox.tables[0].rowMappingStrategy=CUSTOM",
                "outbox.tables[0].fieldMappings.bk_adr_nummer.name=ereignisdaten.kundeId",
                "outbox.tables[0].fieldMappings.bk_adr_nummer.dataType=LONG",
                "outbox.tables[0].fieldMappings.bk_stichtag_dat.name=ereignisdaten.bestandsKunden.stichtag",
                "outbox.tables[0].fieldMappings.bk_stichtag_dat.dataType=DATE",
                "outbox.tables[0].fieldMappings.bk_stichtag_dat.format=yyyy-MM-dd",
                "outbox.tables[0].fieldMappings.ext_job_id.name=ereignisdaten.bestandsKunden.ext_job_id",
                "outbox.tables[0].fieldMappings.ext_job_id.dataType=LONG",
                "outbox.tables[0].fieldMappings.kunde_ausgefallen_knz.name=ereignisdaten.bestandsKunden.ausfallflag",
                "outbox.tables[0].fieldMappings.kunde_ausgefallen_knz.dataType=BOOLEAN",
                "outbox.tables[0].fieldMappings.doppelkunde_knz.name=ereignisdaten.bestandsKunden.doppelkunde",
                "outbox.tables[0].fieldMappings.doppelkunde_knz.dataType=BOOLEAN",
                "outbox.tables[0].fieldMappings.umsatz_btr.name=ereignisdaten.bestandsKunden.umsatz",
                "outbox.tables[0].fieldMappings.umsatz_btr.dataType=DECIMAL",
                "outbox.tables[0].fieldMappings.kne_obligo_btr.name=ereignisdaten.bestandsKunden.summe",
                "outbox.tables[0].fieldMappings.kne_obligo_btr.dataType=DECIMAL",
                "outbox.tables[0].fieldMappings.kne_obligo_ts.name=ereignisdaten.bestandsKunden.summe_vom",
                "outbox.tables[0].fieldMappings.kne_obligo_ts.dataType=DATETIME",
                "outbox.tables[0].fieldMappings.kne_obligo_ts.format=yyyy-MM-dd'T'HH:mm:ss",
                "outbox.tables[0].staticFields.ereignistyp=BerechnungAnfrage.BestandsKunden_anfragen",
                "outbox.tables[0].staticFields.ereignisdaten.firma=1",
                "outbox.tables[0].pollIntervalMs=200",
                "outbox.tables[0].batchSize=10",

                // ── Table 1: rps_adr_status ──────────────────────────────────────────
                "outbox.tables[1].tableName=rps_adr_status",
                "outbox.tables[1].idColumn=dwh_job_id_batch",
                "outbox.tables[1].keyColumn=dwh_job_id_batch",
                "outbox.tables[1].staticTopic=batchlauf-status",
                "outbox.tables[1].acknowledgementStrategy=CUSTOM",
                "outbox.tables[1].customAcknowledgementQuery="
                        + "UPDATE rps_adr_status "
                        + "SET daten_abgeholt_jn = 1, daten_abholung_dat = CURRENT_TIMESTAMP "
                        + "WHERE dwh_job_id_batch = ?",
                "outbox.tables[1].customQuery="
                        + "SELECT * FROM rps_adr_status t1 "
                        + "WHERE t1.daten_abgeholt_jn = 0 "
                        + "AND NOT EXISTS ("
                        + "SELECT 1 FROM rps_adr t2 "
                        + "WHERE t2.bk_stichtag_dat = t1.stichtag_dat "
                        + "AND t2.ext_job_id = t1.dwh_job_id_batch)",
                "outbox.tables[1].rowMappingStrategy=CUSTOM",
                "outbox.tables[1].fieldMappings.anz_adr.name=ereignisdaten.anzahlEintraege",
                "outbox.tables[1].fieldMappings.anz_adr.dataType=LONG",
                "outbox.tables[1].fieldMappings.stichtag_dat.name=ereignisdaten.stichtag",
                "outbox.tables[1].fieldMappings.stichtag_dat.dataType=DATE",
                "outbox.tables[1].fieldMappings.stichtag_dat.format=yyyy-MM-dd",
                "outbox.tables[1].fieldMappings.dwh_job_id_batch.name=ereignisdaten.extJobId",
                "outbox.tables[1].fieldMappings.dwh_job_id_batch.dataType=LONG",
                "outbox.tables[1].staticFields.ereignistyp=BatchlaufStatus.BatchlaufinfoBereitgestellt",
                "outbox.tables[1].pollIntervalMs=200",
                "outbox.tables[1].batchSize=10",
        }
)
@Testcontainers
class RpsAdrIntegrationTest {

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
                    new NewTopic("berechnung-anfrage", 1, (short) 1),
                    new NewTopic("batchlauf-status",   1, (short) 1)
            )).all().get(10, TimeUnit.SECONDS);
        }
    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    ObjectMapper objectMapper;

    KafkaConsumer<String, String> berechnungConsumer;
    KafkaConsumer<String, String> batchlaufConsumer;

    @BeforeEach
    void setUp() {
        jdbcTemplate.execute("TRUNCATE TABLE rps_adr");
        jdbcTemplate.execute("TRUNCATE TABLE rps_adr_status");

        berechnungConsumer = createConsumer(kafka.getBootstrapServers(), "berechnung-anfrage-consumer");
        berechnungConsumer.subscribe(Collections.singletonList("berechnung-anfrage"));
        berechnungConsumer.poll(Duration.ofMillis(500));

        batchlaufConsumer = createConsumer(kafka.getBootstrapServers(), "batchlauf-status-consumer");
        batchlaufConsumer.subscribe(Collections.singletonList("batchlauf-status"));
        batchlaufConsumer.poll(Duration.ofMillis(500));
    }

    @AfterEach
    void tearDown() {
        berechnungConsumer.close();
        batchlaufConsumer.close();
    }

    /**
     * Full end-to-end scenario:
     * <ol>
     *   <li>Insert one {@code rps_adr_status} row and two {@code rps_adr} rows
     *       for the same batch.
     *   <li>Wait for both {@code rps_adr} rows to be published and deleted.
     *   <li>Wait for the {@code rps_adr_status} row to be published and
     *       acknowledged.
     * </ol>
     */
    @Test
    void shouldProcessAdrRowsThenPublishBatchlaufStatus() {
        String batchId = nextId();
        String adrId1  = nextId();
        String adrId2  = nextId();

        // Insert status row – not yet collected, two ADR records expected
        jdbcTemplate.update(
                "INSERT INTO rps_adr_status (dwh_job_id_batch, stichtag_dat, anz_adr, daten_abgeholt_jn) "
                + "VALUES (?, DATE '2024-03-01', 2, 0)",
                batchId);

        // Insert two ADR records that match the status row (same batch + stichtag)
        jdbcTemplate.update(
                "INSERT INTO rps_adr (ext_load_id, bk_stichtag_dat, ext_job_id, bk_adr_nummer, "
                + "umsatz_btr, kne_obligo_btr, kne_obligo_ts) "
                + "VALUES (?, DATE '2024-03-01', ?, 100001, 1500.00, 200.00, "
                + "TIMESTAMP '2024-03-01 08:00:00')",
                adrId1, batchId);
        jdbcTemplate.update(
                "INSERT INTO rps_adr (ext_load_id, bk_stichtag_dat, ext_job_id, bk_adr_nummer, "
                + "umsatz_btr, kne_obligo_btr, kne_obligo_ts) "
                + "VALUES (?, DATE '2024-03-01', ?, 100002, 2500.00, 300.00, "
                + "TIMESTAMP '2024-03-01 09:00:00')",
                adrId2, batchId);

        // Both ADR rows must be deleted (published + acknowledged)
        await().atMost(Duration.ofSeconds(20))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    int remaining = jdbcTemplate.queryForObject(
                            "SELECT COUNT(*) FROM rps_adr WHERE ext_job_id = ?",
                            Integer.class, batchId);
                    assertThat(remaining).isZero();
                });

        // Exactly two BerechnungAnfrage messages must have arrived
        List<ConsumerRecord<String, String>> berechnungMessages = pollAllMessages(berechnungConsumer, 2);
        assertThat(berechnungMessages).hasSize(2);

        // The STATUS row must be acknowledged
        await().atMost(Duration.ofSeconds(20))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    int acked = jdbcTemplate.queryForObject(
                            "SELECT daten_abgeholt_jn FROM rps_adr_status WHERE dwh_job_id_batch = ?",
                            Integer.class, batchId);
                    assertThat(acked).isEqualTo(1);
                });

        // daten_abholung_dat must have been written
        int withTimestamp = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM rps_adr_status "
                + "WHERE dwh_job_id_batch = ? AND daten_abholung_dat IS NOT NULL",
                Integer.class, batchId);
        assertThat(withTimestamp).isEqualTo(1);

        // Exactly one BatchlaufinfoBereitgestellt message must have arrived
        List<ConsumerRecord<String, String>> batchlaufMessages = pollAllMessages(batchlaufConsumer, 1);
        assertThat(batchlaufMessages).hasSize(1);
    }

    /**
     * Verifies the JSON payload structure of a {@code BerechnungAnfrage}
     * Kafka message.
     */
    @Test
    @SuppressWarnings("unchecked")
    void shouldProduceBerechnungAnfragePayloadWithCorrectStructure() throws Exception {
        String batchId = nextId();
        String adrId   = nextId();

        jdbcTemplate.update(
                "INSERT INTO rps_adr_status (dwh_job_id_batch, stichtag_dat, anz_adr, daten_abgeholt_jn) "
                + "VALUES (?, DATE '2024-06-15', 1, 0)",
                batchId);
        jdbcTemplate.update(
                "INSERT INTO rps_adr (ext_load_id, bk_stichtag_dat, ext_job_id, bk_adr_nummer, "
                + "kunde_ausgefallen_knz, doppelkunde_knz, umsatz_btr, kne_obligo_btr, kne_obligo_ts) "
                + "VALUES (?, DATE '2024-06-15', ?, 999001, TRUE, FALSE, 4200.00, 500.00, "
                + "TIMESTAMP '2024-06-15 12:30:00')",
                adrId, batchId);

        // Wait until the ADR row is deleted
        await().atMost(Duration.ofSeconds(20))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    int remaining = jdbcTemplate.queryForObject(
                            "SELECT COUNT(*) FROM rps_adr WHERE ext_load_id = ?",
                            Integer.class, adrId);
                    assertThat(remaining).isZero();
                });

        List<ConsumerRecord<String, String>> received = pollAllMessages(berechnungConsumer, 1);
        assertThat(received).hasSize(1);

        // Kafka message key is the customer number
        assertThat(received.get(0).key()).isEqualTo("999001");

        Map<String, Object> payload = objectMapper.readValue(received.get(0).value(), Map.class);

        // Static field: event type discriminator
        assertThat(payload.get("ereignistyp"))
                .isEqualTo("BerechnungAnfrage.BestandsKunden_anfragen");

        Map<String, Object> ereignisdaten = (Map<String, Object>) payload.get("ereignisdaten");
        assertThat(ereignisdaten).isNotNull();

        // Static field: company identifier (always serialised as JSON string by the adapter)
        assertThat(ereignisdaten.get("firma")).isEqualTo("1");

        // Column-derived field: customer ID
        assertThat(((Number) ereignisdaten.get("kundeId")).longValue()).isEqualTo(999001L);

        Map<String, Object> bestandsKunden = (Map<String, Object>) ereignisdaten.get("bestandsKunden");
        assertThat(bestandsKunden).isNotNull();

        assertThat(bestandsKunden.get("stichtag")).isEqualTo("2024-06-15");
        // ext_job_id is VARCHAR in the test schema; LONG dataType converts it to a number
        assertThat(((Number) bestandsKunden.get("ext_job_id")).longValue())
                .isEqualTo(Long.parseLong(batchId));
        assertThat(bestandsKunden.get("ausfallflag")).isEqualTo(true);
        assertThat(bestandsKunden.get("doppelkunde")).isEqualTo(false);
        assertThat(((Number) bestandsKunden.get("umsatz")).doubleValue()).isEqualTo(4200.00);
        assertThat(((Number) bestandsKunden.get("summe")).doubleValue()).isEqualTo(500.00);
        assertThat(bestandsKunden.get("summe_vom")).isEqualTo("2024-06-15T12:30:00");
    }

    /**
     * Verifies the JSON payload structure of a
     * {@code BatchlaufinfoBereitgestellt} Kafka message.
     */
    @Test
    @SuppressWarnings("unchecked")
    void shouldProduceBatchlaufStatusPayloadWithCorrectStructure() throws Exception {
        String batchId = nextId();

        // Insert a STATUS row with no ADR rows – immediately visible to poller 2
        jdbcTemplate.update(
                "INSERT INTO rps_adr_status (dwh_job_id_batch, stichtag_dat, anz_adr, daten_abgeholt_jn) "
                + "VALUES (?, DATE '2024-09-30', 42, 0)",
                batchId);

        // Wait until STATUS row is acknowledged
        await().atMost(Duration.ofSeconds(20))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    int acked = jdbcTemplate.queryForObject(
                            "SELECT daten_abgeholt_jn FROM rps_adr_status WHERE dwh_job_id_batch = ?",
                            Integer.class, batchId);
                    assertThat(acked).isEqualTo(1);
                });

        List<ConsumerRecord<String, String>> received = pollAllMessages(batchlaufConsumer, 1);
        assertThat(received).hasSize(1);

        // Kafka message key is the batch job ID
        assertThat(received.get(0).key()).isEqualTo(batchId);

        Map<String, Object> payload = objectMapper.readValue(received.get(0).value(), Map.class);

        // Static field: event type discriminator
        assertThat(payload.get("ereignistyp"))
                .isEqualTo("BatchlaufStatus.BatchlaufinfoBereitgestellt");

        Map<String, Object> ereignisdaten = (Map<String, Object>) payload.get("ereignisdaten");
        assertThat(ereignisdaten).isNotNull();

        assertThat(((Number) ereignisdaten.get("anzahlEintraege")).longValue()).isEqualTo(42L);
        assertThat(ereignisdaten.get("stichtag")).isEqualTo("2024-09-30");
        // dwh_job_id_batch is VARCHAR; LONG dataType converts it to a number
        assertThat(((Number) ereignisdaten.get("extJobId")).longValue())
                .isEqualTo(Long.parseLong(batchId));
    }

    /**
     * Verifies that {@code rps_adr} rows without a matching
     * {@code rps_adr_status} entry are NOT picked up by the poller.
     */
    @Test
    void shouldNotPublishAdrRowsWithoutMatchingStatus() {
        String batchId = nextId();
        String adrId   = nextId();

        // Insert ADR row – no STATUS entry → EXISTS condition is false
        jdbcTemplate.update(
                "INSERT INTO rps_adr (ext_load_id, bk_stichtag_dat, ext_job_id, bk_adr_nummer) "
                + "VALUES (?, DATE '2024-01-01', ?, 777001)",
                adrId, batchId);

        // Wait a few poll cycles; the row must not be processed
        await().during(Duration.ofSeconds(2))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    int remaining = jdbcTemplate.queryForObject(
                            "SELECT COUNT(*) FROM rps_adr WHERE ext_load_id = ?",
                            Integer.class, adrId);
                    assertThat(remaining).isEqualTo(1);
                });

        // No BerechnungAnfrage messages should have been published
        List<ConsumerRecord<String, String>> messages = pollAllMessages(berechnungConsumer, 1);
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
