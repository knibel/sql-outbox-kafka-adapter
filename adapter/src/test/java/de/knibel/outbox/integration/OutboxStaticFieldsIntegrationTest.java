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
 * Integration test for static value injection via the unified {@code mappings} DSL.
 *
 * <p>Verifies that constant values configured as mapping rules (with
 * {@code value} and no {@code source}) are injected into every JSON payload
 * sent to Kafka, including nested paths.
 */
@SpringBootTest(
        classes = Application.class,
        properties = {
                "outbox.tables[0].tableName=static_fields_outbox",
                "outbox.tables[0].idColumn=id",
                "outbox.tables[0].staticTopic=static-fields-topic",
                "outbox.tables[0].statusColumn=status",
                "outbox.tables[0].pendingValue=PENDING",
                "outbox.tables[0].doneValue=DONE",
                // Column mapping rules
                "outbox.tables[0].mappings[0].source=order_id",
                "outbox.tables[0].mappings[0].target=orderId",
                "outbox.tables[0].mappings[1].source=customer_name",
                "outbox.tables[0].mappings[1].target=customer.name",
                "outbox.tables[0].mappings[2].source=amount",
                "outbox.tables[0].mappings[2].target=amount",
                "outbox.tables[0].mappings[2].dataType=DOUBLE",
                // Static value rules: a flat constant and a nested constant
                "outbox.tables[0].mappings[3].target=ereignistyp",
                "outbox.tables[0].mappings[3].value=Kundenanfrage.BestandsKunden_anfragen",
                "outbox.tables[0].mappings[4].target=meta.source",
                "outbox.tables[0].mappings[4].value=outbox-adapter",
                "outbox.tables[0].pollIntervalMs=200",
                "outbox.tables[0].batchSize=10",
        }
)
@Testcontainers
class OutboxStaticFieldsIntegrationTest {

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
            admin.createTopics(List.of(new NewTopic("static-fields-topic", 1, (short) 1)))
                    .all()
                    .get(10, TimeUnit.SECONDS);
        }
    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    ObjectMapper objectMapper;

    KafkaConsumer<String, String> consumer;

    @BeforeEach
    void setUp() {
        jdbcTemplate.execute("TRUNCATE TABLE static_fields_outbox");

        consumer = createConsumer(kafka.getBootstrapServers());
        consumer.subscribe(Collections.singletonList("static-fields-topic"));
        consumer.poll(Duration.ofMillis(500));
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldInjectStaticFieldsIntoPayload() throws Exception {
        String id = UUID.randomUUID().toString();
        jdbcTemplate.update(
                "INSERT INTO static_fields_outbox (id, order_id, customer_name, amount, status) "
                + "VALUES (?, ?, ?, ?, 'PENDING')",
                id, "ORD-42", "Alice", 123.45);

        // Wait until the row is marked DONE.
        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    String status = jdbcTemplate.queryForObject(
                            "SELECT status FROM static_fields_outbox WHERE id = ?",
                            String.class, id);
                    assertThat(status).isEqualTo("DONE");
                });

        // Verify the Kafka message
        List<ConsumerRecord<String, String>> received = pollAllMessages(1);
        assertThat(received).hasSize(1);

        Map<String, Object> payload = objectMapper.readValue(received.get(0).value(), Map.class);

        // Column-derived fields
        assertThat(payload.get("orderId")).isEqualTo("ORD-42");
        assertThat(((Number) payload.get("amount")).doubleValue()).isEqualTo(123.45);

        Map<String, Object> customer = (Map<String, Object>) payload.get("customer");
        assertThat(customer).isNotNull();
        assertThat(customer.get("name")).isEqualTo("Alice");

        // Static fields
        assertThat(payload.get("ereignistyp")).isEqualTo("Kundenanfrage.BestandsKunden_anfragen");

        Map<String, Object> meta = (Map<String, Object>) payload.get("meta");
        assertThat(meta).isNotNull();
        assertThat(meta.get("source")).isEqualTo("outbox-adapter");
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldInjectStaticFieldsForEveryRow() throws Exception {
        int rowCount = 3;
        for (int i = 0; i < rowCount; i++) {
            String id = UUID.randomUUID().toString();
            jdbcTemplate.update(
                    "INSERT INTO static_fields_outbox (id, order_id, customer_name, amount, status) "
                    + "VALUES (?, ?, ?, ?, 'PENDING')",
                    id, "ORD-" + i, "Customer-" + i, 10.0 + i);
        }

        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    int doneCount = jdbcTemplate.queryForObject(
                            "SELECT COUNT(*) FROM static_fields_outbox WHERE status = 'DONE'",
                            Integer.class);
                    assertThat(doneCount).isEqualTo(rowCount);
                });

        List<ConsumerRecord<String, String>> received = pollAllMessages(rowCount);
        assertThat(received).hasSize(rowCount);

        for (ConsumerRecord<String, String> record : received) {
            Map<String, Object> payload = objectMapper.readValue(record.value(), Map.class);
            assertThat(payload.get("ereignistyp")).isEqualTo("Kundenanfrage.BestandsKunden_anfragen");
            Map<String, Object> meta = (Map<String, Object>) payload.get("meta");
            assertThat(meta).isNotNull();
            assertThat(meta.get("source")).isEqualTo("outbox-adapter");
        }
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
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "static-fields-test-consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(props);
    }
}
