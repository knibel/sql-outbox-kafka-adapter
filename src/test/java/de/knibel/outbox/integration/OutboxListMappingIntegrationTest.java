package de.knibel.outbox.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.knibel.outbox.Application;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
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
 * Integration test for the {@code listMappings} feature.
 *
 * <p>Verifies that paired {@code new_*}/{@code old_*} columns are collected
 * into a JSON array where each unique suffix produces one array element
 * containing the configured key property, "after" value, and "before" value.
 *
 * <p>The scenario simulates a product audit trail table where each row
 * records changes to multiple attributes (price, stock, label).
 */
@SpringBootTest(
        classes = Application.class,
        properties = {
                "outbox.tables[0].tableName=product_audit",
                "outbox.tables[0].idColumn=audit_id",
                "outbox.tables[0].keyColumn=product_key",
                "outbox.tables[0].staticTopic=product-audit-topic",
                "outbox.tables[0].statusColumn=status",
                "outbox.tables[0].pendingValue=PENDING",
                "outbox.tables[0].doneValue=DONE",
                "outbox.tables[0].rowMappingStrategy=CUSTOM",
                // Fixed columns mapped with fieldMappings
                "outbox.tables[0].fieldMappings.audit_ts.name=timestamp",
                "outbox.tables[0].fieldMappings.audit_ts.dataType=DATETIME",
                "outbox.tables[0].fieldMappings.audit_ts.format=yyyy-MM-dd'T'HH:mm:ss",
                "outbox.tables[0].fieldMappings.action.name=action",
                "outbox.tables[0].fieldMappings.action.valueMappings.I=INSERT",
                "outbox.tables[0].fieldMappings.action.valueMappings.U=UPDATE",
                "outbox.tables[0].fieldMappings.action.valueMappings.D=DELETE",
                "outbox.tables[0].fieldMappings.product_key.name=productKey",
                // Paired columns collected into list via listMappings
                "outbox.tables[0].listMappings.modifications.keyProperty=attribute",
                "outbox.tables[0].listMappings.modifications.patterns[new_(.*)].name=after",
                "outbox.tables[0].listMappings.modifications.patterns[old_(.*)].name=before",
                "outbox.tables[0].pollIntervalMs=200",
                "outbox.tables[0].batchSize=10",
        }
)
@Testcontainers
class OutboxListMappingIntegrationTest {

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
            admin.createTopics(List.of(new NewTopic("product-audit-topic", 1, (short) 1)))
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
        jdbcTemplate.execute("TRUNCATE TABLE product_audit");

        consumer = createConsumer(kafka.getBootstrapServers());
        consumer.subscribe(Collections.singletonList("product-audit-topic"));
        consumer.poll(Duration.ofMillis(500));
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldProduceListFromPairedColumns() throws Exception {
        String id = UUID.randomUUID().toString();
        Timestamp ts = Timestamp.valueOf(LocalDateTime.of(2024, 3, 15, 10, 30, 0));
        jdbcTemplate.update(
                "INSERT INTO product_audit "
                + "(audit_id, audit_ts, action, product_key, new_price, old_price, new_stock, old_stock, new_label, old_label, status) "
                + "VALUES (?, ?, 'U', ?, ?, ?, ?, ?, ?, ?, 'PENDING')",
                id, ts, "SKU-42", 29.99, 24.99, 150, 100, "Widget Pro", "Widget");

        // Wait until the row is marked DONE.
        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    String status = jdbcTemplate.queryForObject(
                            "SELECT status FROM product_audit WHERE audit_id = ?",
                            String.class, id);
                    assertThat(status).isEqualTo("DONE");
                });

        // Verify the Kafka message
        List<ConsumerRecord<String, String>> received = pollAllMessages(1);
        assertThat(received).hasSize(1);

        // Verify the key is the product_key
        assertThat(received.get(0).key()).isEqualTo("SKU-42");

        Map<String, Object> payload = objectMapper.readValue(received.get(0).value(), Map.class);

        // Fixed field mappings
        assertThat(payload.get("timestamp")).isEqualTo("2024-03-15T10:30:00");
        assertThat(payload.get("action")).isEqualTo("UPDATE");
        assertThat(payload.get("productKey")).isEqualTo("SKU-42");

        // List mapping: modifications array
        List<Map<String, Object>> modifications = (List<Map<String, Object>>) payload.get("modifications");
        assertThat(modifications).isNotNull().hasSize(3);

        // Verify each element contains attribute, after, before
        Map<String, Map<String, Object>> byAttribute = new HashMap<>();
        for (Map<String, Object> mod : modifications) {
            byAttribute.put((String) mod.get("attribute"), mod);
        }

        assertThat(byAttribute).containsKeys("price", "stock", "label");

        Map<String, Object> priceChange = byAttribute.get("price");
        assertThat(((Number) priceChange.get("after")).doubleValue()).isEqualTo(29.99);
        assertThat(((Number) priceChange.get("before")).doubleValue()).isEqualTo(24.99);

        Map<String, Object> stockChange = byAttribute.get("stock");
        assertThat(stockChange.get("after")).isEqualTo(150);
        assertThat(stockChange.get("before")).isEqualTo(100);

        Map<String, Object> labelChange = byAttribute.get("label");
        assertThat(labelChange.get("after")).isEqualTo("Widget Pro");
        assertThat(labelChange.get("before")).isEqualTo("Widget");
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldHandleNullValuesInListMapping() throws Exception {
        String id = UUID.randomUUID().toString();
        Timestamp ts = Timestamp.valueOf(LocalDateTime.of(2024, 6, 1, 8, 0, 0));
        // INSERT action: old values are null
        jdbcTemplate.update(
                "INSERT INTO product_audit "
                + "(audit_id, audit_ts, action, product_key, new_price, old_price, new_stock, old_stock, status) "
                + "VALUES (?, ?, 'I', ?, ?, NULL, ?, NULL, 'PENDING')",
                id, ts, "SKU-99", 19.99, 50);

        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    String status = jdbcTemplate.queryForObject(
                            "SELECT status FROM product_audit WHERE audit_id = ?",
                            String.class, id);
                    assertThat(status).isEqualTo("DONE");
                });

        List<ConsumerRecord<String, String>> received = pollAllMessages(1);
        assertThat(received).hasSize(1);

        Map<String, Object> payload = objectMapper.readValue(received.get(0).value(), Map.class);
        assertThat(payload.get("action")).isEqualTo("INSERT");

        List<Map<String, Object>> modifications = (List<Map<String, Object>>) payload.get("modifications");
        assertThat(modifications).isNotNull();

        // All columns are present (including nulls), so entries still appear
        Map<String, Map<String, Object>> byAttribute = new HashMap<>();
        for (Map<String, Object> mod : modifications) {
            byAttribute.put((String) mod.get("attribute"), mod);
        }

        assertThat(byAttribute.get("price").get("after")).isNotNull();
        assertThat(byAttribute.get("price").get("before")).isNull();
        assertThat(byAttribute.get("stock").get("after")).isNotNull();
        assertThat(byAttribute.get("stock").get("before")).isNull();
    }

    @Test
    void shouldProcessMultipleRowsWithListMapping() {
        int rowCount = 5;
        for (int i = 0; i < rowCount; i++) {
            String id = UUID.randomUUID().toString();
            Timestamp ts = Timestamp.valueOf(LocalDateTime.of(2024, 1, 1 + i, 12, 0, 0));
            jdbcTemplate.update(
                    "INSERT INTO product_audit "
                    + "(audit_id, audit_ts, action, product_key, new_price, old_price, new_stock, old_stock, status) "
                    + "VALUES (?, ?, 'U', ?, ?, ?, ?, ?, 'PENDING')",
                    id, ts, "SKU-" + i, 10.0 + i, 5.0 + i, 100 + i, 50 + i);
        }

        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    int doneCount = jdbcTemplate.queryForObject(
                            "SELECT COUNT(*) FROM product_audit WHERE status = 'DONE'",
                            Integer.class);
                    assertThat(doneCount).isEqualTo(rowCount);
                });

        List<ConsumerRecord<String, String>> received = pollAllMessages(rowCount);
        assertThat(received).hasSize(rowCount);
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
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "list-mapping-test-consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(props);
    }
}
