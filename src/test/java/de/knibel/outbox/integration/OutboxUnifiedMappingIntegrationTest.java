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
 * Integration test verifying the unified {@code mappings} DSL.
 *
 * <p>Exercises all mapping rule types in a single configuration:
 * <ul>
 *   <li>Explicit column mapping with dataType and format</li>
 *   <li>Explicit column mapping with valueMappings</li>
 *   <li>Regex pattern mapping (replaces columnPatterns)</li>
 *   <li>Group/list mapping (replaces listMappings)</li>
 *   <li>Static value injection (replaces staticFields)</li>
 * </ul>
 *
 * <p>Uses the {@code product_audit} table which has {@code new_*}/{@code old_*}
 * columns ideal for demonstrating regex and group rules.
 */
@SpringBootTest(
        classes = Application.class,
        properties = {
                "outbox.tables[0].tableName=product_audit",
                "outbox.tables[0].idColumn=audit_id",
                "outbox.tables[0].keyColumn=product_key",
                "outbox.tables[0].staticTopic=unified-mapping-topic",
                "outbox.tables[0].statusColumn=status",
                "outbox.tables[0].pendingValue=PENDING",
                "outbox.tables[0].doneValue=DONE",
                "outbox.tables[0].pollIntervalMs=200",
                "outbox.tables[0].batchSize=10",

                // ── Unified mappings DSL ──

                // ① Explicit column mapping with dataType + format
                "outbox.tables[0].mappings[0].source=audit_ts",
                "outbox.tables[0].mappings[0].target=timestamp",
                "outbox.tables[0].mappings[0].dataType=DATETIME",
                "outbox.tables[0].mappings[0].format=yyyy-MM-dd'T'HH:mm:ss",

                // ② Explicit column mapping with valueMappings
                "outbox.tables[0].mappings[1].source=action",
                "outbox.tables[0].mappings[1].target=action",
                "outbox.tables[0].mappings[1].valueMappings.I=INSERT",
                "outbox.tables[0].mappings[1].valueMappings.U=UPDATE",
                "outbox.tables[0].mappings[1].valueMappings.D=DELETE",

                // ③ Explicit column mapping (simple)
                "outbox.tables[0].mappings[2].source=product_key",
                "outbox.tables[0].mappings[2].target=productKey",

                // ④ Group/list mapping: new_* columns → modifications[].after
                "outbox.tables[0].mappings[3].source=/new_(.*)/",
                "outbox.tables[0].mappings[3].target=modifications",
                "outbox.tables[0].mappings[3].group.by=$1",
                "outbox.tables[0].mappings[3].group.keyProperty=attribute",
                "outbox.tables[0].mappings[3].group.property=after",

                // ⑤ Group/list mapping: old_* columns → modifications[].before
                "outbox.tables[0].mappings[4].source=/old_(.*)/",
                "outbox.tables[0].mappings[4].target=modifications",
                "outbox.tables[0].mappings[4].group.by=$1",
                "outbox.tables[0].mappings[4].group.keyProperty=attribute",
                "outbox.tables[0].mappings[4].group.property=before",

                // ⑥ Static value injection
                "outbox.tables[0].mappings[5].target=eventType",
                "outbox.tables[0].mappings[5].value=ProductAudit.Changed",
        }
)
@Testcontainers
class OutboxUnifiedMappingIntegrationTest {

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
            admin.createTopics(List.of(new NewTopic("unified-mapping-topic", 1, (short) 1)))
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
        consumer.subscribe(Collections.singletonList("unified-mapping-topic"));
        consumer.poll(Duration.ofMillis(500));
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldPublishWithAllMappingRuleTypes() throws Exception {
        String id = UUID.randomUUID().toString();
        Timestamp ts = Timestamp.valueOf(LocalDateTime.of(2024, 3, 15, 10, 30, 0));
        jdbcTemplate.update(
                "INSERT INTO product_audit "
                + "(audit_id, audit_ts, action, product_key, "
                + "new_price, old_price, new_stock, old_stock, new_label, old_label, status) "
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

        // Verify the Kafka message.
        List<ConsumerRecord<String, String>> received = pollAllMessages(1);
        assertThat(received).hasSize(1);

        // Key comes from keyColumn=product_key
        assertThat(received.get(0).key()).isEqualTo("SKU-42");

        Map<String, Object> payload = objectMapper.readValue(received.get(0).value(), Map.class);

        // ① Explicit mapping with DATETIME format: audit_ts → timestamp
        assertThat(payload.get("timestamp")).isEqualTo("2024-03-15T10:30:00");

        // ② Explicit mapping with valueMappings: action 'U' → "UPDATE"
        assertThat(payload.get("action")).isEqualTo("UPDATE");

        // ③ Explicit mapping: product_key → productKey
        assertThat(payload.get("productKey")).isEqualTo("SKU-42");

        // ④⑤ Group/list mapping: new_*/old_* → modifications array
        List<Map<String, Object>> modifications =
                (List<Map<String, Object>>) payload.get("modifications");
        assertThat(modifications).isNotNull().hasSize(3);

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

        // ⑥ Static value: eventType
        assertThat(payload.get("eventType")).isEqualTo("ProductAudit.Changed");
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
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "unified-mapping-test-consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(props);
    }
}
