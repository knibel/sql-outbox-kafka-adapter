package com.github.knibel.outbox.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
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
 * Integration test verifying the {@code CUSTOM} row mapping strategy with
 * date/datetime formatting and integer-to-enum value mapping.
 *
 * <p>The field mappings include:
 * <ul>
 *   <li>{@code order_id → orderId} (dataType: STRING)</li>
 *   <li>{@code created_at → createdAt} (dataType: DATETIME, format: yyyy-MM-dd'T'HH:mm:ss)</li>
 *   <li>{@code order_date → orderDate} (dataType: DATE, format: yyyy-MM-dd)</li>
 *   <li>{@code status_code → orderStatus} (valueMappings: 1→ACTIVE, 2→INACTIVE, 3→DELETED)</li>
 *   <li>{@code customer_name → customer.name} (no dataType – uses DB default)</li>
 * </ul>
 */
@SpringBootTest(
        classes = Application.class,
        properties = {
                "outbox.tables[0].tableName=custom_mapping_outbox",
                "outbox.tables[0].idColumn=id",
                "outbox.tables[0].staticTopic=custom-datetime-valuemap-topic",
                "outbox.tables[0].statusColumn=status",
                "outbox.tables[0].pendingValue=PENDING",
                "outbox.tables[0].doneValue=DONE",
                "outbox.tables[0].rowMappingStrategy=CUSTOM",
                "outbox.tables[0].fieldMappings.order_id.name=orderId",
                "outbox.tables[0].fieldMappings.order_id.dataType=STRING",
                "outbox.tables[0].fieldMappings.created_at.name=createdAt",
                "outbox.tables[0].fieldMappings.created_at.dataType=DATETIME",
                "outbox.tables[0].fieldMappings.created_at.format=yyyy-MM-dd'T'HH:mm:ss",
                "outbox.tables[0].fieldMappings.order_date.name=orderDate",
                "outbox.tables[0].fieldMappings.order_date.dataType=DATE",
                "outbox.tables[0].fieldMappings.order_date.format=yyyy-MM-dd",
                "outbox.tables[0].fieldMappings.status_code.name=orderStatus",
                "outbox.tables[0].fieldMappings.status_code.valueMappings.1=ACTIVE",
                "outbox.tables[0].fieldMappings.status_code.valueMappings.2=INACTIVE",
                "outbox.tables[0].fieldMappings.status_code.valueMappings.3=DELETED",
                "outbox.tables[0].fieldMappings.customer_name.name=customer.name",
                "outbox.tables[0].pollIntervalMs=200",
                "outbox.tables[0].batchSize=10",
        }
)
@Testcontainers
class OutboxCustomDateTimeValueMapIntegrationTest {

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
            admin.createTopics(List.of(new NewTopic("custom-datetime-valuemap-topic", 1, (short) 1)))
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
        jdbcTemplate.execute("TRUNCATE TABLE custom_mapping_outbox");

        consumer = createConsumer(kafka.getBootstrapServers());
        consumer.subscribe(Collections.singletonList("custom-datetime-valuemap-topic"));
        consumer.poll(Duration.ofMillis(500));
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldFormatDateTimeAndApplyValueMapping() throws Exception {
        String id = UUID.randomUUID().toString();
        jdbcTemplate.update(
                "INSERT INTO custom_mapping_outbox "
                + "(id, order_id, customer_name, created_at, order_date, status_code, status) "
                + "VALUES (?, ?, ?, TIMESTAMP '2024-01-15 10:30:45', DATE '2024-01-15', 1, 'PENDING')",
                id, "ORD-100", "Alice");

        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    String status = jdbcTemplate.queryForObject(
                            "SELECT status FROM custom_mapping_outbox WHERE id = ?",
                            String.class, id);
                    assertThat(status).isEqualTo("DONE");
                });

        List<ConsumerRecord<String, String>> received = pollAllMessages(1);
        assertThat(received).hasSize(1);

        Map<String, Object> payload = objectMapper.readValue(received.get(0).value(), Map.class);

        // order_id mapped with dataType=STRING
        assertThat(payload.get("orderId")).isEqualTo("ORD-100");

        // created_at mapped with dataType=DATETIME, format=yyyy-MM-dd'T'HH:mm:ss
        assertThat(payload.get("createdAt")).isEqualTo("2024-01-15T10:30:45");

        // order_date mapped with dataType=DATE, format=yyyy-MM-dd
        assertThat(payload.get("orderDate")).isEqualTo("2024-01-15");

        // status_code mapped via valueMappings: 1 → ACTIVE
        assertThat(payload.get("orderStatus")).isEqualTo("ACTIVE");

        // customer_name without dataType → nested, uses DB default
        Map<String, Object> customer = (Map<String, Object>) payload.get("customer");
        assertThat(customer).isNotNull();
        assertThat(customer.get("name")).isEqualTo("Alice");
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldHandleNullDateTimeAndUnmappedStatusCode() throws Exception {
        String id = UUID.randomUUID().toString();
        // status_code=99 is not in the valueMappings → original integer value preserved
        jdbcTemplate.update(
                "INSERT INTO custom_mapping_outbox "
                + "(id, order_id, customer_name, created_at, order_date, status_code, status) "
                + "VALUES (?, ?, ?, NULL, NULL, 99, 'PENDING')",
                id, "ORD-101", "Bob");

        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    String status = jdbcTemplate.queryForObject(
                            "SELECT status FROM custom_mapping_outbox WHERE id = ?",
                            String.class, id);
                    assertThat(status).isEqualTo("DONE");
                });

        List<ConsumerRecord<String, String>> received = pollAllMessages(1);
        assertThat(received).hasSize(1);

        Map<String, Object> payload = objectMapper.readValue(received.get(0).value(), Map.class);

        // null date/datetime values should remain null
        assertThat(payload.get("createdAt")).isNull();
        assertThat(payload.get("orderDate")).isNull();

        // status_code=99 not in valueMappings → original integer value
        assertThat(payload.get("orderStatus")).isEqualTo(99);

        Map<String, Object> customer = (Map<String, Object>) payload.get("customer");
        assertThat(customer.get("name")).isEqualTo("Bob");
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldMapDifferentStatusCodes() throws Exception {
        String id1 = UUID.randomUUID().toString();
        String id2 = UUID.randomUUID().toString();
        jdbcTemplate.update(
                "INSERT INTO custom_mapping_outbox "
                + "(id, order_id, customer_name, created_at, order_date, status_code, status) "
                + "VALUES (?, ?, ?, TIMESTAMP '2024-06-01 09:00:00', DATE '2024-06-01', 2, 'PENDING')",
                id1, "ORD-200", "Carol");
        jdbcTemplate.update(
                "INSERT INTO custom_mapping_outbox "
                + "(id, order_id, customer_name, created_at, order_date, status_code, status) "
                + "VALUES (?, ?, ?, TIMESTAMP '2024-06-02 18:30:00', DATE '2024-06-02', 3, 'PENDING')",
                id2, "ORD-201", "Dave");

        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    int doneCount = jdbcTemplate.queryForObject(
                            "SELECT COUNT(*) FROM custom_mapping_outbox WHERE status = 'DONE'",
                            Integer.class);
                    assertThat(doneCount).isEqualTo(2);
                });

        List<ConsumerRecord<String, String>> received = pollAllMessages(2);
        assertThat(received).hasSize(2);

        // Collect payloads keyed by orderId for reliable assertion order
        Map<String, Map<String, Object>> byOrderId = new HashMap<>();
        for (ConsumerRecord<String, String> r : received) {
            @SuppressWarnings("unchecked")
            Map<String, Object> p = objectMapper.readValue(r.value(), Map.class);
            byOrderId.put((String) p.get("orderId"), p);
        }

        Map<String, Object> p1 = byOrderId.get("ORD-200");
        assertThat(p1.get("orderStatus")).isEqualTo("INACTIVE");
        assertThat(p1.get("createdAt")).isEqualTo("2024-06-01T09:00:00");
        assertThat(p1.get("orderDate")).isEqualTo("2024-06-01");

        Map<String, Object> p2 = byOrderId.get("ORD-201");
        assertThat(p2.get("orderStatus")).isEqualTo("DELETED");
        assertThat(p2.get("createdAt")).isEqualTo("2024-06-02T18:30:00");
        assertThat(p2.get("orderDate")).isEqualTo("2024-06-02");
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
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "custom-datetime-valuemap-test-consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(props);
    }
}
