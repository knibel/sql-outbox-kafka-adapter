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
 * Integration test verifying the {@code CUSTOM} row mapping strategy with
 * nested object support.
 *
 * <ol>
 *   <li>Rows are inserted into a table with flat columns.
 *   <li>The poller picks them up, applies the configured field mappings
 *       (including dot-separated paths for nesting), builds a JSON payload,
 *       publishes to Kafka, and marks them {@code DONE}.
 *   <li>A test Kafka consumer verifies the payload has the expected nested
 *       JSON structure.
 * </ol>
 */
@SpringBootTest(
        classes = Application.class,
        properties = {
                "outbox.tables[0].tableName=custom_mapping_outbox",
                "outbox.tables[0].idColumn=id",
                "outbox.tables[0].staticTopic=custom-mapping-topic",
                "outbox.tables[0].statusColumn=status",
                "outbox.tables[0].pendingValue=PENDING",
                "outbox.tables[0].doneValue=DONE",
                "outbox.tables[0].rowMappingStrategy=CUSTOM",
                "outbox.tables[0].fieldMappings.order_id.name=orderId",
                "outbox.tables[0].fieldMappings.customer_name.name=customer.name",
                "outbox.tables[0].fieldMappings.customer_email.name=customer.email",
                "outbox.tables[0].fieldMappings.city.name=customer.address.city",
                "outbox.tables[0].pollIntervalMs=200",
                "outbox.tables[0].batchSize=10",
        }
)
@Testcontainers
class OutboxCustomMappingStrategyIntegrationTest {

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
            admin.createTopics(List.of(new NewTopic("custom-mapping-topic", 1, (short) 1)))
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
        consumer.subscribe(Collections.singletonList("custom-mapping-topic"));
        consumer.poll(Duration.ofMillis(500));
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldPublishNestedJsonPayloadToKafka() throws Exception {
        String id = UUID.randomUUID().toString();
        jdbcTemplate.update(
                "INSERT INTO custom_mapping_outbox (id, order_id, customer_name, customer_email, city, total_amount, is_active, status) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING')",
                id, "ORD-001", "John Doe", "john@example.com", "Berlin", 99.95, true);

        // Wait until the row is marked DONE.
        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    String status = jdbcTemplate.queryForObject(
                            "SELECT status FROM custom_mapping_outbox WHERE id = ?",
                            String.class, id);
                    assertThat(status).isEqualTo("DONE");
                });

        // Verify the Kafka message has the expected nested structure.
        List<ConsumerRecord<String, String>> received = pollAllMessages(1);
        assertThat(received).hasSize(1);

        Map<String, Object> payload = objectMapper.readValue(received.get(0).value(), Map.class);

        // Flat mapping: order_id → orderId
        assertThat(payload.get("orderId")).isEqualTo("ORD-001");

        // Nested mapping: customer_name → customer.name, customer_email → customer.email
        Map<String, Object> customer = (Map<String, Object>) payload.get("customer");
        assertThat(customer).isNotNull();
        assertThat(customer.get("name")).isEqualTo("John Doe");
        assertThat(customer.get("email")).isEqualTo("john@example.com");

        // Deeply nested mapping: city → customer.address.city
        Map<String, Object> address = (Map<String, Object>) customer.get("address");
        assertThat(address).isNotNull();
        assertThat(address.get("city")).isEqualTo("Berlin");
    }

    @Test
    void shouldHandleNullValuesInCustomMapping() throws Exception {
        String id = UUID.randomUUID().toString();
        jdbcTemplate.update(
                "INSERT INTO custom_mapping_outbox (id, order_id, customer_name, customer_email, city, total_amount, is_active, status) "
                + "VALUES (?, ?, ?, NULL, NULL, NULL, NULL, 'PENDING')",
                id, "ORD-002", "Jane");

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

        @SuppressWarnings("unchecked")
        Map<String, Object> payload = objectMapper.readValue(received.get(0).value(), Map.class);
        assertThat(payload.get("orderId")).isEqualTo("ORD-002");

        @SuppressWarnings("unchecked")
        Map<String, Object> customer = (Map<String, Object>) payload.get("customer");
        assertThat(customer).isNotNull();
        assertThat(customer.get("name")).isEqualTo("Jane");
        // null values are present in the JSON
        assertThat(customer).containsKey("email");
        assertThat(customer.get("email")).isNull();
    }

    @Test
    void shouldProcessMultipleRowsWithCustomMapping() {
        int rowCount = 5;
        for (int i = 0; i < rowCount; i++) {
            String id = UUID.randomUUID().toString();
            jdbcTemplate.update(
                    "INSERT INTO custom_mapping_outbox (id, order_id, customer_name, customer_email, city, total_amount, is_active, status) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING')",
                    id, "ORD-" + i, "Customer " + i, "cust" + i + "@test.com", "City " + i, 10.0 + i, true);
        }

        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    int doneCount = jdbcTemplate.queryForObject(
                            "SELECT COUNT(*) FROM custom_mapping_outbox WHERE status = 'DONE'",
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
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "custom-mapping-test-consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(props);
    }
}
