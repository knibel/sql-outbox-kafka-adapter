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
 * Integration test verifying the unified {@code mappings} DSL.
 *
 * <p>Uses a mix of explicit column mappings, a static value, and a
 * wildcard camelCase fallback – something impossible with the old
 * single-strategy approach.
 */
@SpringBootTest(
        classes = Application.class,
        properties = {
                "outbox.tables[0].tableName=custom_mapping_outbox",
                "outbox.tables[0].idColumn=id",
                "outbox.tables[0].staticTopic=unified-mapping-topic",
                "outbox.tables[0].statusColumn=status",
                "outbox.tables[0].pendingValue=PENDING",
                "outbox.tables[0].doneValue=DONE",
                "outbox.tables[0].pollIntervalMs=200",
                "outbox.tables[0].batchSize=10",

                // ── Unified mappings DSL ──
                // Explicit mapping: order_id → myOrderId
                "outbox.tables[0].mappings[0].source=order_id",
                "outbox.tables[0].mappings[0].target=orderId",
                "outbox.tables[0].mappings[0].dataType=STRING",

                // Nested mapping: customer_name → customer.name
                "outbox.tables[0].mappings[1].source=customer_name",
                "outbox.tables[0].mappings[1].target=customer.name",

                // Nested mapping: customer_email → customer.email
                "outbox.tables[0].mappings[2].source=customer_email",
                "outbox.tables[0].mappings[2].target=customer.email",

                // Deeply nested: city → customer.address.city
                "outbox.tables[0].mappings[3].source=city",
                "outbox.tables[0].mappings[3].target=customer.address.city",

                // Static value injection
                "outbox.tables[0].mappings[4].target=eventType",
                "outbox.tables[0].mappings[4].value=OrderCreated",

                // CamelCase wildcard for remaining columns
                "outbox.tables[0].mappings[5].source=*",
                "outbox.tables[0].mappings[5].target=_camelCase",
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
        jdbcTemplate.execute("TRUNCATE TABLE custom_mapping_outbox");

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
    void shouldPublishWithUnifiedMappingsDsl() throws Exception {
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

        // Verify the Kafka message.
        List<ConsumerRecord<String, String>> received = pollAllMessages(1);
        assertThat(received).hasSize(1);

        Map<String, Object> payload = objectMapper.readValue(received.get(0).value(), Map.class);

        // Explicit mapping: order_id → orderId (with STRING dataType)
        assertThat(payload.get("orderId")).isEqualTo("ORD-001");

        // Nested mapping: customer_name → customer.name
        Map<String, Object> customer = (Map<String, Object>) payload.get("customer");
        assertThat(customer).isNotNull();
        assertThat(customer.get("name")).isEqualTo("John Doe");
        assertThat(customer.get("email")).isEqualTo("john@example.com");

        // Deeply nested: city → customer.address.city
        Map<String, Object> address = (Map<String, Object>) customer.get("address");
        assertThat(address).isNotNull();
        assertThat(address.get("city")).isEqualTo("Berlin");

        // Static value injection
        assertThat(payload.get("eventType")).isEqualTo("OrderCreated");

        // CamelCase wildcard picks up remaining unmapped columns (total_amount, is_active etc.)
        assertThat(payload).containsKey("totalAmount");
        assertThat(payload).containsKey("isActive");
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
