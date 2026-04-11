package com.github.knibel.outbox.jdbc;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

class RowMapperUtilTest {

    // ── toCamelCase ──────────────────────────────────────────────────────────

    @ParameterizedTest
    @CsvSource({
            "order_id,        orderId",
            "customer_first_name, customerFirstName",
            "id,              id",
            "ID,              id",
            "ORDER_ID,        orderId",
            "payload,         payload",
            "_internal_flag,  _internalFlag",
            "a,               a",
            "a_b_c,           aBC",
            "already_camel,   alreadyCamel",
    })
    void toCamelCase_convertsCorrectly(String input, String expected) {
        assertThat(RowMapperUtil.toCamelCase(input)).isEqualTo(expected);
    }

    @Test
    void toCamelCase_nullReturnsNull() {
        assertThat(RowMapperUtil.toCamelCase(null)).isNull();
    }

    @Test
    void toCamelCase_emptyReturnsEmpty() {
        assertThat(RowMapperUtil.toCamelCase("")).isEmpty();
    }

    // ── setNestedValue ───────────────────────────────────────────────────────

    @Test
    void setNestedValue_flatPath() {
        Map<String, Object> root = new LinkedHashMap<>();
        RowMapperUtil.setNestedValue(root, "orderId", "123");
        assertThat(root).containsEntry("orderId", "123");
    }

    @Test
    void setNestedValue_nestedPath() {
        Map<String, Object> root = new LinkedHashMap<>();
        RowMapperUtil.setNestedValue(root, "customer.name", "John");
        RowMapperUtil.setNestedValue(root, "customer.email", "john@example.com");

        @SuppressWarnings("unchecked")
        Map<String, Object> customer = (Map<String, Object>) root.get("customer");
        assertThat(customer)
                .containsEntry("name", "John")
                .containsEntry("email", "john@example.com");
    }

    @Test
    void setNestedValue_deeplyNestedPath() {
        Map<String, Object> root = new LinkedHashMap<>();
        RowMapperUtil.setNestedValue(root, "customer.address.city", "New York");
        RowMapperUtil.setNestedValue(root, "customer.address.zip", "10001");
        RowMapperUtil.setNestedValue(root, "customer.name", "John");

        @SuppressWarnings("unchecked")
        Map<String, Object> customer = (Map<String, Object>) root.get("customer");
        assertThat(customer).containsKey("address").containsEntry("name", "John");

        @SuppressWarnings("unchecked")
        Map<String, Object> address = (Map<String, Object>) customer.get("address");
        assertThat(address)
                .containsEntry("city", "New York")
                .containsEntry("zip", "10001");
    }

    @Test
    void setNestedValue_nullValue() {
        Map<String, Object> root = new LinkedHashMap<>();
        RowMapperUtil.setNestedValue(root, "field", null);
        assertThat(root).containsEntry("field", null);
    }

    @Test
    void setNestedValue_mixedFlatAndNested() {
        Map<String, Object> root = new LinkedHashMap<>();
        RowMapperUtil.setNestedValue(root, "orderId", "ORD-001");
        RowMapperUtil.setNestedValue(root, "customer.name", "John");
        RowMapperUtil.setNestedValue(root, "customer.email", "john@example.com");
        RowMapperUtil.setNestedValue(root, "total", 99.95);

        assertThat(root).containsEntry("orderId", "ORD-001");
        assertThat(root).containsEntry("total", 99.95);

        @SuppressWarnings("unchecked")
        Map<String, Object> customer = (Map<String, Object>) root.get("customer");
        assertThat(customer)
                .containsEntry("name", "John")
                .containsEntry("email", "john@example.com");
    }
}
