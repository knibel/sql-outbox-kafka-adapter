package de.knibel.outbox.jdbc;

import de.knibel.outbox.config.FieldDataType;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    // ── convertValue (basic types) ───────────────────────────────────────────

    @Test
    void convertValue_nullDataType_returnsValueUnchanged() {
        assertThat(RowMapperUtil.convertValue("hello", null)).isEqualTo("hello");
        assertThat(RowMapperUtil.convertValue(42, null)).isEqualTo(42);
    }

    @Test
    void convertValue_nullValue_returnsNull() {
        assertThat(RowMapperUtil.convertValue(null, FieldDataType.STRING)).isNull();
        assertThat(RowMapperUtil.convertValue(null, FieldDataType.INTEGER)).isNull();
    }

    @Test
    void convertValue_toString() {
        assertThat(RowMapperUtil.convertValue(42, FieldDataType.STRING)).isEqualTo("42");
        assertThat(RowMapperUtil.convertValue(3.14, FieldDataType.STRING)).isEqualTo("3.14");
        assertThat(RowMapperUtil.convertValue(true, FieldDataType.STRING)).isEqualTo("true");
        assertThat(RowMapperUtil.convertValue("already", FieldDataType.STRING)).isEqualTo("already");
    }

    @Test
    void convertValue_toInteger_fromNumber() {
        assertThat(RowMapperUtil.convertValue(42L, FieldDataType.INTEGER)).isEqualTo(42);
        assertThat(RowMapperUtil.convertValue(3.9, FieldDataType.INTEGER)).isEqualTo(3);
        assertThat(RowMapperUtil.convertValue(new BigDecimal("100"), FieldDataType.INTEGER)).isEqualTo(100);
    }

    @Test
    void convertValue_toInteger_fromString() {
        assertThat(RowMapperUtil.convertValue("123", FieldDataType.INTEGER)).isEqualTo(123);
    }

    @Test
    void convertValue_toLong_fromNumber() {
        assertThat(RowMapperUtil.convertValue(42, FieldDataType.LONG)).isEqualTo(42L);
        assertThat(RowMapperUtil.convertValue(3.9, FieldDataType.LONG)).isEqualTo(3L);
    }

    @Test
    void convertValue_toLong_fromString() {
        assertThat(RowMapperUtil.convertValue("9876543210", FieldDataType.LONG)).isEqualTo(9876543210L);
    }

    @Test
    void convertValue_toDouble_fromNumber() {
        assertThat(RowMapperUtil.convertValue(42, FieldDataType.DOUBLE)).isEqualTo(42.0);
        assertThat(RowMapperUtil.convertValue(new BigDecimal("99.95"), FieldDataType.DOUBLE)).isEqualTo(99.95);
    }

    @Test
    void convertValue_toDouble_fromString() {
        assertThat(RowMapperUtil.convertValue("3.14", FieldDataType.DOUBLE)).isEqualTo(3.14);
    }

    @Test
    void convertValue_toBoolean() {
        assertThat(RowMapperUtil.convertValue(true, FieldDataType.BOOLEAN)).isEqualTo(true);
        assertThat(RowMapperUtil.convertValue(false, FieldDataType.BOOLEAN)).isEqualTo(false);
        assertThat(RowMapperUtil.convertValue("true", FieldDataType.BOOLEAN)).isEqualTo(true);
        assertThat(RowMapperUtil.convertValue("false", FieldDataType.BOOLEAN)).isEqualTo(false);
    }

    @Test
    void convertValue_toDecimal() {
        assertThat(RowMapperUtil.convertValue(42, FieldDataType.DECIMAL)).isEqualTo(new BigDecimal("42"));
        assertThat(RowMapperUtil.convertValue("99.95", FieldDataType.DECIMAL)).isEqualTo(new BigDecimal("99.95"));
        BigDecimal bd = new BigDecimal("123.456");
        assertThat(RowMapperUtil.convertValue(bd, FieldDataType.DECIMAL)).isSameAs(bd);
    }

    // ── convertValue (DATE) ──────────────────────────────────────────────────

    @Test
    void convertValue_date_fromLocalDate() {
        LocalDate date = LocalDate.of(2024, 1, 15);
        Object result = RowMapperUtil.convertValue(date, FieldDataType.DATE, "yyyy-MM-dd");
        assertThat(result).isEqualTo("2024-01-15");
    }

    @Test
    void convertValue_date_fromSqlDate() {
        java.sql.Date sqlDate = java.sql.Date.valueOf(LocalDate.of(2024, 3, 20));
        Object result = RowMapperUtil.convertValue(sqlDate, FieldDataType.DATE, "yyyy-MM-dd");
        assertThat(result).isEqualTo("2024-03-20");
    }

    @Test
    void convertValue_date_fromTimestamp() {
        Timestamp ts = Timestamp.valueOf(LocalDateTime.of(2024, 6, 15, 10, 30, 0));
        Object result = RowMapperUtil.convertValue(ts, FieldDataType.DATE, "yyyy-MM-dd");
        assertThat(result).isEqualTo("2024-06-15");
    }

    @Test
    void convertValue_date_fromLocalDateTime() {
        LocalDateTime ldt = LocalDateTime.of(2024, 12, 25, 14, 0, 0);
        Object result = RowMapperUtil.convertValue(ldt, FieldDataType.DATE, "dd/MM/yyyy");
        assertThat(result).isEqualTo("25/12/2024");
    }

    @Test
    void convertValue_date_nullReturnsNull() {
        assertThat(RowMapperUtil.convertValue(null, FieldDataType.DATE, "yyyy-MM-dd")).isNull();
    }

    @Test
    void convertValue_date_unsupportedTypeThrows() {
        assertThatThrownBy(() -> RowMapperUtil.convertValue("not-a-date", FieldDataType.DATE, "yyyy-MM-dd"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("LocalDate");
    }

    // ── convertValue (DATETIME) ──────────────────────────────────────────────

    @Test
    void convertValue_datetime_fromLocalDateTime() {
        LocalDateTime ldt = LocalDateTime.of(2024, 1, 15, 10, 30, 45);
        Object result = RowMapperUtil.convertValue(ldt, FieldDataType.DATETIME, "yyyy-MM-dd'T'HH:mm:ss");
        assertThat(result).isEqualTo("2024-01-15T10:30:45");
    }

    @Test
    void convertValue_datetime_fromTimestamp() {
        Timestamp ts = Timestamp.valueOf(LocalDateTime.of(2024, 6, 15, 14, 0, 0));
        Object result = RowMapperUtil.convertValue(ts, FieldDataType.DATETIME, "yyyy-MM-dd HH:mm:ss");
        assertThat(result).isEqualTo("2024-06-15 14:00:00");
    }

    @Test
    void convertValue_datetime_fromLocalDate() {
        LocalDate date = LocalDate.of(2024, 1, 15);
        Object result = RowMapperUtil.convertValue(date, FieldDataType.DATETIME, "yyyy-MM-dd'T'HH:mm:ss");
        assertThat(result).isEqualTo("2024-01-15T00:00:00");
    }

    @Test
    void convertValue_datetime_fromSqlDate() {
        java.sql.Date sqlDate = java.sql.Date.valueOf(LocalDate.of(2024, 3, 20));
        Object result = RowMapperUtil.convertValue(sqlDate, FieldDataType.DATETIME, "yyyy-MM-dd'T'HH:mm:ss");
        assertThat(result).isEqualTo("2024-03-20T00:00:00");
    }

    @Test
    void convertValue_datetime_nullReturnsNull() {
        assertThat(RowMapperUtil.convertValue(null, FieldDataType.DATETIME, "yyyy-MM-dd'T'HH:mm:ss")).isNull();
    }

    @Test
    void convertValue_datetime_unsupportedTypeThrows() {
        assertThatThrownBy(() -> RowMapperUtil.convertValue("not-a-datetime", FieldDataType.DATETIME, "yyyy-MM-dd'T'HH:mm:ss"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("LocalDateTime");
    }

    // ── applyValueMapping ────────────────────────────────────────────────────

    @Test
    void applyValueMapping_nullValue_returnsNull() {
        Map<String, String> mappings = Map.of("1", "ACTIVE");
        assertThat(RowMapperUtil.applyValueMapping(null, mappings)).isNull();
    }

    @Test
    void applyValueMapping_nullMappings_returnsOriginal() {
        assertThat(RowMapperUtil.applyValueMapping(42, null)).isEqualTo(42);
    }

    @Test
    void applyValueMapping_emptyMappings_returnsOriginal() {
        assertThat(RowMapperUtil.applyValueMapping(42, Map.of())).isEqualTo(42);
    }

    @Test
    void applyValueMapping_matchingKey_returnsMappedValue() {
        Map<String, String> mappings = Map.of("1", "ACTIVE", "2", "INACTIVE");
        assertThat(RowMapperUtil.applyValueMapping(1, mappings)).isEqualTo("ACTIVE");
        assertThat(RowMapperUtil.applyValueMapping(2, mappings)).isEqualTo("INACTIVE");
    }

    @Test
    void applyValueMapping_noMatch_returnsOriginal() {
        Map<String, String> mappings = Map.of("1", "ACTIVE", "2", "INACTIVE");
        assertThat(RowMapperUtil.applyValueMapping(99, mappings)).isEqualTo(99);
    }

    @Test
    void applyValueMapping_stringKey_matchesCorrectly() {
        Map<String, String> mappings = Map.of("true", "YES", "false", "NO");
        assertThat(RowMapperUtil.applyValueMapping(true, mappings)).isEqualTo("YES");
        assertThat(RowMapperUtil.applyValueMapping(false, mappings)).isEqualTo("NO");
    }
}
