package com.github.knibel.outbox.jdbc;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SqlIdentifierTest {

    @Test
    void quote_validIdentifier_returnsDoubleQuoted() {
        assertThat(SqlIdentifier.quote("orders_outbox")).isEqualTo("\"orders_outbox\"");
        assertThat(SqlIdentifier.quote("id")).isEqualTo("\"id\"");
        assertThat(SqlIdentifier.quote("_internal")).isEqualTo("\"_internal\"");
        assertThat(SqlIdentifier.quote("col123")).isEqualTo("\"col123\"");
    }

    @Test
    void quote_null_throwsIllegalArgument() {
        assertThatThrownBy(() -> SqlIdentifier.quote(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void quote_blank_throwsIllegalArgument() {
        assertThatThrownBy(() -> SqlIdentifier.quote("   "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void quote_startsWithDigit_throwsIllegalArgument() {
        assertThatThrownBy(() -> SqlIdentifier.quote("1bad"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsafe SQL identifier");
    }

    @Test
    void quote_containsSemicolon_throwsIllegalArgument() {
        assertThatThrownBy(() -> SqlIdentifier.quote("orders; DROP TABLE users--"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsafe SQL identifier");
    }

    @Test
    void quote_containsSpace_throwsIllegalArgument() {
        assertThatThrownBy(() -> SqlIdentifier.quote("my table"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void quote_containsHyphen_throwsIllegalArgument() {
        assertThatThrownBy(() -> SqlIdentifier.quote("my-column"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
