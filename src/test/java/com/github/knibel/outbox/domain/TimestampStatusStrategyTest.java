package com.github.knibel.outbox.domain;

import com.github.knibel.outbox.config.OutboxTableProperties;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TimestampStatusStrategyTest {

    private final TimestampStatusStrategy strategy = new TimestampStatusStrategy();

    private OutboxTableProperties config() {
        OutboxTableProperties cfg = new OutboxTableProperties();
        cfg.setStatusColumn("processed_at");
        return cfg;
    }

    @Test
    void pendingClause_isNullCheck() {
        SqlFragment frag = strategy.pendingClause(config());
        assertThat(frag.sql()).isEqualTo("\"processed_at\" IS NULL");
        assertThat(frag.params()).isEmpty();
    }

    @Test
    void hasInProgressState_isFalse() {
        assertThat(strategy.hasInProgressState()).isFalse();
    }

    @Test
    void doneSetFragment_usesCurrentTimestamp() {
        SqlFragment frag = strategy.doneSetFragment(config());
        assertThat(frag.sql()).contains("CURRENT_TIMESTAMP");
        assertThat(frag.params()).isEmpty();
    }

    @Test
    void stuckClause_isNull() {
        assertThat(strategy.stuckClause(config())).isNull();
    }
}
