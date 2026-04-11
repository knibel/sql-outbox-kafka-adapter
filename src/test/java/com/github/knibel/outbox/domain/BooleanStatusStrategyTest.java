package com.github.knibel.outbox.domain;

import com.github.knibel.outbox.config.OutboxTableProperties;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BooleanStatusStrategyTest {

    private final BooleanStatusStrategy strategy = new BooleanStatusStrategy();

    private OutboxTableProperties config() {
        OutboxTableProperties cfg = new OutboxTableProperties();
        cfg.setStatusColumn("processed");
        return cfg;
    }

    @Test
    void pendingClause_usesFalse() {
        SqlFragment frag = strategy.pendingClause(config());
        assertThat(frag.sql()).isEqualTo("\"processed\" = ?");
        assertThat(frag.params()).containsExactly(false);
    }

    @Test
    void hasInProgressState_isFalse() {
        assertThat(strategy.hasInProgressState()).isFalse();
    }

    @Test
    void doneSetFragment_usesTrue() {
        SqlFragment frag = strategy.doneSetFragment(config());
        assertThat(frag.params()).containsExactly(true);
    }

    @Test
    void failedSetFragment_resetsToPending() {
        SqlFragment frag = strategy.failedSetFragment(config());
        assertThat(frag.params()).containsExactly(false);
    }

    @Test
    void stuckClause_isNull() {
        assertThat(strategy.stuckClause(config())).isNull();
    }
}
