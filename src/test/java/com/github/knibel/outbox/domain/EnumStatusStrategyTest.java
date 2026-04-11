package com.github.knibel.outbox.domain;

import com.github.knibel.outbox.config.OutboxTableProperties;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EnumStatusStrategyTest {

    private final EnumStatusStrategy strategy = new EnumStatusStrategy();

    private OutboxTableProperties config() {
        OutboxTableProperties cfg = new OutboxTableProperties();
        cfg.setTableName("orders_outbox");
        cfg.setStatusColumn("status");
        cfg.setPendingValue("PENDING");
        cfg.setInProgressValue("IN_PROGRESS");
        cfg.setDoneValue("DONE");
        cfg.setFailedValue("FAILED");
        cfg.setUpdatedAtColumn("updated_at");
        cfg.setStuckTtlSeconds(300);
        return cfg;
    }

    @Test
    void pendingClause_producesQuotedColumnWithParam() {
        SqlFragment frag = strategy.pendingClause(config());
        assertThat(frag.sql()).isEqualTo("\"status\" = ?");
        assertThat(frag.params()).containsExactly("PENDING");
    }

    @Test
    void hasInProgressState_isTrue() {
        assertThat(strategy.hasInProgressState()).isTrue();
    }

    @Test
    void claimSetFragment_producesInProgressParam() {
        SqlFragment frag = strategy.claimSetFragment(config());
        assertThat(frag.sql()).isEqualTo("\"status\" = ?");
        assertThat(frag.params()).containsExactly("IN_PROGRESS");
    }

    @Test
    void doneSetFragment_producesDoneParam() {
        SqlFragment frag = strategy.doneSetFragment(config());
        assertThat(frag.params()).containsExactly("DONE");
    }

    @Test
    void failedSetFragment_producesFailedParam() {
        SqlFragment frag = strategy.failedSetFragment(config());
        assertThat(frag.params()).containsExactly("FAILED");
    }

    @Test
    void resetSetFragment_producesPendingParam() {
        SqlFragment frag = strategy.resetSetFragment(config());
        assertThat(frag.params()).containsExactly("PENDING");
    }

    @Test
    void stuckClause_containsInProgressAndTimestampCutoff() {
        SqlFragment frag = strategy.stuckClause(config());
        assertThat(frag).isNotNull();
        assertThat(frag.sql()).contains("\"status\"");
        assertThat(frag.sql()).contains("\"updated_at\"");
        assertThat(frag.sql()).doesNotContain("INTERVAL");
        // First param is the IN_PROGRESS value; second is a Timestamp cutoff
        assertThat(frag.params()).hasSize(2);
        assertThat(frag.params().get(0)).isEqualTo("IN_PROGRESS");
        assertThat(frag.params().get(1)).isInstanceOf(java.sql.Timestamp.class);
        // Cutoff should be in the past (roughly now - stuckTtlSeconds)
        long expectedCutoff = System.currentTimeMillis() - 300_000L;
        long actualCutoff = ((java.sql.Timestamp) frag.params().get(1)).getTime();
        assertThat(actualCutoff).isBetween(expectedCutoff - 2_000L, expectedCutoff + 2_000L);
    }

    @Test
    void stuckClause_returnsNull_whenNoUpdatedAtColumn() {
        OutboxTableProperties cfg = config();
        cfg.setUpdatedAtColumn(null);
        assertThat(strategy.stuckClause(cfg)).isNull();
    }
}
