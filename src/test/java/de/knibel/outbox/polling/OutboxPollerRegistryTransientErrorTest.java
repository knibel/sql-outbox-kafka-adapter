package de.knibel.outbox.polling;

import de.knibel.outbox.config.OutboxTableProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the transient-DB-error suppression logic in
 * {@link OutboxPollerRegistry#shouldSuppressTransientError}.
 */
class OutboxPollerRegistryTransientErrorTest {

    private OutboxTableProperties config;

    @BeforeEach
    void setUp() {
        config = new OutboxTableProperties();
        config.setTableName("test_table");
        config.setStaticTopic("test-topic");
    }

    // ── Suppression disabled ─────────────────────────────────────────────────

    @Test
    void suppressionDisabled_whenBothValuesAreZero() {
        config.setTransientDbErrorSilenceAfterIdleMs(0);
        config.setTransientDbErrorSilenceDurationMs(0);

        long now = System.currentTimeMillis();
        assertThat(OutboxPollerRegistry.shouldSuppressTransientError(
                config, now - 10_000, 0L, now, now)).isFalse();
    }

    @Test
    void suppressionDisabled_whenSilenceAfterIdleIsZero() {
        config.setTransientDbErrorSilenceAfterIdleMs(0);
        config.setTransientDbErrorSilenceDurationMs(60_000);

        long now = System.currentTimeMillis();
        assertThat(OutboxPollerRegistry.shouldSuppressTransientError(
                config, now - 120_000, 0L, now, now)).isFalse();
    }

    @Test
    void suppressionDisabled_whenSilenceDurationIsZero() {
        config.setTransientDbErrorSilenceAfterIdleMs(30_000);
        config.setTransientDbErrorSilenceDurationMs(0);

        long now = System.currentTimeMillis();
        assertThat(OutboxPollerRegistry.shouldSuppressTransientError(
                config, now - 120_000, 0L, now, now)).isFalse();
    }

    // ── Idle threshold (A) not yet reached ──────────────────────────────────

    @Test
    void notSuppressed_whenIdleTimeBelowThreshold() {
        config.setTransientDbErrorSilenceAfterIdleMs(60_000);  // A = 60 s
        config.setTransientDbErrorSilenceDurationMs(120_000);  // B = 120 s

        long now = System.currentTimeMillis();
        long lastRead = now - 30_000; // only 30 s idle (< A)

        assertThat(OutboxPollerRegistry.shouldSuppressTransientError(
                config, now - 120_000, lastRead, now, now)).isFalse();
    }

    // ── Idle threshold reached, within silence window ────────────────────────

    @Test
    void suppressed_whenIdleAboveThresholdAndWithinSilenceWindow() {
        config.setTransientDbErrorSilenceAfterIdleMs(30_000);  // A = 30 s
        config.setTransientDbErrorSilenceDurationMs(120_000);  // B = 120 s

        long now = System.currentTimeMillis();
        long lastRead = now - 60_000;   // 60 s idle (> A)
        long errorStart = now - 10_000; // error started 10 s ago (< B)

        assertThat(OutboxPollerRegistry.shouldSuppressTransientError(
                config, now - 300_000, lastRead, errorStart, now)).isTrue();
    }

    @Test
    void suppressed_whenNeverReadRecordsAndIdleFromStartup_withinSilenceWindow() {
        config.setTransientDbErrorSilenceAfterIdleMs(5_000);   // A = 5 s
        config.setTransientDbErrorSilenceDurationMs(60_000);   // B = 60 s

        long now = System.currentTimeMillis();
        long pollerStart = now - 30_000; // poller started 30 s ago (> A)
        long errorStart  = now - 10_000; // error started 10 s ago (< B)

        // lastRecordReadTimeMs = 0 → idle since startup
        assertThat(OutboxPollerRegistry.shouldSuppressTransientError(
                config, pollerStart, 0L, errorStart, now)).isTrue();
    }

    // ── Silence window (B) expired ───────────────────────────────────────────

    @Test
    void notSuppressed_whenSilenceWindowExpired() {
        config.setTransientDbErrorSilenceAfterIdleMs(30_000);  // A = 30 s
        config.setTransientDbErrorSilenceDurationMs(60_000);   // B = 60 s

        long now = System.currentTimeMillis();
        long lastRead   = now - 120_000; // 120 s idle (> A)
        long errorStart = now - 90_000;  // error started 90 s ago (> B)

        assertThat(OutboxPollerRegistry.shouldSuppressTransientError(
                config, now - 300_000, lastRead, errorStart, now)).isFalse();
    }

    @Test
    void notSuppressed_whenErrorStartsExactlyAtSilenceWindowBoundary() {
        config.setTransientDbErrorSilenceAfterIdleMs(30_000);  // A = 30 s
        config.setTransientDbErrorSilenceDurationMs(60_000);   // B = 60 s

        long now = System.currentTimeMillis();
        long lastRead   = now - 120_000;
        // Error duration == B exactly: boundary is still within window (<=)
        long errorStart = now - 60_000;

        assertThat(OutboxPollerRegistry.shouldSuppressTransientError(
                config, now - 300_000, lastRead, errorStart, now)).isTrue();
    }

    @Test
    void notSuppressed_whenErrorDurationExceedsSilenceWindow() {
        config.setTransientDbErrorSilenceAfterIdleMs(30_000);  // A = 30 s
        config.setTransientDbErrorSilenceDurationMs(60_000);   // B = 60 s

        long now = System.currentTimeMillis();
        long lastRead   = now - 120_000;
        long errorStart = now - 61_000; // 61 s > B

        assertThat(OutboxPollerRegistry.shouldSuppressTransientError(
                config, now - 300_000, lastRead, errorStart, now)).isFalse();
    }
}
