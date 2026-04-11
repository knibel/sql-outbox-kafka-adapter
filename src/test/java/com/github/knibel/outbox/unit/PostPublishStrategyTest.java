package com.github.knibel.outbox.unit;

import com.github.knibel.outbox.config.OutboxTableProperties;
import com.github.knibel.outbox.jdbc.OutboxRepository;
import com.github.knibel.outbox.strategy.DeleteAfterPublishStrategy;
import com.github.knibel.outbox.strategy.MarkDoneStrategy;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * Unit tests for the two built-in {@link com.github.knibel.outbox.strategy.PostPublishStrategy}
 * implementations.
 */
@ExtendWith(MockitoExtension.class)
class PostPublishStrategyTest {

    @Mock
    private OutboxRepository repository;

    private final OutboxTableProperties config = buildConfig();

    // ── DeleteAfterPublishStrategy ──────────────────────────────────────────

    @Test
    void deleteStrategy_callsDeleteByIds() {
        List<String> ids = List.of("id-1", "id-2");

        new DeleteAfterPublishStrategy().onSuccess(config, ids, repository);

        verify(repository).deleteByIds(config, ids);
        verifyNoMoreInteractions(repository);
    }

    @Test
    void deleteStrategy_doesNotCallMarkDone() {
        List<String> ids = List.of("id-1");

        new DeleteAfterPublishStrategy().onSuccess(config, ids, repository);

        verify(repository).deleteByIds(config, ids);
        verifyNoMoreInteractions(repository);
    }

    // ── MarkDoneStrategy ────────────────────────────────────────────────────

    @Test
    void markDoneStrategy_callsMarkDone() {
        List<String> ids = List.of("id-3", "id-4");

        new MarkDoneStrategy().onSuccess(config, ids, repository);

        verify(repository).markDone(config, ids);
        verifyNoMoreInteractions(repository);
    }

    @Test
    void markDoneStrategy_doesNotCallDeleteByIds() {
        List<String> ids = List.of("id-5");

        new MarkDoneStrategy().onSuccess(config, ids, repository);

        verify(repository).markDone(config, ids);
        verifyNoMoreInteractions(repository);
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private static OutboxTableProperties buildConfig() {
        OutboxTableProperties cfg = new OutboxTableProperties();
        cfg.setTableName("orders_outbox");
        cfg.setStaticTopic("orders");
        return cfg;
    }
}
