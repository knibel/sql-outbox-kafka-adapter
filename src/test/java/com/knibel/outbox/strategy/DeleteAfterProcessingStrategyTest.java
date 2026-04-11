package com.knibel.outbox.strategy;

import com.knibel.outbox.OutboxMessage;
import com.knibel.outbox.OutboxRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
class DeleteAfterProcessingStrategyTest {

    @Mock
    private OutboxRepository repository;

    private final DeleteAfterProcessingStrategy strategy = new DeleteAfterProcessingStrategy();

    @Test
    void onSuccess_deletesTheMessageFromTheRepository() {
        OutboxMessage message = new OutboxMessage("my-topic", "key-1", "{\"event\":\"created\"}");

        strategy.onSuccess(message, repository);

        verify(repository).delete(message);
        verifyNoMoreInteractions(repository);
    }

    @Test
    void onSuccess_doesNotMarkMessageAsProcessed() {
        OutboxMessage message = new OutboxMessage("my-topic", "key-2", "{\"event\":\"updated\"}");

        strategy.onSuccess(message, repository);

        // The message entity must not have been mutated (only deletion is expected)
        org.assertj.core.api.Assertions.assertThat(message.isProcessed()).isFalse();
        org.assertj.core.api.Assertions.assertThat(message.getProcessedAt()).isNull();
    }
}
