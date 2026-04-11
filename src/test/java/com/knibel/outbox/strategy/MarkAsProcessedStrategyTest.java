package com.knibel.outbox.strategy;

import com.knibel.outbox.OutboxMessage;
import com.knibel.outbox.OutboxRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
class MarkAsProcessedStrategyTest {

    @Mock
    private OutboxRepository repository;

    private final MarkAsProcessedStrategy strategy = new MarkAsProcessedStrategy();

    @Test
    void onSuccess_marksMessageAsProcessedAndSaves() {
        OutboxMessage message = new OutboxMessage("my-topic", "key-1", "{\"event\":\"created\"}");

        strategy.onSuccess(message, repository);

        assertThat(message.isProcessed()).isTrue();
        assertThat(message.getProcessedAt()).isNotNull();
        verify(repository).save(message);
        verifyNoMoreInteractions(repository);
    }

    @Test
    void onSuccess_doesNotDeleteTheMessage() {
        OutboxMessage message = new OutboxMessage("my-topic", "key-2", "{\"event\":\"deleted\"}");

        strategy.onSuccess(message, repository);

        // delete must never be called
        verify(repository).save(message);
        verifyNoMoreInteractions(repository);
    }
}
