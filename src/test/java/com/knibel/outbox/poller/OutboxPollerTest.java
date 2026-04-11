package com.knibel.outbox.poller;

import com.knibel.outbox.OutboxMessage;
import com.knibel.outbox.OutboxPoller;
import com.knibel.outbox.OutboxRepository;
import com.knibel.outbox.config.OutboxProperties;
import com.knibel.outbox.strategy.DeleteAfterProcessingStrategy;
import com.knibel.outbox.strategy.OutboxProcessingStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OutboxPollerTest {

    @Mock
    private OutboxRepository repository;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private OutboxProcessingStrategy strategy;
    private OutboxPoller poller;

    @BeforeEach
    void setUp() {
        OutboxProperties properties = new OutboxProperties();
        strategy = spy(new DeleteAfterProcessingStrategy());
        poller = new OutboxPoller(repository, kafkaTemplate, strategy, properties);
    }

    @Test
    void poll_doesNothingWhenNoMessagesArePending() {
        when(repository.findPendingMessages(any(PageRequest.class))).thenReturn(List.of());

        poller.poll();

        verifyNoInteractions(kafkaTemplate);
    }

    @Test
    void poll_publishesEachPendingMessageToKafka() throws Exception {
        OutboxMessage msg1 = new OutboxMessage("topic-a", "k1", "payload-1");
        OutboxMessage msg2 = new OutboxMessage("topic-b", "k2", "payload-2");
        when(repository.findPendingMessages(any(PageRequest.class))).thenReturn(List.of(msg1, msg2));
        mockSuccessfulSend("topic-a", "k1", "payload-1");
        mockSuccessfulSend("topic-b", "k2", "payload-2");

        poller.poll();

        verify(kafkaTemplate).send("topic-a", "k1", "payload-1");
        verify(kafkaTemplate).send("topic-b", "k2", "payload-2");
    }

    @Test
    void poll_callsStrategyOnSuccessForEachMessage() throws Exception {
        OutboxMessage msg = new OutboxMessage("events", "key", "{\"type\":\"ORDER_CREATED\"}");
        when(repository.findPendingMessages(any(PageRequest.class))).thenReturn(List.of(msg));
        mockSuccessfulSend("events", "key", "{\"type\":\"ORDER_CREATED\"}");

        poller.poll();

        verify(strategy).onSuccess(eq(msg), eq(repository));
    }

    @Test
    void poll_withDeleteStrategy_deletesRowAfterSuccessfulPublish() throws Exception {
        OutboxMessage msg = new OutboxMessage("events", "key", "{}");
        when(repository.findPendingMessages(any(PageRequest.class))).thenReturn(List.of(msg));
        mockSuccessfulSend("events", "key", "{}");

        poller.poll();

        verify(repository).delete(msg);
        verify(repository, never()).save(any());
    }

    @Test
    void poll_continuesWithRemainingMessagesWhenOnePublishFails() throws Exception {
        OutboxMessage bad = new OutboxMessage("events", "bad-key", "{}");
        OutboxMessage good = new OutboxMessage("events", "good-key", "{\"ok\":true}");
        when(repository.findPendingMessages(any(PageRequest.class))).thenReturn(List.of(bad, good));

        CompletableFuture<SendResult<String, String>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("broker unavailable"));
        when(kafkaTemplate.send("events", "bad-key", "{}")).thenReturn(failedFuture);
        mockSuccessfulSend("events", "good-key", "{\"ok\":true}");

        poller.poll();

        // Only the good message should trigger the strategy
        verify(strategy, times(1)).onSuccess(good, repository);
        verify(strategy, never()).onSuccess(bad, repository);
    }

    // ── helpers ────────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private void mockSuccessfulSend(String topic, String key, String payload) {
        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition(topic, 0), 0L, 0, 0L, 0, 0);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, payload);
        SendResult<String, String> sendResult = new SendResult<>(record, metadata);

        CompletableFuture<SendResult<String, String>> future = CompletableFuture.completedFuture(sendResult);
        when(kafkaTemplate.send(topic, key, payload)).thenReturn(future);
    }
}
