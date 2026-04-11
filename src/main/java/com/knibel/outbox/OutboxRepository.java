package com.knibel.outbox;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

/**
 * JPA repository for {@link OutboxMessage}.
 */
public interface OutboxRepository extends JpaRepository<OutboxMessage, UUID> {

    /**
     * Returns all unprocessed messages ordered by creation time (oldest first).
     *
     * @param limit maximum number of messages to fetch in one batch
     * @return list of pending outbox messages
     */
    @Query("SELECT m FROM OutboxMessage m WHERE m.processed = false ORDER BY m.createdAt ASC LIMIT :limit")
    List<OutboxMessage> findPendingMessages(int limit);
}
