package com.knibel.outbox;

import java.util.List;
import java.util.UUID;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

/**
 * JPA repository for {@link OutboxMessage}.
 */
public interface OutboxRepository extends JpaRepository<OutboxMessage, UUID> {

    /**
     * Returns unprocessed messages ordered by creation time (oldest first).
     *
     * @param pageable limits the number of rows returned per batch
     * @return list of pending outbox messages
     */
    @Query("SELECT m FROM OutboxMessage m WHERE m.processed = false ORDER BY m.createdAt ASC")
    List<OutboxMessage> findPendingMessages(PageRequest pageable);
}
