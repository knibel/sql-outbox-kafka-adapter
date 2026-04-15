package de.knibel.outbox.jdbc.acknowledgement;

import de.knibel.outbox.config.OutboxTableProperties;
import de.knibel.outbox.repository.AcknowledgementHandler;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Marks rows as processed by executing a user-provided SQL statement.
 * The statement receives each row's ID as a single bind parameter.
 */
public class CustomAcknowledgementHandler implements AcknowledgementHandler {

    private final JdbcTemplate jdbc;

    public CustomAcknowledgementHandler(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public void acknowledge(OutboxTableProperties config, List<String> ids) {
        if (ids.isEmpty()) return;

        String sql = config.getCustomAcknowledgementQuery();
        for (String id : ids) {
            jdbc.update(sql, id);
        }
    }
}
