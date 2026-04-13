package de.knibel.outbox.jdbc.acknowledgement;

import de.knibel.outbox.config.OutboxTableProperties;
import de.knibel.outbox.jdbc.SqlIdentifier;
import de.knibel.outbox.repository.AcknowledgementHandler;
import java.util.List;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Marks rows as processed by updating the status column to the configured
 * {@code doneValue}.
 */
public class StatusAcknowledgementHandler implements AcknowledgementHandler {

    private final NamedParameterJdbcTemplate namedJdbc;

    public StatusAcknowledgementHandler(NamedParameterJdbcTemplate namedJdbc) {
        this.namedJdbc = namedJdbc;
    }

    @Override
    public void acknowledge(OutboxTableProperties config, List<String> ids) {
        if (ids.isEmpty()) return;

        String table     = SqlIdentifier.quote(config.getTableName());
        String idCol     = SqlIdentifier.quote(config.getIdColumn());
        String statusCol = SqlIdentifier.quote(config.getStatusColumn());

        String sql = "UPDATE " + table
                + " SET " + statusCol + " = :doneValue"
                + " WHERE " + idCol + " IN (:ids)";

        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("doneValue", config.getDoneValue())
                .addValue("ids", ids);

        namedJdbc.update(sql, params);
    }
}
