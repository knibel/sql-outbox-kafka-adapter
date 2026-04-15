package de.knibel.outbox.jdbc.acknowledgement;

import de.knibel.outbox.config.OutboxTableProperties;
import de.knibel.outbox.jdbc.SqlIdentifier;
import de.knibel.outbox.repository.AcknowledgementHandler;
import java.util.List;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Marks rows as processed by writing {@code CURRENT_TIMESTAMP} into the
 * configured {@code processedAtColumn}.
 */
public class TimestampAcknowledgementHandler implements AcknowledgementHandler {

    private final NamedParameterJdbcTemplate namedJdbc;

    public TimestampAcknowledgementHandler(NamedParameterJdbcTemplate namedJdbc) {
        this.namedJdbc = namedJdbc;
    }

    @Override
    public void acknowledge(OutboxTableProperties config, List<String> ids) {
        if (ids.isEmpty()) return;

        String table          = SqlIdentifier.quote(config.getTableName());
        String idCol          = SqlIdentifier.quote(config.getIdColumn());
        String processedAtCol = SqlIdentifier.quote(config.getProcessedAtColumn());

        String sql = "UPDATE " + table
                + " SET " + processedAtCol + " = CURRENT_TIMESTAMP"
                + " WHERE " + idCol + " IN (:ids)";

        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("ids", ids);

        namedJdbc.update(sql, params);
    }
}
