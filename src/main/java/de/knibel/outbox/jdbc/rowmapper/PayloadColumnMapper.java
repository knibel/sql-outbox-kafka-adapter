package de.knibel.outbox.jdbc.rowmapper;

import de.knibel.outbox.config.OutboxTableProperties;
import de.knibel.outbox.jdbc.SqlIdentifier;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads the Kafka record value from a single pre-serialized JSON column
 * ({@code payloadColumn}).
 *
 * <p>This is the default row-mapping strategy and preserves backward
 * compatibility with the original behaviour.
 */
public class PayloadColumnMapper implements PayloadMapper {

    @Override
    public String buildSelectList(OutboxTableProperties config) {
        List<String> cols = new ArrayList<>();
        cols.add(SqlIdentifier.quote(config.getIdColumn()));
        if (config.getKeyColumn() != null) {
            cols.add(SqlIdentifier.quote(config.getKeyColumn()));
        }
        cols.add(SqlIdentifier.quote(config.getPayloadColumn()));
        if (config.getTopicColumn() != null) {
            cols.add(SqlIdentifier.quote(config.getTopicColumn()));
        }
        if (config.getHeadersColumn() != null) {
            cols.add(SqlIdentifier.quote(config.getHeadersColumn()));
        }
        return String.join(", ", cols);
    }

    @Override
    public String mapPayload(ResultSet rs, OutboxTableProperties config) throws SQLException {
        return rs.getString(config.getPayloadColumn());
    }
}
