package de.knibel.outbox.jdbc.rowmapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.knibel.outbox.config.MappingRule;
import de.knibel.outbox.config.OutboxTableProperties;
import de.knibel.outbox.repository.OutboxDataMapper;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link PayloadMapper} that processes the ordered
 * {@link MappingRule} list from configuration.
 *
 * <p>This mapper acts as a thin JDBC adapter: it converts the
 * {@link ResultSet} to a {@link Map} and delegates the actual
 * mapping logic to a {@link MappingRuleDataMapper} (which implements
 * the technology-independent {@link OutboxDataMapper} interface).
 *
 * <p>The only JDBC-specific concern handled here is the
 * {@link MappingRule#TARGET_RAW _raw} target sentinel, which reads
 * a single column value directly from the result set to avoid the
 * overhead of converting all columns to a map first.
 *
 * @see MappingRuleDataMapper
 * @see OutboxDataMapper
 */
public class MappingRulePayloadMapper implements PayloadMapper {

    private final ObjectMapper objectMapper;
    private final MappingRuleDataMapper dataMapper;

    public MappingRulePayloadMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.dataMapper = new MappingRuleDataMapper();
    }

    /**
     * Returns the underlying technology-independent data mapper.
     *
     * <p>Useful when callers need to map pre-extracted row data
     * (e.g. from a JPA entity or NoSQL document) without going
     * through a JDBC {@link ResultSet}.
     */
    public OutboxDataMapper getDataMapper() {
        return dataMapper;
    }

    @Override
    public String mapPayload(ResultSet rs, OutboxTableProperties config) throws SQLException {
        List<MappingRule> rules = config.getMappings();

        // Raw target short-circuit – reads directly from the ResultSet
        for (MappingRule rule : rules) {
            if (rule.isRawTarget() && rule.getSource() != null) {
                return rs.getString(rule.getSource());
            }
        }

        Map<String, Object> row = resultSetToMap(rs);
        Map<String, Object> mapped = dataMapper.map(row, config);

        try {
            return objectMapper.writeValueAsString(mapped);
        } catch (Exception e) {
            throw new SQLException("Failed to serialize payload to JSON", e);
        }
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    /**
     * Converts the current {@link ResultSet} row to a column-name → value map.
     */
    private static Map<String, Object> resultSetToMap(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();
        Map<String, Object> row = new LinkedHashMap<>(columnCount);
        for (int i = 1; i <= columnCount; i++) {
            row.put(meta.getColumnLabel(i), rs.getObject(i));
        }
        return row;
    }
}
