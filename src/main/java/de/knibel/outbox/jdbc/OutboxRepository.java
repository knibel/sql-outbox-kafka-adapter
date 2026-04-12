package de.knibel.outbox.jdbc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.knibel.outbox.config.AcknowledgementStrategy;
import de.knibel.outbox.config.OutboxTableProperties;
import de.knibel.outbox.config.RowMappingStrategy;
import de.knibel.outbox.domain.OutboxRecord;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 * JDBC-based repository for outbox table operations.
 *
 * <p><b>SQL injection prevention:</b> all column and table names from
 * configuration are validated and double-quoted by {@link SqlIdentifier}.
 * Values are always passed as bind parameters — never concatenated into SQL.
 *
 * <p><b>Concurrency:</b> {@link #claimBatch} uses
 * {@code FOR UPDATE SKIP LOCKED} so multiple application instances never
 * process the same row simultaneously.
 *
 * <p><b>Database compatibility:</b> only standard ANSI SQL is used.
 * {@code FETCH FIRST n ROWS ONLY} (ANSI SQL:2008) replaces the
 * non-standard {@code LIMIT} clause and is supported by PostgreSQL 8.4+
 * and Oracle 12c+.  {@code CURRENT_TIMESTAMP} is ANSI SQL.
 * {@code FOR UPDATE SKIP LOCKED} is the sole vendor extension and is
 * supported by PostgreSQL 9.5+ and Oracle 12c+.
 *
 * <p><b>Oracle note:</b> Oracle does not allow {@code FOR UPDATE} on a
 * query that uses {@code FETCH FIRST} (ORA-02014).  Therefore, the
 * row-limiting clause is placed inside a subquery that selects only the
 * primary key, and the outer query joins back to the table with
 * {@code FOR UPDATE SKIP LOCKED}.
 */
@Repository
public class OutboxRepository {

    private static final Logger log = LoggerFactory.getLogger(OutboxRepository.class);
    private static final TypeReference<Map<String, String>> MAP_TYPE = new TypeReference<>() {};

    private final JdbcTemplate jdbc;
    private final NamedParameterJdbcTemplate namedJdbc;
    private final ObjectMapper objectMapper;

    public OutboxRepository(JdbcTemplate jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.namedJdbc = new NamedParameterJdbcTemplate(jdbc);
        this.objectMapper = objectMapper;
    }

    // ── Public API ──────────────────────────────────────────────────────────

    /**
     * Selects up to {@link OutboxTableProperties#getBatchSize()} pending rows
     * using {@code FOR UPDATE SKIP LOCKED} to prevent concurrent instances from
     * claiming the same rows.
     *
     * <p>The definition of "pending" depends on the configured
     * {@link AcknowledgementStrategy}:
     * <ul>
     *   <li>{@code STATUS} – rows where {@code statusColumn = pendingValue}.
     *   <li>{@code DELETE} – all rows present in the table.
     *   <li>{@code TIMESTAMP} – rows where {@code processedAtColumn IS NULL}.
     * </ul>
     *
     * @return the pending rows mapped to {@link OutboxRecord}s; empty if none.
     */
    @Transactional
    public List<OutboxRecord> claimBatch(OutboxTableProperties config) {
        // ── Custom query: execute user-provided SQL as-is ───────────────
        if (config.getCustomQuery() != null && !config.getCustomQuery().isBlank()) {
            return jdbc.query(config.getCustomQuery(),
                    (rs, rowNum) -> mapRow(rs, config));
        }

        // ── Auto-generated query ────────────────────────────────────────
        String table      = SqlIdentifier.quote(config.getTableName());
        String idCol      = SqlIdentifier.quote(config.getIdColumn());
        String selectList = buildSelectList(config);

        AcknowledgementStrategy strategy = config.getAcknowledgementStrategy();

        if (strategy == AcknowledgementStrategy.STATUS) {
            String statusCol = SqlIdentifier.quote(config.getStatusColumn());
            String sql = "SELECT " + selectList
                    + " FROM " + table
                    + " WHERE " + idCol + " IN ("
                    +   "SELECT " + idCol + " FROM " + table
                    +   " WHERE " + statusCol + " = ?"
                    +   " ORDER BY " + idCol
                    +   " FETCH FIRST ? ROWS ONLY"
                    + ") FOR UPDATE SKIP LOCKED";
            return jdbc.query(sql, (rs, rowNum) -> mapRow(rs, config),
                    config.getPendingValue(), config.getBatchSize());

        } else if (strategy == AcknowledgementStrategy.TIMESTAMP) {
            String processedAtCol = SqlIdentifier.quote(config.getProcessedAtColumn());
            String sql = "SELECT " + selectList
                    + " FROM " + table
                    + " WHERE " + idCol + " IN ("
                    +   "SELECT " + idCol + " FROM " + table
                    +   " WHERE " + processedAtCol + " IS NULL"
                    +   " ORDER BY " + idCol
                    +   " FETCH FIRST ? ROWS ONLY"
                    + ") FOR UPDATE SKIP LOCKED";
            return jdbc.query(sql, (rs, rowNum) -> mapRow(rs, config),
                    config.getBatchSize());

        } else {
            // DELETE strategy: every row present is pending
            String sql = "SELECT " + selectList
                    + " FROM " + table
                    + " WHERE " + idCol + " IN ("
                    +   "SELECT " + idCol + " FROM " + table
                    +   " ORDER BY " + idCol
                    +   " FETCH FIRST ? ROWS ONLY"
                    + ") FOR UPDATE SKIP LOCKED";
            return jdbc.query(sql, (rs, rowNum) -> mapRow(rs, config),
                    config.getBatchSize());
        }
    }

    /**
     * Marks all given rows as done (successfully published to Kafka).
     */
    @Transactional
    public void markDone(OutboxTableProperties config, List<String> ids) {
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

    /**
     * Deletes all given rows (successfully published to Kafka).
     */
    @Transactional
    public void deleteByIds(OutboxTableProperties config, List<String> ids) {
        if (ids.isEmpty()) return;

        String table = SqlIdentifier.quote(config.getTableName());
        String idCol = SqlIdentifier.quote(config.getIdColumn());

        String sql = "DELETE FROM " + table
                + " WHERE " + idCol + " IN (:ids)";

        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("ids", ids);

        namedJdbc.update(sql, params);
    }

    /**
     * Records the current timestamp in {@code processedAtColumn} for all given
     * rows (successfully published to Kafka).
     */
    @Transactional
    public void markProcessedAt(OutboxTableProperties config, List<String> ids) {
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

    /**
     * Executes the user-provided custom acknowledgement query for each
     * processed row.  The query receives the row's {@code idColumn} value
     * as the sole bind parameter ({@code ?}).
     */
    @Transactional
    public void executeCustomAcknowledgement(OutboxTableProperties config, List<String> ids) {
        if (ids.isEmpty()) return;

        String sql = config.getCustomAcknowledgementQuery();
        for (String id : ids) {
            jdbc.update(sql, id);
        }
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    /** Builds the comma-separated SELECT column list. */
    private String buildSelectList(OutboxTableProperties config) {
        RowMappingStrategy rowMapping = config.getRowMappingStrategy();

        if (rowMapping == RowMappingStrategy.TO_CAMEL_CASE) {
            // Select all columns; the mapper will convert names to camelCase.
            return "*";
        }

        if (rowMapping == RowMappingStrategy.CUSTOM) {
            // Select meta columns + all source columns from the field mappings.
            List<String> cols = new ArrayList<>();
            cols.add(SqlIdentifier.quote(config.getIdColumn()));
            if (config.getKeyColumn() != null) {
                cols.add(SqlIdentifier.quote(config.getKeyColumn()));
            }
            if (config.getTopicColumn() != null) {
                cols.add(SqlIdentifier.quote(config.getTopicColumn()));
            }
            if (config.getHeadersColumn() != null) {
                cols.add(SqlIdentifier.quote(config.getHeadersColumn()));
            }
            for (String sourceColumn : config.getFieldMappings().keySet()) {
                String quoted = SqlIdentifier.quote(sourceColumn);
                if (!cols.contains(quoted)) {
                    cols.add(quoted);
                }
            }
            return String.join(", ", cols);
        }

        // PAYLOAD_COLUMN (default): original behaviour
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

    /** Maps a result-set row to an {@link OutboxRecord}. */
    private OutboxRecord mapRow(java.sql.ResultSet rs, OutboxTableProperties config)
            throws java.sql.SQLException {
        String id = rs.getString(config.getIdColumn());

        String kafkaKey = config.getKeyColumn() != null
                ? rs.getString(config.getKeyColumn())
                : id;

        String topic = config.getTopicColumn() != null
                ? rs.getString(config.getTopicColumn())
                : config.getStaticTopic();

        Map<String, String> headers = Collections.emptyMap();
        if (config.getHeadersColumn() != null) {
            String headersJson = rs.getString(config.getHeadersColumn());
            if (headersJson != null && !headersJson.isBlank()) {
                headers = parseHeaders(headersJson, config.getTableName());
            }
        }

        String payload;
        RowMappingStrategy rowMapping = config.getRowMappingStrategy();
        Map<String, String> staticFields = config.getStaticFields();

        if (rowMapping == RowMappingStrategy.TO_CAMEL_CASE) {
            try {
                payload = RowMapperUtil.buildCamelCasePayload(rs, objectMapper, staticFields);
            } catch (Exception e) {
                log.warn("Failed to build camelCase payload for table '{}', row id='{}': {}",
                        config.getTableName(), id, e.getMessage());
                payload = "{}";
            }
        } else if (rowMapping == RowMappingStrategy.CUSTOM) {
            try {
                payload = RowMapperUtil.buildCustomPayload(rs, config.getFieldMappings(), objectMapper, staticFields);
            } catch (Exception e) {
                log.warn("Failed to build custom payload for table '{}', row id='{}': {}",
                        config.getTableName(), id, e.getMessage());
                payload = "{}";
            }
        } else {
            // PAYLOAD_COLUMN (default)
            payload = rs.getString(config.getPayloadColumn());
        }

        return new OutboxRecord(id, kafkaKey, topic, payload, headers);
    }

    private Map<String, String> parseHeaders(String json, String tableName) {
        try {
            return objectMapper.readValue(json, MAP_TYPE);
        } catch (Exception e) {
            log.warn("Could not parse headers JSON for table '{}': {}", tableName, e.getMessage());
            return new HashMap<>();
        }
    }
}
