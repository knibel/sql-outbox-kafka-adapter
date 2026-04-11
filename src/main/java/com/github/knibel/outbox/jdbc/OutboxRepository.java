package com.github.knibel.outbox.jdbc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.knibel.outbox.config.OutboxTableProperties;
import com.github.knibel.outbox.domain.OutboxRecord;
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
 * <p><b>Database compatibility:</b> only standard JDBC SQL is used.
 * {@code FOR UPDATE SKIP LOCKED} is the sole non-standard feature and is
 * supported by PostgreSQL 9.5+, MySQL 8.0+, and Oracle 12c+.
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
     * @return the pending rows mapped to {@link OutboxRecord}s; empty if none.
     */
    @Transactional
    public List<OutboxRecord> claimBatch(OutboxTableProperties config) {
        String table     = SqlIdentifier.quote(config.getTableName());
        String idCol     = SqlIdentifier.quote(config.getIdColumn());
        String statusCol = SqlIdentifier.quote(config.getStatusColumn());
        String selectList = buildSelectList(config);

        String sql = "SELECT " + selectList
                + " FROM " + table
                + " WHERE " + statusCol + " = ?"
                + " ORDER BY " + idCol
                + " LIMIT ?"
                + " FOR UPDATE SKIP LOCKED";

        return jdbc.query(sql, (rs, rowNum) -> mapRow(rs, config),
                config.getPendingValue(), config.getBatchSize());
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
     * Deletes all given rows from the outbox table.
     *
     * <p>Used by the {@link com.github.knibel.outbox.strategy.DeleteAfterPublishStrategy}
     * to remove rows immediately after they have been successfully published to Kafka.
     */
    @Transactional
    public void deleteByIds(OutboxTableProperties config, List<String> ids) {
        if (ids.isEmpty()) return;

        String table = SqlIdentifier.quote(config.getTableName());
        String idCol = SqlIdentifier.quote(config.getIdColumn());

        String sql = "DELETE FROM " + table + " WHERE " + idCol + " IN (:ids)";

        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("ids", ids);

        namedJdbc.update(sql, params);
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    /** Builds the comma-separated SELECT column list. */
    private String buildSelectList(OutboxTableProperties config) {
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

        String payload = rs.getString(config.getPayloadColumn());

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
