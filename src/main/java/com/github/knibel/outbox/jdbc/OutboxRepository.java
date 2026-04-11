package com.github.knibel.outbox.jdbc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.knibel.outbox.config.AcknowledgementStrategy;
import com.github.knibel.outbox.config.OutboxTableProperties;
import com.github.knibel.outbox.domain.OutboxRecord;
import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
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
 * <p><b>Database compatibility:</b> standard ANSI SQL is used where possible.
 * {@code FETCH FIRST n ROWS ONLY} (ANSI SQL:2008) is used for PostgreSQL.
 * Oracle requires {@code ROWNUM} for row-limiting inside {@code FOR UPDATE
 * SKIP LOCKED} queries because {@code FETCH FIRST} internally creates an
 * inline view that Oracle rejects with {@code ORA-02014}.
 * {@code CURRENT_TIMESTAMP} is ANSI SQL.
 * {@code FOR UPDATE SKIP LOCKED} is the sole vendor extension and is
 * supported by PostgreSQL 9.5+ and Oracle 12c+.
 */
@Repository
public class OutboxRepository {

    private static final Logger log = LoggerFactory.getLogger(OutboxRepository.class);
    private static final TypeReference<Map<String, String>> MAP_TYPE = new TypeReference<>() {};

    private final JdbcTemplate jdbc;
    private final NamedParameterJdbcTemplate namedJdbc;
    private final ObjectMapper objectMapper;
    private final boolean oracle;

    public OutboxRepository(JdbcTemplate jdbc, ObjectMapper objectMapper, DataSource dataSource) {
        this.jdbc = jdbc;
        this.namedJdbc = new NamedParameterJdbcTemplate(jdbc);
        this.objectMapper = objectMapper;
        this.oracle = detectOracle(dataSource);
    }

    private static boolean detectOracle(DataSource dataSource) {
        try (var conn = dataSource.getConnection()) {
            DatabaseMetaData meta = conn.getMetaData();
            String product = meta.getDatabaseProductName();
            return product != null && product.toLowerCase().contains("oracle");
        } catch (Exception e) {
            return false;
        }
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
        String table      = quoteIdentifier(config.getTableName());
        String idCol      = quoteIdentifier(config.getIdColumn());
        String selectList = buildSelectList(config);

        AcknowledgementStrategy strategy = config.getAcknowledgementStrategy();

        if (strategy == AcknowledgementStrategy.STATUS) {
            String statusCol = quoteIdentifier(config.getStatusColumn());
            String sql;
            if (oracle) {
                sql = "SELECT " + selectList
                        + " FROM " + table
                        + " WHERE " + statusCol + " = ? AND ROWNUM <= ?"
                        + " ORDER BY " + idCol
                        + " FOR UPDATE SKIP LOCKED";
            } else {
                sql = "SELECT " + selectList
                        + " FROM " + table
                        + " WHERE " + statusCol + " = ?"
                        + " ORDER BY " + idCol
                        + " FETCH FIRST ? ROWS ONLY"
                        + " FOR UPDATE SKIP LOCKED";
            }
            return jdbc.query(sql, (rs, rowNum) -> mapRow(rs, config),
                    config.getPendingValue(), config.getBatchSize());

        } else if (strategy == AcknowledgementStrategy.TIMESTAMP) {
            String processedAtCol = quoteIdentifier(config.getProcessedAtColumn());
            String sql;
            if (oracle) {
                sql = "SELECT " + selectList
                        + " FROM " + table
                        + " WHERE " + processedAtCol + " IS NULL AND ROWNUM <= ?"
                        + " ORDER BY " + idCol
                        + " FOR UPDATE SKIP LOCKED";
            } else {
                sql = "SELECT " + selectList
                        + " FROM " + table
                        + " WHERE " + processedAtCol + " IS NULL"
                        + " ORDER BY " + idCol
                        + " FETCH FIRST ? ROWS ONLY"
                        + " FOR UPDATE SKIP LOCKED";
            }
            return jdbc.query(sql, (rs, rowNum) -> mapRow(rs, config),
                    config.getBatchSize());

        } else {
            // DELETE strategy: every row present is pending
            String sql;
            if (oracle) {
                sql = "SELECT " + selectList
                        + " FROM " + table
                        + " WHERE ROWNUM <= ?"
                        + " ORDER BY " + idCol
                        + " FOR UPDATE SKIP LOCKED";
            } else {
                sql = "SELECT " + selectList
                        + " FROM " + table
                        + " ORDER BY " + idCol
                        + " FETCH FIRST ? ROWS ONLY"
                        + " FOR UPDATE SKIP LOCKED";
            }
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

        String table     = quoteIdentifier(config.getTableName());
        String idCol     = quoteIdentifier(config.getIdColumn());
        String statusCol = quoteIdentifier(config.getStatusColumn());

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

        String table = quoteIdentifier(config.getTableName());
        String idCol = quoteIdentifier(config.getIdColumn());

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

        String table          = quoteIdentifier(config.getTableName());
        String idCol          = quoteIdentifier(config.getIdColumn());
        String processedAtCol = quoteIdentifier(config.getProcessedAtColumn());

        String sql = "UPDATE " + table
                + " SET " + processedAtCol + " = CURRENT_TIMESTAMP"
                + " WHERE " + idCol + " IN (:ids)";

        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("ids", ids);

        namedJdbc.update(sql, params);
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    /**
     * Validates and quotes the identifier, uppercasing it first when running
     * on Oracle. Oracle stores unquoted DDL identifiers as uppercase, so
     * double-quoted identifiers must also be uppercase to match.
     */
    private String quoteIdentifier(String name) {
        if (oracle) {
            return SqlIdentifier.quote(name.toUpperCase());
        }
        return SqlIdentifier.quote(name);
    }

    /** Builds the comma-separated SELECT column list. */
    private String buildSelectList(OutboxTableProperties config) {
        List<String> cols = new ArrayList<>();
        cols.add(quoteIdentifier(config.getIdColumn()));
        if (config.getKeyColumn() != null) {
            cols.add(quoteIdentifier(config.getKeyColumn()));
        }
        cols.add(quoteIdentifier(config.getPayloadColumn()));
        if (config.getTopicColumn() != null) {
            cols.add(quoteIdentifier(config.getTopicColumn()));
        }
        if (config.getHeadersColumn() != null) {
            cols.add(quoteIdentifier(config.getHeadersColumn()));
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
