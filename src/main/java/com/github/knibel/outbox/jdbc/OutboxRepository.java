package com.github.knibel.outbox.jdbc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.knibel.outbox.config.OutboxTableProperties;
import com.github.knibel.outbox.domain.OutboxRecord;
import com.github.knibel.outbox.domain.SqlFragment;
import com.github.knibel.outbox.domain.StatusStrategy;
import com.github.knibel.outbox.domain.StatusStrategyFactory;
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
 * Values are always passed as positional {@code ?} bind parameters.
 *
 * <p><b>Concurrency:</b> {@link #claimBatch} uses
 * {@code FOR UPDATE SKIP LOCKED} so multiple application instances (or
 * multiple poller threads) never process the same row simultaneously.
 *
 * <p><b>Database compatibility:</b> all generated SQL is standard JDBC SQL.
 * The only non-standard feature used is {@code FOR UPDATE SKIP LOCKED}, which
 * is supported by PostgreSQL 9.5+, MySQL 8.0+, and Oracle 12c+.  No
 * database-specific extensions (CTEs with {@code RETURNING}, vendor interval
 * functions, etc.) are used.
 */
@Repository
public class OutboxRepository {

    private static final Logger log = LoggerFactory.getLogger(OutboxRepository.class);
    private static final TypeReference<Map<String, String>> MAP_TYPE = new TypeReference<>() {};

    private final JdbcTemplate jdbc;
    private final NamedParameterJdbcTemplate namedJdbc;
    private final ObjectMapper objectMapper;
    private final StatusStrategyFactory strategyFactory;

    public OutboxRepository(JdbcTemplate jdbc,
                            ObjectMapper objectMapper,
                            StatusStrategyFactory strategyFactory) {
        this.jdbc = jdbc;
        this.namedJdbc = new NamedParameterJdbcTemplate(jdbc);
        this.objectMapper = objectMapper;
        this.strategyFactory = strategyFactory;
    }

    // ── Public API ──────────────────────────────────────────────────────────

    /**
     * Claims up to {@link OutboxTableProperties#getBatchSize()} pending rows
     * and, for strategies with an IN_PROGRESS state, atomically marks them
     * in-progress within the same database transaction.
     *
     * <p>Uses {@code FOR UPDATE SKIP LOCKED} to prevent concurrent instances
     * from claiming the same rows.
     *
     * @return the claimed rows mapped to {@link OutboxRecord}s; empty list if
     *         no pending rows exist.
     */
    @Transactional
    public List<OutboxRecord> claimBatch(OutboxTableProperties config) {
        StatusStrategy strategy = strategyFactory.getStrategy(config.getStatusStrategy());
        return strategy.hasInProgressState()
                ? claimWithInProgress(config, strategy)
                : selectPending(config, strategy);
    }

    /**
     * Marks all given rows as done (successfully published to Kafka).
     */
    @Transactional
    public void markDone(OutboxTableProperties config, List<String> ids) {
        if (ids.isEmpty()) return;
        StatusStrategy strategy = strategyFactory.getStrategy(config.getStatusStrategy());
        SqlFragment setFrag = strategy.doneSetFragment(config);
        bulkUpdate(config, setFrag, ids);
    }

    /**
     * Marks all given rows as failed (could not be published to Kafka).
     */
    @Transactional
    public void markFailed(OutboxTableProperties config, List<String> ids) {
        if (ids.isEmpty()) return;
        StatusStrategy strategy = strategyFactory.getStrategy(config.getStatusStrategy());
        SqlFragment setFrag = strategy.failedSetFragment(config);
        bulkUpdate(config, setFrag, ids);
    }

    /**
     * Resets stuck IN_PROGRESS rows back to PENDING.
     *
     * <p>A row is considered stuck when it has been in IN_PROGRESS state for
     * longer than {@link OutboxTableProperties#getStuckTtlSeconds()} seconds.
     * This handles JVM crashes where a batch was claimed but never completed.
     *
     * <p>Does nothing if the strategy does not support stuck-row detection
     * (i.e. when {@link StatusStrategy#stuckClause} returns {@code null}).
     */
    @Transactional
    public int resetStuck(OutboxTableProperties config) {
        StatusStrategy strategy = strategyFactory.getStrategy(config.getStatusStrategy());
        SqlFragment stuckWhere = strategy.stuckClause(config);
        if (stuckWhere == null) {
            return 0;
        }
        SqlFragment resetSet = strategy.resetSetFragment(config);
        if (resetSet == null) {
            return 0;
        }

        String table = SqlIdentifier.quote(config.getTableName());
        String sql = "UPDATE " + table + " SET " + resetSet.sql()
                + " WHERE " + stuckWhere.sql();

        List<Object> params = new ArrayList<>(resetSet.params());
        params.addAll(stuckWhere.params());

        int count = jdbc.update(sql, params.toArray());
        if (count > 0) {
            log.warn("Reset {} stuck row(s) in table '{}' back to PENDING",
                    count, config.getTableName());
        }
        return count;
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    /**
     * Claims pending rows in two steps within a single database transaction:
     *
     * <ol>
     *   <li><b>SELECT … FOR UPDATE SKIP LOCKED</b> – reads the batch of pending
     *       rows and acquires a row-level exclusive lock on each one.  Rows
     *       already locked by another poller instance are silently skipped.
     *   <li><b>UPDATE … SET status = IN_PROGRESS</b> – marks those exact rows as
     *       in-progress while the locks from step 1 are still held by the open
     *       transaction.
     * </ol>
     *
     * <p>Both statements execute inside the {@link Transactional} boundary of
     * {@link #claimBatch}.  Row locks acquired in step 1 are held at the
     * <em>transaction</em> level (not the statement level), so they remain in
     * place until the transaction commits after step 2 completes.  This means
     * no other poller can claim the same rows between the two steps.
     *
     * <p>This two-step approach uses only standard JDBC SQL and is compatible
     * with any database that supports {@code FOR UPDATE SKIP LOCKED}
     * (PostgreSQL 9.5+, MySQL 8.0+, Oracle 12c+).
     */
    private List<OutboxRecord> claimWithInProgress(OutboxTableProperties config,
                                                    StatusStrategy strategy) {
        String table = SqlIdentifier.quote(config.getTableName());
        String idCol = SqlIdentifier.quote(config.getIdColumn());
        SqlFragment pendingFrag = strategy.pendingClause(config);

        String selectList = buildSelectList(config);

        // ── Step 1: read pending rows and lock them ───────────────────────
        String selectSql = "SELECT " + selectList
                + " FROM " + table
                + " WHERE " + pendingFrag.sql()
                + " ORDER BY " + idCol
                + " LIMIT ?"
                + " FOR UPDATE SKIP LOCKED";

        List<Object> selectParams = new ArrayList<>(pendingFrag.params());
        selectParams.add(config.getBatchSize());

        List<OutboxRecord> records =
                jdbc.query(selectSql, (rs, rowNum) -> mapRow(rs, config), selectParams.toArray());

        if (records.isEmpty()) return records;

        // ── Step 2: mark those rows IN_PROGRESS (locks still held) ────────
        List<String> ids = records.stream().map(OutboxRecord::id).toList();
        bulkUpdate(config, strategy.claimSetFragment(config), ids);

        return records;
    }

    /**
     * Selects pending rows using {@code FOR UPDATE SKIP LOCKED} without
     * updating them to an in-progress state (TIMESTAMP / BOOLEAN strategies).
     */
    private List<OutboxRecord> selectPending(OutboxTableProperties config,
                                              StatusStrategy strategy) {
        String table = SqlIdentifier.quote(config.getTableName());
        String idCol = SqlIdentifier.quote(config.getIdColumn());
        SqlFragment pendingFrag = strategy.pendingClause(config);

        String selectList = buildSelectList(config);

        String sql = "SELECT " + selectList
                + " FROM " + table
                + " WHERE " + pendingFrag.sql()
                + " ORDER BY " + idCol
                + " LIMIT ?"
                + " FOR UPDATE SKIP LOCKED";

        List<Object> params = new ArrayList<>(pendingFrag.params());
        params.add(config.getBatchSize());

        return jdbc.query(sql, (rs, rowNum) -> mapRow(rs, config), params.toArray());
    }

    /**
     * Executes a bulk UPDATE for the given IDs using a named parameter
     * {@code IN (:ids)} clause.
     *
     * <p>Positional {@code ?} placeholders in the SET fragment SQL are replaced
     * with named parameters ({@code :setParam0}, {@code :setParam1}, …) by
     * scanning the SQL character by character.  This is more robust than using
     * {@link String#replaceFirst} in a loop, which would fail if {@code ?}
     * appeared in unexpected positions (e.g. inside quoted strings).
     */
    private void bulkUpdate(OutboxTableProperties config, SqlFragment setFrag, List<String> ids) {
        String table = SqlIdentifier.quote(config.getTableName());
        String idCol = SqlIdentifier.quote(config.getIdColumn());

        // Convert positional ? placeholders in the SET clause to named params.
        List<Object> setParamValues = setFrag.params();
        MapSqlParameterSource params = new MapSqlParameterSource("ids", ids);
        StringBuilder namedSetSql = new StringBuilder(setFrag.sql().length() + 16);
        int paramIndex = 0;
        for (char ch : setFrag.sql().toCharArray()) {
            if (ch == '?') {
                String name = "setParam" + paramIndex;
                namedSetSql.append(':').append(name);
                params.addValue(name, setParamValues.get(paramIndex));
                paramIndex++;
            } else {
                namedSetSql.append(ch);
            }
        }

        String namedSql = "UPDATE " + table
                + " SET " + namedSetSql
                + " WHERE " + idCol + " IN (:ids)";
        namedJdbc.update(namedSql, params);
    }

    /** Builds the comma-separated SELECT column list for claim / select queries. */
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
