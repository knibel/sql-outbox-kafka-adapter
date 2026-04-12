package de.knibel.outbox.jdbc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.knibel.outbox.config.OutboxTableProperties;
import de.knibel.outbox.config.RowMappingStrategy;
import de.knibel.outbox.domain.OutboxRecord;
import de.knibel.outbox.jdbc.rowmapper.CamelCasePayloadMapper;
import de.knibel.outbox.jdbc.rowmapper.CustomFieldPayloadMapper;
import de.knibel.outbox.jdbc.rowmapper.PayloadColumnMapper;
import de.knibel.outbox.jdbc.rowmapper.PayloadMapper;
import de.knibel.outbox.jdbc.selection.CustomQuerySelectionStrategy;
import de.knibel.outbox.jdbc.selection.SelectionQuery;
import de.knibel.outbox.jdbc.selection.SelectionStrategy;
import de.knibel.outbox.jdbc.selection.SimpleSelectionStrategy;
import de.knibel.outbox.repository.OutboxRepository;
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
 * JDBC-based implementation of {@link OutboxRepository}.
 *
 * <p>Uses {@link SelectionStrategy} to build the claim query and
 * {@link PayloadMapper} to map result-set rows to JSON payloads.
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
public class JdbcOutboxRepository implements OutboxRepository {

    private static final Logger log = LoggerFactory.getLogger(JdbcOutboxRepository.class);
    private static final TypeReference<Map<String, String>> MAP_TYPE = new TypeReference<>() {};

    private final JdbcTemplate jdbc;
    private final NamedParameterJdbcTemplate namedJdbc;
    private final ObjectMapper objectMapper;

    public JdbcOutboxRepository(JdbcTemplate jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.namedJdbc = new NamedParameterJdbcTemplate(jdbc);
        this.objectMapper = objectMapper;
    }

    // ── Public API ──────────────────────────────────────────────────────────

    @Override
    @Transactional
    public List<OutboxRecord> claimBatch(OutboxTableProperties config) {
        PayloadMapper payloadMapper = resolvePayloadMapper(config);
        SelectionStrategy selectionStrategy = resolveSelectionStrategy(config);

        String selectList = payloadMapper.buildSelectList(config);
        SelectionQuery query = selectionStrategy.buildClaimQuery(config, selectList);

        return jdbc.query(query.sql(),
                (rs, rowNum) -> mapRow(rs, config, payloadMapper),
                query.params());
    }

    @Override
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

    @Override
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

    @Override
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

    @Override
    @Transactional
    public void executeCustomAcknowledgement(OutboxTableProperties config, List<String> ids) {
        if (ids.isEmpty()) return;

        String sql = config.getCustomAcknowledgementQuery();
        for (String id : ids) {
            jdbc.update(sql, id);
        }
    }

    // ── Strategy resolution ──────────────────────────────────────────────────

    private SelectionStrategy resolveSelectionStrategy(OutboxTableProperties config) {
        if (config.getCustomQuery() != null && !config.getCustomQuery().isBlank()) {
            return new CustomQuerySelectionStrategy();
        }
        return new SimpleSelectionStrategy();
    }

    private PayloadMapper resolvePayloadMapper(OutboxTableProperties config) {
        return switch (config.getRowMappingStrategy()) {
            case TO_CAMEL_CASE -> new CamelCasePayloadMapper(objectMapper);
            case CUSTOM        -> new CustomFieldPayloadMapper(objectMapper);
            default            -> new PayloadColumnMapper();
        };
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    /** Maps a result-set row to an {@link OutboxRecord}. */
    private OutboxRecord mapRow(java.sql.ResultSet rs, OutboxTableProperties config,
                                PayloadMapper payloadMapper)
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
        try {
            payload = payloadMapper.mapPayload(rs, config);
        } catch (Exception e) {
            log.warn("Failed to build payload for table '{}', row id='{}': {}",
                    config.getTableName(), id, e.getMessage());
            payload = "{}";
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
