package de.knibel.outbox.jdbc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.knibel.outbox.config.OutboxTableProperties;
import de.knibel.outbox.domain.OutboxRecord;
import de.knibel.outbox.jdbc.acknowledgement.CustomAcknowledgementHandler;
import de.knibel.outbox.jdbc.acknowledgement.DeleteAcknowledgementHandler;
import de.knibel.outbox.jdbc.acknowledgement.StatusAcknowledgementHandler;
import de.knibel.outbox.jdbc.acknowledgement.TimestampAcknowledgementHandler;
import de.knibel.outbox.jdbc.rowmapper.CamelCaseRowMapper;
import de.knibel.outbox.jdbc.rowmapper.CustomFieldRowMapper;
import de.knibel.outbox.jdbc.rowmapper.MappingRuleRowMapper;
import de.knibel.outbox.jdbc.rowmapper.PayloadColumnRowMapper;
import de.knibel.outbox.jdbc.selection.CustomQuerySelectionStrategy;
import de.knibel.outbox.jdbc.selection.SelectionQuery;
import de.knibel.outbox.jdbc.selection.SelectionStrategy;
import de.knibel.outbox.jdbc.selection.SimpleSelectionStrategy;
import de.knibel.outbox.repository.AcknowledgementHandler;
import de.knibel.outbox.repository.OutboxRepository;
import de.knibel.outbox.repository.OutboxRowMapper;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 * JDBC-based implementation of {@link OutboxRepository}.
 *
 * <p>Uses {@link SelectionStrategy} to build the claim query and
 * {@link OutboxRowMapper} to map result-set rows to structured payload
 * maps, which are then serialized to JSON via Jackson.
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
        OutboxRowMapper rowMapper = resolveRowMapper(config);
        SelectionStrategy selectionStrategy = resolveSelectionStrategy(config);

        SelectionQuery query = selectionStrategy.buildClaimQuery(config);

        return jdbc.query(query.sql(),
                (rs, rowNum) -> mapRow(rs, config, rowMapper),
                query.params());
    }

    @Override
    @Transactional
    public void acknowledge(OutboxTableProperties config, List<String> ids) {
        if (ids.isEmpty()) return;
        AcknowledgementHandler handler = resolveAcknowledgementHandler(config);
        handler.acknowledge(config, ids);
    }

    // ── Strategy resolution ──────────────────────────────────────────────────

    private SelectionStrategy resolveSelectionStrategy(OutboxTableProperties config) {
        if (config.getCustomQuery() != null && !config.getCustomQuery().isBlank()) {
            return new CustomQuerySelectionStrategy();
        }
        return new SimpleSelectionStrategy();
    }

    private OutboxRowMapper resolveRowMapper(OutboxTableProperties config) {
        // New unified mappings take precedence
        if (config.getMappings() != null && !config.getMappings().isEmpty()) {
            // Check for raw target shortcut – use PayloadColumnRowMapper directly
            for (var rule : config.getMappings()) {
                if (rule.isRawTarget() && rule.getSource() != null) {
                    return new PayloadColumnRowMapper(rule.getSource());
                }
            }
            return new MappingRuleRowMapper(config.getMappings());
        }
        // Legacy strategy resolution
        return switch (config.getRowMappingStrategy()) {
            case TO_CAMEL_CASE -> new CamelCaseRowMapper(config.getStaticFields());
            case CUSTOM        -> new CustomFieldRowMapper(
                    config.getFieldMappings(), config.getCompiledColumnPatterns(),
                    config.getListMappings(), config.getStaticFields());
            default            -> new PayloadColumnRowMapper(config.getPayloadColumn());
        };
    }

    private AcknowledgementHandler resolveAcknowledgementHandler(OutboxTableProperties config) {
        return switch (config.getAcknowledgementStrategy()) {
            case DELETE    -> new DeleteAcknowledgementHandler(namedJdbc);
            case TIMESTAMP -> new TimestampAcknowledgementHandler(namedJdbc);
            case CUSTOM    -> new CustomAcknowledgementHandler(jdbc);
            default        -> new StatusAcknowledgementHandler(namedJdbc);
        };
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    /** Maps a result-set row to an {@link OutboxRecord}. */
    private OutboxRecord mapRow(java.sql.ResultSet rs, OutboxTableProperties config,
                                OutboxRowMapper rowMapper)
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
            // PayloadColumnRowMapper: read the raw string directly via
            // rs.getString() to handle database-specific types (e.g. Oracle CLOB)
            if (rowMapper instanceof PayloadColumnRowMapper pcm) {
                payload = rs.getString(pcm.getPayloadColumn());
            } else {
                Map<String, Object> row = ResultSetConverter.toMap(rs);
                Map<String, Object> mapped = rowMapper.mapRow(row);
                payload = objectMapper.writeValueAsString(mapped);
            }
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
