package de.knibel.outbox.repository;

import de.knibel.outbox.config.OutboxTableProperties;
import java.util.Map;

/**
 * Technology-independent interface for mapping raw data rows to structured
 * output maps.
 *
 * <p>This interface decouples the mapping logic from any specific persistence
 * technology (JDBC, JPA, NoSQL, etc.) and serialization format (JSON, Avro,
 * etc.).  Implementations receive a plain {@link Map} of column-name → value
 * pairs and return a transformed {@link Map} ready for serialization.
 *
 * <p>The primary implementation is
 * {@link de.knibel.outbox.jdbc.rowmapper.MappingRuleDataMapper}, which
 * processes the ordered {@link de.knibel.outbox.config.MappingRule} list
 * from configuration.
 *
 * <p><b>Modularisation:</b> this interface is intentionally placed in the
 * {@code repository} package (alongside {@link OutboxRepository} and
 * {@link AcknowledgementHandler}) so that it carries no JDBC or transport
 * dependency and can be extracted into a standalone library.
 *
 * @see de.knibel.outbox.jdbc.rowmapper.MappingRuleDataMapper
 */
public interface OutboxDataMapper {

    /**
     * Maps a single data row to a structured output map.
     *
     * @param row    column-name → value pairs from any data source
     * @param config table-specific configuration (provides mapping rules,
     *               payload column name, etc.)
     * @return mapped data as a structured map, ready for serialization
     */
    Map<String, Object> map(Map<String, Object> row, OutboxTableProperties config);
}
