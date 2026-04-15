package de.knibel.outbox.datamapper;

import de.knibel.outbox.config.DataMapperConfig;
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
 * <p>The primary implementation is {@link MappingRuleDataMapper}, which
 * processes the ordered {@link de.knibel.outbox.config.MappingRule} list
 * from a {@link DataMapperConfig}.
 *
 * <p><b>Modularisation:</b> this interface lives in the {@code datamapper}
 * package together with its configuration contract ({@link DataMapperConfig})
 * and the rule-based implementation ({@link MappingRuleDataMapper}).  The
 * package carries no JDBC or transport dependency and can be extracted into
 * a standalone library.
 *
 * @see DataMapperConfig
 * @see MappingRuleDataMapper
 */
public interface OutboxDataMapper {

    /**
     * Maps a single data row to a structured output map.
     *
     * @param row    column-name → value pairs from any data source
     * @param config mapping configuration (provides the ordered list of
     *               mapping rules)
     * @return mapped data as a structured map, ready for serialization
     */
    Map<String, Object> map(Map<String, Object> row, DataMapperConfig config);
}
