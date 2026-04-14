package de.knibel.outbox.config;

import java.util.List;

/**
 * Configuration interface exposing only the information relevant to
 * the data-mapper module.
 *
 * <p>This interface is intentionally narrow: it provides the ordered
 * list of {@link MappingRule}s that define how source columns are
 * transformed into target fields.  It knows nothing about polling,
 * selection strategies, acknowledgement, or any other infrastructure
 * concern.
 *
 * <p>{@link OutboxTableProperties} implements this interface so
 * existing adapter code can pass its configuration directly.
 * External consumers of the data-mapper module may provide their own
 * lightweight implementation.
 *
 * @see de.knibel.outbox.datamapper.OutboxDataMapper
 * @see MappingRule
 */
public interface DataMapperConfig {

    /**
     * Returns the ordered list of mapping rules.
     *
     * <p>Rules are evaluated top-to-bottom; once a column is claimed by
     * a rule, later rules skip it.
     *
     * @return mapping rules (never {@code null}; may be empty)
     */
    List<MappingRule> getMappings();
}
