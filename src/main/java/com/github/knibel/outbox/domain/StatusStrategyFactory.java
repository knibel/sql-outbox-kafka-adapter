package com.github.knibel.outbox.domain;

import org.springframework.stereotype.Component;

/**
 * Creates the appropriate {@link StatusStrategy} instance for a given
 * {@link StatusStrategyType}.
 */
@Component
public class StatusStrategyFactory {

    public StatusStrategy getStrategy(StatusStrategyType type) {
        return switch (type) {
            case ENUM      -> new EnumStatusStrategy();
            case TIMESTAMP -> new TimestampStatusStrategy();
            case BOOLEAN   -> new BooleanStatusStrategy();
        };
    }
}
