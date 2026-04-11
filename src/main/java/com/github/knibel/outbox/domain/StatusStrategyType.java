package com.github.knibel.outbox.domain;

/**
 * Selects which strategy is used to track row processing status.
 */
public enum StatusStrategyType {

    /**
     * Uses string enum values (e.g. PENDING / IN_PROGRESS / DONE).
     * Supports a three-phase flow and stuck-row recovery.
     * <strong>Recommended for effectively-once semantics.</strong>
     */
    ENUM,

    /**
     * Uses a nullable timestamp column: {@code NULL} = pending,
     * {@code CURRENT_TIMESTAMP} = done.
     * Two-phase only (no IN_PROGRESS state). At-least-once semantics.
     */
    TIMESTAMP,

    /**
     * Uses a boolean column: {@code false} = pending, {@code true} = done.
     * Two-phase only (no IN_PROGRESS state). At-least-once semantics.
     */
    BOOLEAN
}
