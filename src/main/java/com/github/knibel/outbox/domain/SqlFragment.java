package com.github.knibel.outbox.domain;

import java.util.List;

/**
 * A SQL clause fragment paired with its positional bind parameters.
 *
 * <p>Used by {@link StatusStrategy} implementations to return SQL snippets
 * that are safe to embed in prepared statements.
 *
 * @param sql    SQL fragment, may contain {@code ?} placeholders.
 * @param params Bind parameters in the same order as the {@code ?} placeholders.
 */
public record SqlFragment(String sql, List<Object> params) {

    /** Creates a fragment with no bind parameters. */
    public static SqlFragment noParams(String sql) {
        return new SqlFragment(sql, List.of());
    }

    /** Creates a fragment with one or more bind parameters. */
    public static SqlFragment of(String sql, Object... params) {
        return new SqlFragment(sql, List.of(params));
    }

    /** Returns {@code true} if this fragment has at least one bind parameter. */
    public boolean hasParams() {
        return !params.isEmpty();
    }

    /** Returns bind parameters as an array (convenience for JdbcTemplate). */
    public Object[] paramsArray() {
        return params.toArray();
    }
}
