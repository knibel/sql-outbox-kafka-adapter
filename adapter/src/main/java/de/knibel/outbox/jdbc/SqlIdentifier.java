package de.knibel.outbox.jdbc;

import java.util.regex.Pattern;

/**
 * Utility for validating and double-quoting SQL identifiers (table names,
 * column names) that originate from configuration rather than user input.
 *
 * <p>Even though configuration values are not direct user input, they must
 * still be validated before embedding in SQL, because {@link
 * java.sql.PreparedStatement} bind parameters cannot be used for identifiers.
 * Double-quoting ensures reserved words are handled safely.
 *
 * <p>Valid identifiers: start with a letter or underscore, followed by
 * letters, digits, or underscores – matching {@code ^[a-zA-Z_][a-zA-Z0-9_]*$}.
 */
public final class SqlIdentifier {

    private static final Pattern SAFE = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");

    private SqlIdentifier() {}

    /**
     * Validates {@code name} and returns it wrapped in double-quotes.
     *
     * @throws IllegalArgumentException if {@code name} is null, blank, or
     *                                  contains characters outside
     *                                  {@code [a-zA-Z_][a-zA-Z0-9_]*}.
     */
    public static String quote(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("SQL identifier must not be null or blank");
        }
        if (!SAFE.matcher(name).matches()) {
            throw new IllegalArgumentException(
                    "Unsafe SQL identifier '" + name + "': only letters, digits, and underscores allowed");
        }
        return "\"" + name + "\"";
    }
}
