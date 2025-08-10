package io.github.heartblast.flink.connector.jdbc.sybase.database.dialect;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.core.database.dialect.AbstractDialectConverter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

/**
 * Runtime converter that is responsible for converting between JDBC object
 * and Flink internal object for Sybase ASE.
 */
@Internal
public class SybaseDialectConverter extends AbstractDialectConverter {

    private static final long serialVersionUID = 1L;

    public SybaseDialectConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    public String converterName() {
        return "sybase";
    }

    @Override
    protected JdbcDeserializationConverter createInternalConverter(LogicalType type) {
        LogicalTypeRoot root = type.getTypeRoot();
        switch (root) {
            case TINYINT:
                // Sybase ASE TINYINT is UNSIGNED (0..255).
                // Avoid byteValue() (signed) -> preserve value as Short.
                return (val) -> {
                    if (val == null) {
                        return null;
                    }
                    if (val instanceof Number) {
                        int i = ((Number) val).intValue();
                        return toUnsignedTinyShort(i);
                    }
                    if (val instanceof byte[]) {
                        byte[] b = (byte[]) val;
                        if (b.length == 0) {
                            return null; // Protecting against the possibility that the JDBC driver will return empty bytes
                        }
                        int i = Byte.toUnsignedInt(b[0]);
                        return toUnsignedTinyShort(i);
                    }
                    if (val instanceof CharSequence) {
                        int i = parseIntStrict(val.toString());
                        return toUnsignedTinyShort(i);
                    }
                    throw typeError("TINYINT", val);
                };

            default:
                return super.createInternalConverter(type);
        }
    }

    // -------------------- helpers --------------------

    /** After checking 0..255, return as Short (to prevent sign loss). */
    private static Short toUnsignedTinyShort(int i) {
        if (i < 0 || i > 255) {
            throw new IllegalArgumentException("Sybase TINYINT out of range: " + i + " (expected 0..255)");
        }
        return (short) i;
    }

    private static int parseIntStrict(String s) {
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid numeric string for TINYINT: '" + s + "'", e);
        }
    }

    private static IllegalArgumentException typeError(String logical, Object val) {
        String t = (val == null) ? "null" : val.getClass().getName();
        return new IllegalArgumentException("Unsupported value type for " + logical + ": " + t);
    }
}
