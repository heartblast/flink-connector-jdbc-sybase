package io.github.heartblast.flink.connector.jdbc.sybase.database.dialect;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.core.database.dialect.AbstractDialectConverter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/**
 * Runtime converter that is responsible for converting between JDBC object and Flink internal object for Sybase ASE.
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
        switch (type.getTypeRoot()) {
            case TINYINT:
                // Sybase ASE TINYINT can be mapped to Java Short or Integer
                return val -> ((Number) val).byteValue();
            default:
                return super.createInternalConverter(type);
        }
    }
}

