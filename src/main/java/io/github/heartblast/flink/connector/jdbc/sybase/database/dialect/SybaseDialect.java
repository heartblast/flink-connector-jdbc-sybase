package io.github.heartblast.flink.connector.jdbc.sybase.database.dialect;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.core.database.dialect.AbstractDialect;
import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialectConverter;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** JDBC dialect for Sybase ASE. */
@Internal
public class SybaseDialect extends AbstractDialect {

    @Override
    public String dialectName() {
        return "sybase";
    }

    @Override
    public Optional<Range> timestampPrecisionRange() {
        // Sybase ASE supports TIMESTAMP up to 6 digits by default.
        return Optional.of(Range.of(0, 6));
    }

    @Override
    public Optional<Range> decimalPrecisionRange() {
        return Optional.of(Range.of(1, 38));
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("com.sybase.jdbc4.jdbc.SybDriver");
    }

    @Override
    public String quoteIdentifier(String identifier) {
        // Sybase ASE can be used without quotes by default, but square brackets or single quotes are also acceptable.
        // Safely enclosing with "" is preferred.
        //return "\"" + identifier + "\"";
        return identifier;
    }

    @Override
    public Optional<String> getUpsertStatement(
            String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        List<String> nonUniqueKeyFields =
                Arrays.stream(fieldNames)
                        .filter(f -> !Arrays.asList(uniqueKeyFields).contains(f))
                        .collect(Collectors.toList());

        String fieldsProjection = Arrays.stream(fieldNames)
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));

        String valuesBinding = Arrays.stream(fieldNames)
                .map(f -> ":" + f)
                .collect(Collectors.joining(", "));

        // Sybase ASE lacks standard MERGE support, so a transaction-based UPSERT pattern must be used.
        return Optional.empty();
    }

    @Override
    public JdbcDialectConverter getRowConverter(RowType rowType) {
        return new SybaseDialectConverter(rowType);
    }

    @Override
    public String getLimitClause(long limit) {
        // Sybase ASE uses SET ROWCOUNT instead of LIMIT
        return "SET ROWCOUNT " + limit;
    }

    @Override
    public Set<LogicalTypeRoot> supportedTypes() {
        return EnumSet.of(
                LogicalTypeRoot.CHAR,
                LogicalTypeRoot.VARCHAR,
                LogicalTypeRoot.BOOLEAN,
                LogicalTypeRoot.BINARY,
                LogicalTypeRoot.VARBINARY,
                LogicalTypeRoot.DECIMAL,
                LogicalTypeRoot.TINYINT,
                LogicalTypeRoot.SMALLINT,
                LogicalTypeRoot.INTEGER,
                LogicalTypeRoot.BIGINT,
                LogicalTypeRoot.FLOAT,
                LogicalTypeRoot.DOUBLE,
                LogicalTypeRoot.DATE,
                LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE,
                LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
                // Sybase ASE has weak support for TIMEZONE, so the WITH TIMEZONE series is excluded.
        );
    }
}
