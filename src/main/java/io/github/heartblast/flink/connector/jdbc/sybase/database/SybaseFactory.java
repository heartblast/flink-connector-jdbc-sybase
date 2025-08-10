package io.github.heartblast.flink.connector.jdbc.sybase.database;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.core.database.JdbcFactory;
import org.apache.flink.connector.jdbc.core.database.catalog.JdbcCatalog;
import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialect;

import io.github.heartblast.flink.connector.jdbc.sybase.database.dialect.SybaseDialect;

/** Factory for {@link SybaseDialect}. */
@Internal
public class SybaseFactory implements JdbcFactory {
    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:sybase:");
    }

    @Override
    public JdbcDialect createDialect() {
        return new SybaseDialect();
    }

    @Override
    public JdbcCatalog createCatalog(
            ClassLoader classLoader,
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String baseUrl) {
        throw new UnsupportedOperationException("Catalog for sybase is not supported yet.");
    }
}
