package org.example;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createKeyspace;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.dropTable;

public class CassandraClient implements AutoCloseable {
    private final CqlSession session;

    public CassandraClient(String datacenter, String[] ips, int port) {
        final DriverConfigLoader loader = DriverConfigLoader.programmaticBuilder()
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(60))
                .build();

        final List<InetSocketAddress> contactPoints = Arrays.stream(ips)
                .map((ip) -> new InetSocketAddress(ip, port))
                .toList();

        this.session = CqlSession.builder()
                .withConfigLoader(loader)
                .addContactPoints(contactPoints)
                .withLocalDatacenter(datacenter)
                .build();
    }

    public void close() {
        this.session.close();
    }

    public ResultSet execute(SimpleStatement statement) {
        return this.session.execute(statement);
    }

    public void createKeyspaceIfNotExists(String keyspace, int replicationFactor) {
        final SimpleStatement statement = createKeyspace(keyspace)
                .ifNotExists()
                .withSimpleStrategy(replicationFactor)
                .build();

        this.execute(statement);
    }

    public void dropTableIfExists(String keyspace, String table) {
        final SimpleStatement statement = dropTable(keyspace, table)
                .ifExists()
                .build();

        this.execute(statement);
    }

    public int count(String keyspace, String table) {
        final SimpleStatement statement = selectFrom(keyspace, table)
                .countAll()
                .build();

        final Row row = this.execute(statement).one();

        return (row == null) ? 0 : row.getInt("count");
    }
}
