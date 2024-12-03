package org.example;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import java.net.InetSocketAddress;

import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createKeyspace;

public class CassandraClient {
    private final CqlSession session;

    public CassandraClient(String ip, int port) {
        this.session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(ip, port))
                .build();
    }

    public ResultSet execute(String query) {
        return this.session.execute(query);
    }

    public ResultSet execute(SimpleStatement statement) {
        return this.session.execute(statement);
    }

    public void createKeyspaceIfNotExists(String name, int replicationFactor) {
        final SimpleStatement statement = createKeyspace(name)
                .ifNotExists()
                .withSimpleStrategy(replicationFactor)
                .build();

        this.execute(statement);
    }
}
