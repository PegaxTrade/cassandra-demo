package org.example;

import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createKeyspace;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.dropTable;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public final class CassandraClient implements AutoCloseable {
  private final CqlSession session;

  public CassandraClient(
      final String datacenter,
      final String[] ips,
      final int port
  ) {
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

  public @NotNull ResultSet execute(final SimpleStatement statement) {
    return this.session.execute(statement);
  }

  public void createKeyspaceIfNotExists(final String keyspace, final int replicationFactor) {
    final SimpleStatement statement = createKeyspace(keyspace)
        .ifNotExists()
        .withSimpleStrategy(replicationFactor)
        .build();

    this.execute(statement);
  }

  public void dropTableIfExists(final String keyspace, final String table) {
    final SimpleStatement statement = dropTable(keyspace, table)
        .ifExists()
        .build();

    this.execute(statement);
  }
}
