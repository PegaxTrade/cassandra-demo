package org.example;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createTable;

public class Main {
    public static void main(String[] args) {
        final String datacenter = "datacenter1";
        final String[] ips = new String[]{
                "192.168.0.163",
                "192.168.0.24",
                "192.168.0.214"
        };
        final int port = 9042;
        final String keyspace = "demo";
        final int replicationFactor = 3;
        final String table = "lorem";
        final int rowCount = 1000;

        try (final CassandraClient client = new CassandraClient(datacenter, ips, port)) {
            Main.log("createKeyspaceIfNotExists");

            client.createKeyspaceIfNotExists(keyspace, replicationFactor);

            Main.log("dropTableIfExists");

            client.dropTableIfExists(keyspace, table);

            Main.log("tableCreate");

            Main.tableCreate(client, keyspace, table);

            Main.log("insertRows");

            final LocalDateTime timeStart = LocalDateTime.now();

            Main.insertRows(client, keyspace, table, rowCount);

            final LocalDateTime timeEnd = LocalDateTime.now();

            final Duration duration = Duration.between(timeStart, timeEnd);

            Main.log(String.format("Inserting %d items takes %s milliseconds", rowCount, duration.toMillis()));
        }
    }

    private static void log(String message) {
        System.out.format("[%s] %s%n", LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME), message);
    }

    private static void tableCreate(
            @NotNull CassandraClient client,
            @NotNull String keyspace,
            @NotNull String table
    ) {
        SimpleStatement statement = createTable(keyspace, table)
                .ifNotExists()
                .withPartitionKey("id", DataTypes.TEXT)
                .withClusteringColumn("value", DataTypes.INT)
                .withColumn("hex", DataTypes.TEXT)
                .withClusteringOrder("value", ClusteringOrder.DESC)
                .build();

        client.execute(statement);
    }

    private static void insertRows(
            @NotNull CassandraClient client,
            @NotNull String keyspace,
            @NotNull String table,
            int rowCount
    ) {
        Random random = new Random();

        for (int i = 0; i < rowCount; i++) {
            Main.insertRow(client, keyspace, table, random.nextInt());
        }
    }

    private static void insertRow(
            @NotNull CassandraClient client,
            @NotNull String keyspace,
            @NotNull String table,
            int value
    ) {
        final String id = Integer.toString(value);
        final String hex = Integer.toHexString(value);

        final SimpleStatement statement = insertInto(keyspace, table)
                .value("id", literal(id))
                .value("value", literal(value))
                .value("hex", literal(hex))
                .build();

        client.execute(statement);
    }
}
