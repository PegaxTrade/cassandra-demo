package org.example;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.stream.IntStream;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
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

            final Random random = new Random();
            final int[] values = IntStream.generate(random::nextInt)
                    .limit(rowCount)
                    .toArray();

            final Duration durationInsert = Main.stopwatch(() -> {
                for (int value : values) {
                    Main.insertRow(client, keyspace, table, value);
                }
            });

            Main.log(String.format("Inserting %d items takes %s ms", rowCount, durationInsert.toMillis()));

            final Duration durationSelect = Main.stopwatch(() -> {
                for (int value : values) {
                    Main.selectRow(client, keyspace, table, value);
                }
            });

            Main.log(String.format("Selecting %d items takes %s ms", rowCount, durationSelect.toMillis()));
        }
    }

    private static void log(@NotNull String message) {
        System.out.format("[%s] %s%n", LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME), message);
    }

    private static Duration stopwatch(@NotNull Runnable callback) {
        final long timeStart = System.currentTimeMillis();

        callback.run();

        final long timeEnd = System.currentTimeMillis();

        return Duration.ofMillis(timeEnd - timeStart);
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

    private static ResultSet selectRow(
            @NotNull CassandraClient client,
            @NotNull String keyspace,
            @NotNull String table,
            int value
    ) {
        final String id = Integer.toString(value);

        final SimpleStatement statement = selectFrom(keyspace, table)
                .all()
                .whereColumn("id").isEqualTo(literal(id))
                .build();

        return client.execute(statement);
    }
}
