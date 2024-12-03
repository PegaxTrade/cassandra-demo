package org.example;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import org.jetbrains.annotations.NotNull;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createTable;

public class Main {
    public static void main(String[] args) {
        final String ip = "192.168.0.163";
        final int port = 9042;
        final String keyspace = "demo";
        final int replicationFactor = 3;
        final String table = "lorem";
        final int rowCount = 1000;

        final CassandraClient client = new CassandraClient(ip, port);

        client.createKeyspaceIfNotExists(keyspace, replicationFactor);

        Main.initTable(client, keyspace, table);
        Main.insertRows(client, keyspace, table, rowCount);
    }

    private static void initTable(
            @NotNull CassandraClient client,
            @NotNull String keyspace,
            @NotNull String table
    ) {
        SimpleStatement statement = createTable(keyspace, table)
                .ifNotExists()
                .withPartitionKey("id", DataTypes.TEXT)
                .withColumn("value", DataTypes.INT)
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
        for (int i = 0; i < rowCount; i++) {
            Main.insertRow(client, keyspace, table, i);
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
