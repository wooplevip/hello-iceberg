package com.woople.iceberg.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

public class JavaAPIExample {
    public static void main(String[] args) {
        createTable();
    }

    public static void query() {
        Configuration conf = new Configuration();
        String warehousePath = "warehouse";
        HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
        TableIdentifier name = TableIdentifier.of("iceberg_db", "movielens_ratings");
        Table table = catalog.loadTable(name);
        CloseableIterable<Record> result = IcebergGenerics.read(table)
                .where(Expressions.and(
                        Expressions.greaterThan("userId", 660),
                        Expressions.greaterThan("rating", 4),
                        Expressions.greaterThan("moviedId", 130000)))
                .select("userId", "moviedId", "rating")
                .build();

        result.forEach(x -> System.out.println(x.toString()));
    }

    public static void descTable() {
        Configuration conf = new Configuration();
        String warehousePath = "warehouse";
        HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
        TableIdentifier name = TableIdentifier.of("iceberg_db", "logs01");
        Table table = catalog.loadTable(name);

        System.out.println(table.schema().columns().get(1).isOptional());
        System.out.println(table.schema().columns().get(1).isRequired());
        System.out.println(table.specs());
    }

    public static void createTable() {
        Configuration conf = new Configuration();
        String warehousePath = "warehouse";
        HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);

        Schema schema = new Schema(
                Types.NestedField.optional(1, "uuid", Types.StringType.get()),
                Types.NestedField.optional(2, "level", Types.StringType.get()),
                Types.NestedField.optional(3, "ts", Types.TimestampType.withoutZone()),
                Types.NestedField.optional(4, "message", Types.StringType.get())
        );
        TableIdentifier name = TableIdentifier.of("iceberg_db", "logs01");
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .hour("ts")
                .identity("level")
                .build();
        Table table = catalog.createTable(name, schema, spec);

        System.out.println(table.schema());
    }
}
