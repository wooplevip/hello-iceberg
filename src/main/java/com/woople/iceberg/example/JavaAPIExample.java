package com.woople.iceberg.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;

public class JavaAPIExample {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        String warehousePath = "warehouse";
        HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
        TableIdentifier name = TableIdentifier.of("iceberg_db", "movielens_ratings");
        Table table = catalog.loadTable(name);

        System.out.println(table.schemas());

        CloseableIterable<Record> result = IcebergGenerics.read(table)
                .where(Expressions.and(
                        Expressions.greaterThan("userId", 660),
                        Expressions.greaterThan("rating", 4),
                        Expressions.greaterThan("moviedId", 130000)))
                .select("userId", "moviedId", "rating")
                .build();

        result.forEach(x -> System.out.println(x.toString()));
    }
}
