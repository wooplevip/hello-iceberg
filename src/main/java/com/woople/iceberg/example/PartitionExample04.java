package com.woople.iceberg.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;

import static org.apache.iceberg.expressions.Expressions.bucket;


public class PartitionExample04 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        String warehousePath = "warehouse";
        HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
        TableIdentifier name = TableIdentifier.of("iceberg_db", "movielens_ratings");
        Table table = catalog.loadTable(name);

        //removePartition(table);
        addPartition(table);
    }

    public static void addPartition(Table table) {
        table.updateSpec()
                .addField(bucket("userId", 4))
                //.addField("userId")
                .commit();
    }

    public static void removePartition(Table table) {
        table.updateSpec()
                .removeField(bucket("userId", 4))
                .commit();
    }

}
