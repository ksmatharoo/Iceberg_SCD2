package org.ksm.integration;

import lombok.extern.log4j.Log4j2;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;


import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class IcebergTableCreation {

    HiveCatalog hiveCatalog;

    public IcebergTableCreation(HiveCatalog hiveCatalog) {
        this.hiveCatalog = hiveCatalog;
    }

    public Table createTable(Schema schema, List<String> partitionColumns, TableIdentifier identifier,
                               String baseLocation ) {

        if (!hiveCatalog.tableExists(identifier)) {

            String location = baseLocation + "/" + identifier.name();

            Map<String, String> tableProperties = new HashMap<String, String>();
            tableProperties.put("format-version", "2");

            PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
            partitionColumns.forEach(field -> builder.identity(field) );
            PartitionSpec spec = builder.build();

            Table table = hiveCatalog.createTable(identifier, schema, spec, location, tableProperties);
            log.info("table :{} created at : {}", identifier, table.location());
            return table;
        } else {
            Table table =  hiveCatalog.loadTable(identifier);
            log.info("table :{} already exists, at : {}", identifier, table.location());
            return table;
        }
    }

    public Table createTableFromDataset(Dataset<Row> input, List<String> partitionColumns, TableIdentifier identifier,
                             String baseLocation) {
        if (!hiveCatalog.tableExists(identifier)) {

            Schema schema = SparkSchemaUtil.convert(input.schema());
            String location = baseLocation + "/" + identifier.name();

            Map<String, String> tableProperties = new HashMap<String, String>();
            tableProperties.put("format-version", "2");

            PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
            partitionColumns.forEach(field -> builder.identity(field) );
            PartitionSpec spec = builder.build();

            Table table = hiveCatalog.createTable(identifier, schema, spec, location, tableProperties);
            log.info("table :{} created at : {}", identifier, table.location());
            return table;
        } else {
            Table table =  hiveCatalog.loadTable(identifier);
            log.info("table :{} already exists, at : {}", identifier, table.location());
            return table;
        }
    }

    public Table createTableFromDataset(Dataset<Row> input, List<String> partitionColumns, TableIdentifier identifier,
                                        String baseLocation, String transformation) {
        if (!hiveCatalog.tableExists(identifier)) {

            Schema schema = SparkSchemaUtil.convert(input.schema());
            String location = baseLocation + "/" + identifier.name();

            Map<String, String> tableProperties = new HashMap<String, String>();
            tableProperties.put("format-version", "2");

            PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
            if("day".equalsIgnoreCase(transformation)){
                partitionColumns.forEach(field -> builder.day(field));
            } else {
                partitionColumns.forEach(field -> builder.identity(field) );
            }
            PartitionSpec spec = builder.build();

            Table table = hiveCatalog.createTable(identifier, schema, spec, location, tableProperties);
            log.info("table :{} created at : {}", identifier, table.location());
            return table;
        } else {
            Table table =  hiveCatalog.loadTable(identifier);
            log.info("table :{} already exists, at : {}", identifier, table.location());
            return table;
        }
    }

}
