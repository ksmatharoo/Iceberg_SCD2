package org.ksm.integration;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class IcebergTransactionAPI {
    public static void main(String[] args) throws NoSuchTableException {

        SparkSession spark = Utils.getSparkSession();

        Map<String, String> properties = new HashMap<String, String>();
        String userDirectory = System.getProperty("user.dir");
        String dir = "/data";
        String wareHousePath = userDirectory + dir;
        properties.put("warehouse", wareHousePath);
        properties.put("uri", "thrift://172.19.0.5:9083");


        HiveCatalog hiveCatalog = new HiveCatalog();
        hiveCatalog.setConf(spark.sparkContext().hadoopConfiguration());
        hiveCatalog.initialize("hive", properties);

        //testTable1692501104601
        String dbName = "default";
        //String tableName = "testTable" + System.currentTimeMillis();
        String tableName = "testTable1692501104601";
        TableIdentifier tableIdentifier = TableIdentifier.of(dbName, tableName);


        Dataset<Row> rowDataset = Utils.readCSVFileWithoutDate(spark, "");
        IcebergTableCreation tableCreation = new IcebergTableCreation(hiveCatalog);
        tableCreation.createTableFromDataset(rowDataset, Arrays.asList("hire_date"), tableIdentifier, wareHousePath);


        //rowDataset.writeTo(tableIdentifier.toString()).append();


        String incrementalData = "d";
        rowDataset.registerTempTable(incrementalData);

        String mergeCmd  = String.format(" MERGE INTO %s i USING d ON (i.EMPLOYEE_ID = d.EMPLOYEE_ID) " +
                "WHEN MATCHED AND i.FIRST_NAME = 'Donald' " +
                " THEN DELETE " +
                "WHEN MATCHED " +
                " THEN UPDATE " +
                " SET EMAIL = d.EMAIL " +
                "WHEN NOT MATCHED " +
                " THEN INSERT * ; " +
                "",tableName);


        spark.sql(mergeCmd).show();
        spark.sql("select * from "+tableName).show();

        Table table = hiveCatalog.loadTable(tableIdentifier);

        // Begin a new transaction
        Transaction transaction = table.newTransaction();


        // Create a DataFile with the new data file path
        //DataFile newDataFile = DataFiles.builder(table.spec()).withPath("newDataFilePath").withFileSizeInBytes(0) // Set actual size
          //      .withRecordCount(1000).build();

        //transaction.newAppend().appendFile(newDataFile).set("test","test1").commit();

        transaction.updateProperties().set("test","ksm").commit();

        transaction.commitTransaction();
        System.out.println();



       /* Table table = hiveCatalog.loadTable(tableIdentifier);


        Transaction transaction = table.newTransaction();

        // Append data to the table
        DataFile[] newFiles = transaction.newAppend()
                .appendFile(newDataPath)
                .commit();

        // Set arbitrary table properties
      *//*  TableProperties properties = new TableProperties();
        properties.set("custom_property_1", "value_1");
        properties.set("custom_property_2", "value_2");
*//*
        table.updateProperties()
                .setAll(properties).commit();

        // Validate and commit the transaction
        transaction.commitTransaction();*/


    }
}
