package org.ksm.scd;


import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.spark.extensions.IcebergMergeInto;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.ksm.integration.IcebergTableCreation;
import org.ksm.integration.Utils;
import scala.Tuple2;

import java.util.*;

import static org.ksm.scd.SCD2Process.*;

public class SCD2Test {

    public static void main(String[] args) throws NoSuchTableException, ParseException {


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
        String tableName = "openTable_3sep";
        String closeTableName = "CloseTable_3sep";
        String rejectTableName = "rejectTable_3sep";
        TableIdentifier openIdentifier = TableIdentifier.of(dbName, tableName);
        TableIdentifier closeIdentifier = TableIdentifier.of(dbName, closeTableName);
        TableIdentifier rejectIdentifier = TableIdentifier.of(dbName, rejectTableName);

        //get data
        Dataset<Row> inputDataset = SCD2TestData.getIncomingData(spark);

        //scd2 context
        String[] rowKeys = {"id", "name"};
        String[] md5Ignore = {"valid_from", "valid_to", "update_ts","delete_indicator"};
        String validFromColumnName = "update_ts";

        List<String> selectColumns = Arrays.asList(rowKeys);
        List<String> ignoreColumns = new ArrayList<>();
        ignoreColumns.addAll(selectColumns);
        ignoreColumns.addAll(Arrays.asList(md5Ignore));

        //enrich with rowKey and rowMd5
        Dataset<Row> enriched = SCD2Utils.enrichWithScdColumns(inputDataset, selectColumns, ignoreColumns);

        //de-duplicate based on row_key and emit rejects
        Tuple2<Dataset<Row>, Dataset<Row>> datasetDatasetTuple = SCD2Process.deduplicateAndReject(enriched, SCD2Process.getRejectSchema());
        Dataset<Row> finalRejectDataset = datasetDatasetTuple._2();
        finalRejectDataset.show(false);
        IcebergTableCreation tableCreation = new IcebergTableCreation(hiveCatalog);
        tableCreation.createTableFromDataset(finalRejectDataset, Arrays.asList(), rejectIdentifier, wareHousePath);
        finalRejectDataset.writeTo(rejectIdentifier.toString()).append();

        //de-duplicated dataset with unique only row_keys
        enriched = datasetDatasetTuple._1();

        //add validFrom and validTo columns
        Dataset<Row> incomingData = SCD2Utils.addSCD2Columns(enriched, validFromColumnName);
        //create openClose Table if not exist
        tableCreation.createTableFromDataset(incomingData, Arrays.asList(VALID_TO), openIdentifier, wareHousePath,"day");


        //process InsertUpdatedAndClosedRecords With Where Clause
        Dataset<Row> openData = spark.sql("select * from " + openIdentifier.toString());
        Dataset<Row> insertUpdatedAndClosedRecords = SCD2Process.getInsertUpdatedAndClosedRecordsWithWhereClause(incomingData, openData);


        //write close records
        Dataset<Row> closed = insertUpdatedAndClosedRecords.where("status = " + SCD2Process.eSTATUS.CLOSED.getNumVal()).select(selectWithPrefix(insertUpdatedAndClosedRecords.columns(), SCD2Process.right));
        closed = closed.select(SCD2Process.removePrefix(closed.columns(), SCD2Process.right));
        closed.show(false);
        closed = closed.drop("status");
        //tableCreation.createTableFromDataset(closed, Arrays.asList(), closeIdentifier, wareHousePath);
        if(closed.count() > 0) {
            Dataset<Row> rowDataset = SCD2Utils.makeNonNullable(closed);
            tableCreation.createTableFromDataset(incomingData, Arrays.asList(VALID_TO), closeIdentifier, wareHousePath,"day");
            rowDataset.writeTo(closeIdentifier.toString()).append();
        }


        //write reject records
        Dataset<Row> reject = insertUpdatedAndClosedRecords.where("status = " + SCD2Process.eSTATUS.DUPLICATE_RECORD_OPEN.getNumVal()).
                select(selectWithPrefix(insertUpdatedAndClosedRecords.columns(),STATUS,SCD2Process.left));

        reject = reject.select(SCD2Process.removePrefix(reject.columns(), SCD2Process.left));
        reject.show(false);
        Dataset<Row> rejectDataset = SCD2Process.convertToRejectDataset(reject, SCD2Process.getRejectSchema());
        rejectDataset.writeTo(rejectIdentifier.toString()).append();


        //write updated And Inserted records
        Dataset<Row> updatedAndInserted = insertUpdatedAndClosedRecords.where("status =" + SCD2Process.eSTATUS.UPDATED.getNumVal() + " or status =" + SCD2Process.eSTATUS.INSERT.getNumVal()).select(selectWithPrefix(insertUpdatedAndClosedRecords.columns(), SCD2Process.left));
        updatedAndInserted = updatedAndInserted.select(SCD2Process.removePrefix(updatedAndInserted.columns(), SCD2Process.left));
        updatedAndInserted.show(false);
        updatedAndInserted = updatedAndInserted.drop("status");


        //String delta = "d";
        //updatedAndInserted.registerTempTable(delta);

        boolean test1 = true;

        ////////tests////////
        if (test1) {
            MergeHelper.executeMerge(updatedAndInserted, tableName);
        } else {
            Map<String, Column> updated = new HashMap<>();
            String[] columns = updatedAndInserted.columns();
            for (int i = 0; i < columns.length; i++) {
                updated.put(columns[i], updatedAndInserted.col(columns[i]));
            }

            Map<String, Column> inserted = new HashMap<>();
            String[] columnss = updatedAndInserted.columns();
            for (int i = 0; i < columnss.length; i++) {
                inserted.put(columnss[i], updatedAndInserted.col(columnss[i]));
            }
            IcebergMergeInto.apply(openIdentifier.toString()).
                    using(updatedAndInserted.as("source")).
                    when(String.format("%s.row_key = source.row_key", tableName)).
                    whenMatched().
                    update(updated).
                    whenNotMatched().
                    insert(inserted).
                    merge();

        }

        System.out.println("end");
        ///////////
       /* String mergeCmd = String.format(" MERGE INTO %s i USING d ON (i.row_key = d.row_key) " +
                "WHEN MATCHED " +
                " THEN UPDATE SET * " +
                "WHEN NOT MATCHED " +
                " THEN INSERT * ; " +
                "", openIdentifier);


        spark.sql(mergeCmd).show();*/


        System.out.println("test");
    }


    public static Dataset<Row> processClose(Dataset<Row> existing, Dataset<Row> newData) {

    /*    Dataset<Row> incrementalData = newData.withColumnRenamed(ROW_KEY, withPrefix("new", ROW_KEY)).
                withColumnRenamed(ROW_MD5, withPrefix("new", ROW_MD5)).
                withColumnRenamed(VALID_FROM, withPrefix("new", VALID_FROM)).
                withColumnRenamed(VALID_TO, withPrefix("new", VALID_TO));

        Dataset<Row> joined = existing.join(incrementalData, ROW_KEY).withColumn(REJECT_STATUS, functions.lit(0));
*/
     /*   String[] columns = joined.columns();
        Map<String, Integer> colNameIndex = new HashMap<>();
        for (int i = 0; i < columns.length; i++) {
            colNameIndex.put(columns[i], i);
        }*/


/*        Dataset<Row> rowDataset = joined.mapPartitions(new MapPartitionsFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Iterator<Row> iterator) throws Exception {

                return new Iterator<Row>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public Row next() {
                        Row row = iterator.next();
                        int size = row.size();
                        Object[] values = ((GenericRow) row).values();

                        String existingMD5 = row.getAs(ROW_MD5);
                        String newMD5 = row.getAs(withPrefix("new", ROW_MD5));
                        if (existingMD5.equalsIgnoreCase(newMD5)) {
                            values[size - 1] = STATUS.DUPLICATE_RECORD_OPEN.getNumVal();
                        } else {
                            values[size - 1] = STATUS.CLOSED.getNumVal();

                            int newRowValidFromIndex = colNameIndex.get(withPrefix("new", VALID_FROM));
                            long newRowTimeStamp = ((Timestamp) values[newRowValidFromIndex]).getTime() - 1;

                            values[colNameIndex.get(VALID_TO)] = new Timestamp(newRowTimeStamp);
                            // update valid to currentTimeStamp
                        }
                        return row;
                    }
                };
            }
        }, RowEncoder.apply(joined.schema()));*/
        return null;
    }





}
