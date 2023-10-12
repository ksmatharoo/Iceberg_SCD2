package org.ksm.scd;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class SCD2TestData {
    public static Dataset<Row> getInitialData(SparkSession spark) {
        List<Row> initialData = Arrays.asList(
                // primary-key<id,name>
                //----------------id----name-------------------------update_ts---------------address--------cin-----
                RowFactory.create("1", "Alice", Timestamp.valueOf("2021-01-01 00:00:00"), "123 Main St", "ABC123"),
                RowFactory.create("2", "Bob", Timestamp.valueOf("2021-01-01 00:00:00"), "456 Elm St", "XYZ456"));

        StructType schema = getSchema();

        Dataset<Row> dimension = spark.createDataFrame(initialData, schema);

        return dimension;

    }

    private static StructType getSchema() {
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.StringType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("update_ts", DataTypes.TimestampType, false),
                DataTypes.createStructField("address", DataTypes.StringType, false),
                DataTypes.createStructField("cin", DataTypes.StringType, false)});
        return schema;
    }


    public static Dataset<Row> getIncomingData1(SparkSession spark) {
        List<Row> initialData = Arrays.asList(
                // primary-key<id,name>
                //----------------id----name-------------------------update_ts---------------address--------cin-----
                RowFactory.create("1", "Alice", Timestamp.valueOf("2021-01-01 00:00:00"), "123 Main St", "ABC123"),
                RowFactory.create("1", "Alice", Timestamp.valueOf("2021-01-01 00:00:00"), "123 Main St", "ABC123"),
                RowFactory.create("2", "Bob", Timestamp.valueOf("2021-01-01 00:00:00"), "456 Elm St", "JAL"),
                RowFactory.create("3", "kSingh", Timestamp.valueOf("2022-01-05 00:00:00"), "456 Elm St", "LDH1"),
                RowFactory.create("4", "BSingh", Timestamp.valueOf("2021-01-01 00:00:00"), "456 Elm St", "LDH")
        );
        StructType schema = getSchema();
        Dataset<Row> incDS = spark.createDataFrame(initialData, schema);

        return incDS;

    }

    public static Dataset<Row> getIncomingData(SparkSession spark) {
        List<Row> initialData = Arrays.asList(
                // primary-key<id,name>
                //----------------id----name-------------------------update_ts---------------address--------cin-----
                RowFactory.create("3", "kSingh", Timestamp.valueOf("2022-01-05 00:00:00"), "456 Elm St", "LDH1"),
                RowFactory.create("4", "BSingh", Timestamp.valueOf("2021-03-01 00:00:00"), "456 Elm St", "LDH_UPDATED"),
                RowFactory.create("5", "DSingh", Timestamp.valueOf("2021-01-01 00:00:00"), "456 Elm St", "LDH")
        );
        StructType schema = getSchema();
        Dataset<Row> incDS = spark.createDataFrame(initialData, schema);

        return incDS;

    }
}
