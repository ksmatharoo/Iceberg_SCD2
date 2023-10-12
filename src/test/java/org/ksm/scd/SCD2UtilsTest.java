package org.ksm.scd;

import junit.framework.TestCase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.ksm.integration.IcebergTableCreation;

import java.util.Arrays;
import java.util.List;

public class SCD2UtilsTest extends TestCase {

    public void testGenerateGivenColumnWithMD5() {

            SparkSession spark = SparkSession.builder()
                    .appName("RowMD5Generator")
                    .master("local[*]") // Set your Spark master here
                    .getOrCreate();

            // Sample DataFrame
            StructType schema = DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("name", DataTypes.StringType, false),
                    DataTypes.createStructField("age", DataTypes.IntegerType, false),
                    DataTypes.createStructField("city", DataTypes.StringType, false)
            });

            List<Row> data = Arrays.asList(
                    RowFactory.create("Alice", 25, "New York"),
                    RowFactory.create("Bob", 30, "San Francisco"),
                    RowFactory.create("Charlie", 28, "Los Angeles")
            );

            Dataset<Row> df = spark.createDataFrame(data, schema);

            // List of columns to ignore
            List<String> ignoreColumns = Arrays.asList("age");


            // Generate MD5 hash column
         /*   df = IcebergTableCreation.SCD2Utils.generateGivenColumnWithMD5(df, "row_md5", Arrays.asList(), ignoreColumns);

            List<String> selectColumns = Arrays.asList("name");

            df = IcebergTableCreation.SCD2Utils.generateGivenColumnWithMD5(df, "row_key", selectColumns, Arrays.asList());
*/
            df.show(false);

            spark.stop();

    }
}