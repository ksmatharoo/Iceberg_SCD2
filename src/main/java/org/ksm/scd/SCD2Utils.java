package org.ksm.scd;

import lombok.extern.log4j.Log4j2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.*;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Log4j2
public  class SCD2Utils {

    public static String ROW_KEY = "row_key";
    public static String ROW_MD5 = "row_md5";

    public static String VALID_FROM = "valid_from";
    public static String VALID_TO = "valid_to";

    public static String DELETE_INDICATOR = "delete_indicator";

    public static String REJECT_STATUS = "reject_status";


    public static String[] SCD_COLUMN = {ROW_KEY, ROW_MD5, VALID_TO};

    public static Dataset<Row> enrichWithScdColumns(Dataset<Row> df, List<String> selectColumns, List<String> ignoreColumns) {

        df = SCD2Utils.generateGivenColumnWithMD5(df, ROW_MD5, Arrays.asList(), ignoreColumns);
        df = SCD2Utils.generateGivenColumnWithMD5(df, ROW_KEY, selectColumns, Arrays.asList());

        return df;
    }

    private static Dataset<Row> generateGivenColumnWithMD5(Dataset<Row> df, String genColName,
                                                           List<String> selectColumns, List<String> ignoreColumns) {

        final List<String> selectColumn = Objects.isNull(selectColumns) ?
                selectColumns = Arrays.asList() : selectColumns;


        final List<String> ignoreColumn = Objects.isNull(ignoreColumns) ?
                ignoreColumns = Arrays.asList() : ignoreColumns;

        if (selectColumns.size() > 0 &&
                ignoreColumns.size() > 0) {
            throw new RuntimeException("generateColumnWithMD5 either with selectColumns or ignoreColumns");
        }

        UserDefinedFunction md5UDF = functions.udf(
                (Row row) -> generateMD5ForRow(row, df.schema(), selectColumn, ignoreColumn),
                DataTypes.StringType
        );

        List<Column> collect = Stream.of(df.columns()).map(col -> new Column(col)).collect(Collectors.toList());

        return df.withColumn(genColName, md5UDF.apply(functions.struct(collect.toArray(new Column[0]))));
    }

    private static String generateMD5ForRow(Row row, StructType schema, List<String> selectColumns, List<String> ignoreColumns) {
        try {
            if (selectColumns.size() > 0 && ignoreColumns.size() > 0) {
                throw new RuntimeException("generateMD5ForRow either pass selectColumns or ignoreColumns");
            }

            MessageDigest md = MessageDigest.getInstance("MD5");
            StringBuilder input = new StringBuilder();
            if (selectColumns.size() > 0) {
                for (int i = 0; i < row.size(); i++) {
                    String columnName = schema.fields()[i].name();
                    if (selectColumns.contains(columnName)) {
                        input.append(row.get(i));
                    }
                }
            } else {
                for (int i = 0; i < row.size(); i++) {
                    String columnName = schema.fields()[i].name();
                    if (!ignoreColumns.contains(columnName)) {
                        input.append(row.get(i));
                    }
                }
            }
            byte[] hashBytes = md.digest(input.toString().getBytes(StandardCharsets.UTF_8));
            StringBuilder hashHex = new StringBuilder();
            for (byte b : hashBytes) {
                hashHex.append(String.format("%02x", b & 0xFF));
            }
            return hashHex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 algorithm not found.", e);
        }
    }


    public static Dataset<Row> addValidFromAndValidTo(Dataset<Row> initialData,String validFromColumnName) {
        // Add 'valid_to' column to initial data
        log.info("validFromColumnName from dataset : {}",validFromColumnName );
        initialData.printSchema();
        return initialData.withColumn(VALID_FROM,initialData.col(validFromColumnName))
                          .withColumn(VALID_TO, functions.lit(Timestamp.valueOf("9999-12-31 00:00:00")))
                          .withColumn(DELETE_INDICATOR,functions.lit("NA"));
    }

    public static Dataset<Row> addSCD2Columns(Dataset<Row> initialData,String validFromColumnName) {
        // Add 'valid_to' column to initial data
        log.info("validFromColumnName from dataset : {}",validFromColumnName );
        initialData.printSchema();
        return initialData.withColumn(VALID_FROM,initialData.col(validFromColumnName))
                .withColumn(VALID_TO, functions.lit(Timestamp.valueOf("9999-12-31 00:00:00")))
                .withColumn(DELETE_INDICATOR,functions.lit("NA"));
    }



    public static Map<String, Integer> getColumnNameIndexMap(String[] columns) {
        Map<String, Integer> colNameIndex = new HashMap<>();
        for (int i = 0; i < columns.length; i++) {
            colNameIndex.put(columns[i], i);
        }
        return colNameIndex;
    }


    public static StructType makeSchemaNonNullable(StructType originalSchema) {
        StructField[] fields = originalSchema.fields();
        StructField[] nonNullableFields = new StructField[fields.length];

        for (int i = 0; i < fields.length; i++) {
            StructField field = fields[i];
            DataType dataType = field.dataType();

            // Create a new StructField with non-nullable type
            StructField nonNullableField = new StructField(
                    field.name(),
                    dataType,
                    false, // Make it non-nullable
                    field.metadata()
            );

            nonNullableFields[i] = nonNullableField;
        }

        return new StructType(nonNullableFields);
    }

    public static Dataset<Row> makeNonNullable(Dataset<Row> inDS){
        SparkSession session = inDS.sparkSession();
        StructType structType = makeSchemaNonNullable(inDS.schema());
        return session.createDataFrame(inDS.javaRDD(),structType);
    }


    public static String withPrefix(String prefix, String name) {
        return String.format("%s_%s", prefix, name);
    }

}