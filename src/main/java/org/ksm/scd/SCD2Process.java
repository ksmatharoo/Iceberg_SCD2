package org.ksm.scd;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;


@Log4j2
public class SCD2Process {

    public static String ROW_KEY = "row_key";
    public static String ROW_MD5 = "row_md5";
    public static String VALID_TO = "valid_to";
    public static String VALID_FROM = "valid_from";
    public static String STATUS = "status";
    public static String left = "left_";
    public static String right = "right_";


    public enum eSTATUS {
        NA(0),
        DUPLICATE_IN_MICRO_BATCH(1),
        CLOSED(2),
        DUPLICATE_RECORD_OPEN(3),
        UPDATED(4),
        INSERT(5);
        private int numVal;

        eSTATUS(int numVal) {
            this.numVal = numVal;
        }

        public int getNumVal() {
            return numVal;
        }

        // Static method to initialize enum based on the integer value
        public static eSTATUS fromInt(int value) {
            for (eSTATUS status : eSTATUS.values()) {
                if (status.numVal == value) {
                    return status;
                }
            }
            throw new IllegalArgumentException("Invalid integer value for eSTATUS: " + value);
        }
    }


    /****
     *
     * get insert, update deleted data
     * @return
     */
    public static Dataset<Row> leftJoinWithColumnPrefix(Dataset<Row> leftTable, Dataset<Row> rightTable) {

        leftTable.show(false);
        rightTable.show(false);

        leftTable = leftTable.select(generateColumnExpressions(leftTable.columns(), left));
        rightTable = rightTable.select(generateColumnExpressions(rightTable.columns(), right));


        leftTable.show(false);
        rightTable.show(false);

        // Perform the left join
        Dataset<Row> joinedTable = leftTable
                .join(rightTable, leftTable.col(withPrefix(left, ROW_KEY)).
                        equalTo(rightTable.col(withPrefix(right, ROW_KEY))), "left");

        return joinedTable;

    }


    public static String withPrefix(String prefix, String name) {
        return String.format("%s%s", prefix, name);
    }

    /**
     * if incoming rowing as < 1000,
     */
    public static Dataset<Row> getInsertUpdatedAndClosedRecordsWithWhereClause(Dataset<Row> newData, Dataset<Row> existing) {

        Set<String> incomingRowKeys = newData.select(ROW_KEY).collectAsList().stream().map(row -> row.getString(0)).collect(Collectors.toSet());

        Dataset<Row> rowsToUpdateClose = existing.filter(new FilterFunction<Row>() {
            @Override
            public boolean call(Row row) throws Exception {
                return incomingRowKeys.contains((String) row.getAs(ROW_KEY));
            }
        });

        Dataset<Row> insertUpdatedAndClosedRecords = getInsertUpdatedAndClosedRecords(newData, rowsToUpdateClose);
        return insertUpdatedAndClosedRecords;
    }


    public static Dataset<Row> getInsertUpdatedAndClosedRecords(Dataset<Row> newData, Dataset<Row> existing) {

        Dataset<Row> rowDataset = leftJoinWithColumnPrefix(newData, existing);
        Column oldRowKey = new Column(withPrefix(right, SCD2Utils.ROW_KEY));
        Dataset<Row> insertRecords = rowDataset.where(oldRowKey.isNull());

        Dataset<Row> updateRecord = rowDataset.where(oldRowKey.isNotNull()).withColumn(STATUS, functions.lit(0));

        Map<String, Integer> colNameIndex = new HashMap<>();
        String[] columns = updateRecord.columns();
        for (int i = 0; i < columns.length; i++) {
            colNameIndex.put(columns[i], i);
        }

        Dataset<Row> rowDataset1 = updateRecord.mapPartitions(new MapPartitionsFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Iterator<Row> iterator) throws Exception {

                Queue<Row> closed = new ArrayDeque<>();

                return new Iterator<Row>() {
                    @Override
                    public boolean hasNext() {
                        if (!closed.isEmpty()) {
                            return true;
                        }
                        return iterator.hasNext();
                    }

                    @Override
                    public Row next() {

                        if (!closed.isEmpty()) {
                            return closed.poll();
                        }

                        Row row = iterator.next();
                        int size = row.size() - 1;

                        Object[] values = ((GenericRow) row).values();

                        String newRowMd5 = row.getAs(withPrefix(left, ROW_MD5));
                        String oldRowMd5 = row.getAs(withPrefix(right, ROW_MD5));

                        //if rowMd5 match then duplicate record
                        if (newRowMd5.equalsIgnoreCase(oldRowMd5)) {
                            values[size] = eSTATUS.DUPLICATE_RECORD_OPEN.getNumVal();
                            return row;
                        }

                        //update valid to and put to close
                        //get validFrom from new data and update in valid to of existing data then put to close
                        Timestamp newRowValidFrom = row.getAs(withPrefix(left, VALID_FROM));
                        values[colNameIndex.get(withPrefix(right, VALID_TO))] = new Timestamp(newRowValidFrom.getTime() - 1);
                        values[size] = eSTATUS.CLOSED.getNumVal();
                        closed.add(row);

                        //update validFrom and put to open
                        Object[] cloned = ((GenericRow) row).values().clone();
                        cloned[size] = eSTATUS.UPDATED.getNumVal();
                        return RowFactory.create(cloned);
                    }
                };
            }
        }, RowEncoder.apply(updateRecord.schema()));

        rowDataset1.show(false);

        return rowDataset1.unionAll(insertRecords.withColumn(STATUS, functions.lit(eSTATUS.INSERT.getNumVal())));

    }


    private static Column[] generateColumnExpressions(String[] columnNames, String prefix) {
        ArrayList<Column> expressions = new ArrayList<>();
        for (int i = 0; i < columnNames.length; i++) {
            if (prefix == null) {
                expressions.add(new Column(columnNames[i]));
            } else {
                expressions.add(functions.col(columnNames[i]).alias(prefix + columnNames[i]));
            }
        }
        return expressions.toArray(new Column[0]);
    }

    public static Column[] selectWithPrefix(String[] columnNames, String prefix) {
        ArrayList<Column> expressions = new ArrayList<>();
        for (int i = 0; i < columnNames.length; i++) {
            if (columnNames[i].startsWith(prefix)) {
                expressions.add(new Column(columnNames[i]));
            }
        }
        return expressions.toArray(new Column[0]);
    }

    public static Column[] selectWithPrefix(String[] columnNames,String extraColumn,String prefix) {
        ArrayList<Column> expressions = new ArrayList<>();
        for (int i = 0; i < columnNames.length; i++) {
            if (columnNames[i].startsWith(prefix)) {
                expressions.add(new Column(columnNames[i]));
            }
        }
        if(StringUtils.isNoneBlank(extraColumn)) {
            expressions.add(new Column(extraColumn));
        }
        return expressions.toArray(new Column[0]);
    }

    public static Column[] removePrefix(String[] columnNames, String prefix) {
        ArrayList<Column> expressions = new ArrayList<>();
        for (int i = 0; i < columnNames.length; i++) {
            if (prefix == null) {
                expressions.add(new Column(columnNames[i]));
            } else {
                expressions.add(functions.col(columnNames[i]).alias(columnNames[i].replaceAll(prefix, "")));
            }
        }
        return expressions.toArray(new Column[0]);
    }


    /***
     * return : <de-duplicated dataset,reject dataset>
     * */
    public static Tuple2<Dataset<Row>,Dataset<Row>> deduplicateAndReject(Dataset<Row> enriched, StructType rejectSchema) {
        Dataset<Row> status = enriched.withColumn(STATUS, functions.lit(eSTATUS.NA.getNumVal()));
        Dataset<Row> deduplication = status.sort(ROW_KEY).mapPartitions(new MapPartitionsFunction<Row, Row>() {

            @Override
            public Iterator<Row> call(Iterator<Row> iterator) throws Exception {

                final String[] previousRowKey = new String[1];
                previousRowKey[0] = null;
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
                        String currentRowKey = row.getAs(ROW_KEY);
                        if (previousRowKey[0] != null) {
                            if (previousRowKey[0].equalsIgnoreCase(currentRowKey)) {
                                values[size - 1] = eSTATUS.DUPLICATE_IN_MICRO_BATCH.getNumVal();
                            }
                        }
                        previousRowKey[0] = currentRowKey;

                        return row;
                    }
                };
            }
        }, RowEncoder.apply(status.schema()));


        Dataset<Row> rejectDataset = deduplication.filter(STATUS + "=" + eSTATUS.DUPLICATE_IN_MICRO_BATCH.getNumVal());
        Dataset<Row> deduplicate = deduplication.filter(STATUS + "=" + eSTATUS.NA.getNumVal()).drop(STATUS);
        Dataset<Row> finalRejectDataset = convertToRejectDataset(rejectDataset,rejectSchema);
        finalRejectDataset.show(false);
        return new Tuple2<>(deduplicate,finalRejectDataset);
    }

    public static Dataset<Row> convertToRejectDataset(Dataset<Row> inDS,StructType rejectSchema){
        Dataset<Row> finalRejectDataset = inDS.mapPartitions(new MapPartitionsFunction<Row, Row>() {
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
                        int status = row.getAs(STATUS);
                        return RowFactory.create(eSTATUS.fromInt(status).toString(), row.toString());
                    }
                };
            }
        }, RowEncoder.apply(rejectSchema));
        return finalRejectDataset;
    }

    public static StructType getRejectSchema() {
        StructType structType = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("reason", DataTypes.StringType, false),
                DataTypes.createStructField("rowData", DataTypes.StringType, false)

        });
        return structType;
    }

}