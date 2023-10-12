package org.ksm.scd;

import org.apache.iceberg.spark.extensions.MergeIntoHelper;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.*;
import org.apache.spark.sql.execution.CommandExecutionMode;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.*;


public class MergeHelper {

    public static void executeMerge(Dataset<Row> source, String tableName) throws ParseException {

        Dataset<Row> source1 = source.as("source");
        String condition = String.format("%s.row_key = source.row_key", tableName);
        Expression rowKeyExpression = functions.expr(condition).expr();

        //update columns
        List<Assignment> assignmentList = getAssignmentList(source, Collections.EMPTY_SET);
        Seq<Assignment> assignments = JavaConverters.asScalaBuffer(assignmentList).toSeq();

        //UpdateAction condition
        Option<Expression> conditionOption = Option.apply(null);
        MergeAction updateAction = new UpdateAction(conditionOption, assignments);
        List<MergeAction> matchedActionList = Arrays.asList(updateAction);

        //todo insert columns
        //todo insert condition
        MergeAction insertAction = new InsertAction(conditionOption, assignments);
        List<MergeAction> unMatchedActionList = Arrays.asList(insertAction);

        boolean executeScalaCode = false;
        if(!executeScalaCode) {
            MergeHelper.executeMerge(source1,
                    tableName,
                    Optional.of(rowKeyExpression),
                    matchedActionList,
                    unMatchedActionList);
        } else {
            MergeIntoHelper.executeMerge(source1,
                    tableName,
                    Option.apply(rowKeyExpression),
                    JavaConverters.asScalaBuffer(matchedActionList).toSeq(),
                    JavaConverters.asScalaBuffer(unMatchedActionList).toSeq()
            );
        }

        System.out.println("end");

    }

    private static List<Assignment> getAssignmentList(Dataset<Row> source, Set<String> exclude) {

        List<Assignment> assignmentList = new ArrayList<>();
        String[] columns = source.columns();
        for (int i = 0; i < columns.length; i++) {
            String columnName = columns[i];
            if (!exclude.contains(columnName)) {
                assignmentList.add(new Assignment(functions.col(columnName).expr(), source.col(columnName).expr()));
            }
        }
        return assignmentList;
    }

    private static void executeMerge(Dataset<Row> mergeSourceDs,
                                     String tableName,
                                     Optional<Expression> whenCondition,
                                     List<MergeAction> whenMatchedAction,
                                     List<MergeAction> whenNotMatchedAction) throws ParseException {
        SparkSession sparkSession = mergeSourceDs.sparkSession();

                LogicalPlan mergePlan = new UnresolvedMergeIntoIcebergTable(
                new UnresolvedRelation(
                        sparkSession.sessionState().sqlParser().parseMultipartIdentifier(tableName),
                        CaseInsensitiveStringMap.empty(),
                        false),
                        sparkSession.sessionState().analyzer().execute(mergeSourceDs.queryExecution().logical()),
                new MergeIntoContext(whenCondition.get(),
                        JavaConverters.asScalaBuffer(whenMatchedAction).seq(),
                        JavaConverters.asScalaBuffer(whenNotMatchedAction).seq())
        );

        runCommand(mergeSourceDs.sparkSession(),mergePlan);
        System.out.println("test");
    }

    private static void runCommand(SparkSession session, LogicalPlan plan){
        QueryExecution queryExecution = session.sessionState().executePlan(plan, CommandExecutionMode.ALL());
        queryExecution.assertCommandExecuted();
    }
}
