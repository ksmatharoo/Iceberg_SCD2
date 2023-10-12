package org.apache.iceberg.spark.extensions

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, MergeAction, MergeIntoContext, UnresolvedMergeIntoIcebergTable}

object MergeIntoHelper {

  def executeMerge(mergeSourceDs: Dataset[Row],
                  tableName: String,
                  whenCondition: Option[Expression],
                  whenMatchedAction: Seq[MergeAction],
                  whenNotMatchedAction: Seq[MergeAction]): Unit = {

    val sparkSession = mergeSourceDs.sparkSession
    val mergePlan = UnresolvedMergeIntoIcebergTable(
      UnresolvedRelation(sparkSession.sessionState.sqlParser.parseMultipartIdentifier(tableName)),
      sparkSession.sessionState.analyzer.ResolveRelations(mergeSourceDs.queryExecution.logical),
      MergeIntoContext(whenCondition.get, whenMatchedAction, whenNotMatchedAction)
    )
    runCommand(sparkSession, mergePlan)
  }

  private def runCommand(sparkSession: SparkSession, plan: LogicalPlan): Unit = {
    val qe = sparkSession.sessionState.executePlan(plan)
    qe.assertCommandExecuted()
  }
}
