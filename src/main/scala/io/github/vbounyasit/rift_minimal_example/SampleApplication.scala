package io.github.vbounyasit.rift_minimal_example

import com.vbounyasit.bigdata.ETL.{ExecutionParameters, OptionalJobParameters}
import com.vbounyasit.bigdata.SparkApplication
import com.vbounyasit.bigdata.config.ConfigDefinition
import org.apache.spark.sql.{DataFrame, SparkSession}

object SampleApplication extends SparkApplication {

  override val configDefinition: ConfigDefinition = new SampleConfigDefinition

  override def executionPlans(implicit spark: SparkSession): Map[String, ExecutionParameters[_, _]] = Map(
    "job1" -> ExecutionParameters(
      new SampleExecutionPlan()
    )
  )

  override def load(dataFrame: DataFrame, database: String, table: String, optionalJobParameters: OptionalJobParameters[Nothing, Nothing]): Unit =
    dataFrame.show()

  def main(args: Array[String]): Unit = {
    val executionData = loadExecutionData(args)
    runETL(executionData)
  }
}
