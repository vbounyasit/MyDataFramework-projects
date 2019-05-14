package io.github.vbounyasit.rift_minimal_example


import com.vbounyasit.bigdata.testing.UnitTestDefinition
import com.vbounyasit.bigdata.testing.data.UnitTestDefinition.{DataFrameDef, DataFrameSchema, UnitTest, _}
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{Row, SparkSession}

class SampleUnitTestDefinition(implicit spark: SparkSession) extends UnitTestDefinition {

  override protected val executionPipelines: SampleExecutionPipelines = new SampleExecutionPipelines

  import executionPipelines.transformers._

  override val unitTests: Seq[UnitTest] = Seq(
    UnitTest("Selector",
      Selector("col1", "col2"),
      inputDf = DataFrameDef(
        DataFrameSchema(
          ("col1", IntegerType),
          ("col2", StringType),
          ("col3", StringType)
        ),
        Row(5, "col2value", "col3value"),
        Row(55, "col2value1", "col3value1"),
        Row(15, "col2value2", "col3value2"),
        Row(20, "col2value3", "col3value3")
      ),
      outputDf = DataFrameDef(
        DataFrameSchema(
          ("col1", IntegerType),
          ("col2", StringType)
        ),
        Row(5, "col2value"),
        Row(55, "col2value1"),
        Row(15, "col2value2"),
        Row(20, "col2value3")
      )
    )
  )
}
