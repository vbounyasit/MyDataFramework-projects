package io.github.vbounyasit.rift_minimal_example

import com.vbounyasit.bigdata.EitherRP
import com.vbounyasit.bigdata.transform.ExecutionPlan
import com.vbounyasit.bigdata.transform.TransformOps._
import org.apache.spark.sql.SparkSession

class SampleExecutionPlan(implicit spark: SparkSession) extends ExecutionPlan {

  override val executionPipelines: SampleExecutionPipelines = new SampleExecutionPipelines

  import executionPipelines._
  import executionPipelines.transformers._

  override def getExecutionPlan(getSource: String => EitherRP): EitherRP = {
    getSource("source1") ==> pipeline1 ==> DropDuplicates("col1")
  }
}
