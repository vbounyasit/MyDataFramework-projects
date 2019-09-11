package io.github.vbounyasit.dataflow_minimal_example

import com.vbounyasit.bigdata.EitherRP
import com.vbounyasit.bigdata.transform.ExecutionPlan
import com.vbounyasit.bigdata.transform.TransformOps._
import io.github.vbounyasit.dataflow_minimal_example.joiner.SampleJoiner
import org.apache.spark.sql.SparkSession

class SampleExecutionPlan(implicit spark: SparkSession) extends ExecutionPlan {

  override val executionPipelines: SampleExecutionPipelines = new SampleExecutionPipelines

  import executionPipelines._

  override def getExecutionPlan(getSource: String => EitherRP): EitherRP = {
    val source1Pipeline = getSource("source1") ==> table1Pipeline1 ==> table1Pipeline2
    val source2Pipeline = getSource("source2") ==> table2Pipeline ==> table2AggregationPipeline

    val result = source1Pipeline.join(source2Pipeline, new SampleJoiner)
    result ==> postJoinPipeline
  }
}
