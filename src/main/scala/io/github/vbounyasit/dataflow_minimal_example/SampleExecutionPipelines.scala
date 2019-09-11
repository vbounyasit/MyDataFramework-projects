package io.github.vbounyasit.dataflow_minimal_example

import com.vbounyasit.bigdata.transform.TransformComponents.ExecutionPipelines
import com.vbounyasit.bigdata.transform.pipeline.impl.Pipeline

class SampleExecutionPipelines extends ExecutionPipelines {

  override val transformers: SampleTransformers = new SampleTransformers

  import transformers._

  val table1Pipeline1 = Pipeline(
    FilterOnTable1()
  )
  val table1Pipeline2 = Pipeline(
    MultiplyByFactor("col1", 5),
    MultiplyByFactor("col2", 2)
  )
  val table2Pipeline = Pipeline(
    MultiplyByColumn("col33", "col11"),
    MultiplyByColumn("col33", "col22")
  )
  val table2AggregationPipeline = Pipeline(
    SumByColumn("index_col", "col33")
  )
  val postJoinPipeline = Pipeline(
    MultiplyByColumn("sum_of_col33", "col1"),
    RenameColumn("sum_of_col33", "result"),
    KeepGreaterThan("result", 2000000)
  )
}

