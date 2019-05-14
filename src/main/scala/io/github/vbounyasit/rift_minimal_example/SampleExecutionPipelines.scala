package io.github.vbounyasit.rift_minimal_example

import com.vbounyasit.bigdata.transform.TransformComponents.ExecutionPipelines
import com.vbounyasit.bigdata.transform.pipeline.impl.Pipeline

class SampleExecutionPipelines extends ExecutionPipelines{

  override val transformers: SampleTransformers = new SampleTransformers

  import transformers._

  val pipeline1 = Pipeline(
    Selector("col3", "col2", "col1")
  )
}
