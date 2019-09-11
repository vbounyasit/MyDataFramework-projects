package io.github.vbounyasit.dataflow_minimal_example

import com.vbounyasit.bigdata.SparkApplication
import com.vbounyasit.bigdata.testing.JobsTestGenerator
import com.vbounyasit.bigdata.testing.common.HiveEnvironment
import com.vbounyasit.bigdata.testing.formats.DataFrameIO.DataFrameWriter
import com.vbounyasit.bigdata.testing.formats.json.{JsonDfLoader, JsonDfWriter}

class SampleTestGenerator extends JobsTestGenerator {

  override val sparkApplication: SparkApplication[_, _] = SampleApplication

  override val hiveEnvironment: HiveEnvironment = new HiveEnvironment(new JsonDfLoader)

  override val dataFrameWriter: DataFrameWriter = new JsonDfWriter

  executeTests()
}
