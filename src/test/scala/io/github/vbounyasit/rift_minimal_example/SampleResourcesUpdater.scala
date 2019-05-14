package io.github.vbounyasit.rift_minimal_example

import com.vbounyasit.bigdata.SparkApplication
import com.vbounyasit.bigdata.testing.formats.json.JsonResourcesUpdater

object SampleResourcesUpdater extends JsonResourcesUpdater {
  override val sparkApplication: SparkApplication[_, _] = SampleApplication

  def main(args: Array[String]): Unit = {
    run()
  }
}
