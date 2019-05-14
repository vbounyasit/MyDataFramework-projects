package io.github.vbounyasit.rift_minimal_example

import com.vbounyasit.bigdata.testing.{UnitTestDefinition, UnitTestGenerator}
import org.apache.spark.sql.SparkSession

class SampleUnitTestGenerator extends UnitTestGenerator {
  override def unitTestDefinitions(implicit spark: SparkSession): Seq[UnitTestDefinition] =
    Seq(new SampleUnitTestDefinition())

  executeTests()
}
