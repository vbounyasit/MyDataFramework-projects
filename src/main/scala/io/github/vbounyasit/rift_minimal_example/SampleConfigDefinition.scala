package io.github.vbounyasit.rift_minimal_example

import com.typesafe.config.{Config, ConfigFactory}
import com.vbounyasit.bigdata.config.ConfigDefinition

class SampleConfigDefinition extends ConfigDefinition{

  override val sourcesConf: Config = ConfigFactory.load("sources")

  override val jobsConf: Config = ConfigFactory.load("jobs_params")

}
