package io.github.vbounyasit.rift_minimal_example.joiner

import com.vbounyasit.bigdata.transform.joiner.Joiner
import com.vbounyasit.bigdata.transform.joiner.JoinerKeys.{CommonKey, JoinKey}

class SampleJoiner extends Joiner {
  override val keys: List[JoinKey] = List(
    CommonKey("index_col")
  )
  override val joinType: String = "inner"

}
