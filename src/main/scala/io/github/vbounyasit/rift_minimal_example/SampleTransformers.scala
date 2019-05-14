package io.github.vbounyasit.rift_minimal_example

import com.vbounyasit.bigdata.transform.TransformComponents.Transformers
import com.vbounyasit.bigdata.transform.Transformer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class SampleTransformers extends Transformers {

  case class Selector(columns: String*) extends Transformer {
    override def transform(dataFrame: DataFrame): DataFrame = dataFrame.select(columns.map(col): _*)
  }

  case class DropDuplicates(columns: String*) extends Transformer {
    override def transform(dataFrame: DataFrame): DataFrame = dataFrame.dropDuplicates(columns)
  }

}
