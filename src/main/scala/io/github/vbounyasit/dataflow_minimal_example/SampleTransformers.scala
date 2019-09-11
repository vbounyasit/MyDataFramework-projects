package io.github.vbounyasit.dataflow_minimal_example

import com.vbounyasit.bigdata.transform.TransformComponents.Transformers
import com.vbounyasit.bigdata.transform.Transformer
import org.apache.spark.sql.DataFrame

class SampleTransformers extends Transformers {
  import org.apache.spark.sql.functions._

  case class FilterOnTable1() extends Transformer {
    override def transform(dataFrame: DataFrame): DataFrame = dataFrame.where(col("col3") === "text2")
  }

  case class MultiplyByFactor(columnName: String, factor: Int) extends Transformer {
    override def transform(dataFrame: DataFrame): DataFrame = dataFrame.withColumn(columnName, col(columnName) * factor)
  }

  case class MultiplyByColumn(columnName: String, multiplyByColumn: String) extends Transformer {
    override def transform(dataFrame: DataFrame): DataFrame = dataFrame.withColumn(columnName, col(columnName) * col(multiplyByColumn))
  }

  case class SumByColumn(keyColumn: String, columnToSum: String) extends Transformer {
    override def transform(dataFrame: DataFrame): DataFrame = dataFrame.groupBy(keyColumn).agg(sum(columnToSum).as(s"sum_of_$columnToSum"))
  }

  case class KeepGreaterThan(columnName: String, thresholdValue: Int) extends Transformer {
    override def transform(dataFrame: DataFrame): DataFrame = dataFrame.where(col(columnName) >= thresholdValue)
  }

  case class RenameColumn(columnName: String, newColumnName: String) extends Transformer {
    override def transform(dataFrame: DataFrame): DataFrame = dataFrame.withColumnRenamed(columnName, newColumnName)
  }
}