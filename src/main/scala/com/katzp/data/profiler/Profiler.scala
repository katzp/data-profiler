package com.katzp.data.profiler

import com.amazon.deequ.metrics.Distribution
import com.amazon.deequ.profiles.{ColumnProfilerRunner, ColumnProfiles, NumericColumnProfile}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization._

object Profiler {

  def runDefaults(df: DataFrame, spark: SparkSession): Dataset[FlatProfile] = {
    val profiles = ColumnProfilerRunner().onData(df).run()
    parseProfiles(profiles, spark)
  }

  case class FlatProfile(
      columnName: String,
      completeness: Option[Double] = None,
      countDistinct: Option[Long] = None,
      dType: Option[String] = None,
      histogram: Option[String] = None,
      min: Option[Double] = None,
      max: Option[Double] = None,
      mean: Option[Double] = None,
      std: Option[Double] = None,
      sum: Option[Double] = None,
      tableSize: Option[Long] = None
  )

  private def parseProfiles(profiles: ColumnProfiles, spark: SparkSession): Dataset[FlatProfile] = {
    val parsed = profiles.profiles.toSeq.map { case (col, profile) =>
      val genericFlatProfile = FlatProfile(
        col,
        Some(profile.completeness),
        Some(profile.approximateNumDistinctValues),
        Some(profile.dataType.toString),
        Some(histogramToJsonString(profile.histogram))
      )
      profile match {
        case numeric: NumericColumnProfile =>
          genericFlatProfile.copy(
            min = numeric.minimum,
            max = numeric.maximum,
            mean = numeric.mean,
            std = numeric.stdDev,
            sum = numeric.sum
          )
        case _ => genericFlatProfile
      }
    }
    import spark.implicits._
    spark.createDataset(
      parsed :+ FlatProfile("__recordCount", tableSize = Some(profiles.numRecords))
    )
  }

  private[profiler] def histogramToJsonString(histogram: Option[Distribution]): String = {
    implicit val formats = DefaultFormats
    histogram.map(write(_)).getOrElse("")
  }
}
