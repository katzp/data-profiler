package com.katzp.data.profiler

import com.katzp.data.profiler.cli.Config
import com.katzp.data.profiler.spark.SparkTestingWrapper
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec

class PipelineSpec extends AnyFlatSpec with SparkTestingWrapper with TSampleData {
  val df = spark.createDataFrame(sample1)
  val config = Config("tableA", tag = Some("testing"))

  "pipeline transformation" should "yield the correct schema" in {
    val transformed = df.transform(Pipeline.transformSource(spark, config))
    val schema = transformed.schema
    val expectedSchema = new StructType()
      .add("columnName", StringType, true)
      .add("completeness", DoubleType, true)
      .add("countDistinct", LongType, true)
      .add("dType", StringType, true)
      .add("histogram", StringType, true)
      .add("min", DoubleType, true)
      .add("max", DoubleType, true)
      .add("mean", DoubleType, true)
      .add("std", DoubleType, true)
      .add("sum", DoubleType, true)
      .add("tableSize", LongType, true)
      .add("qualified_table", StringType, false)
      .add("tag", StringType, false)
      .add("profile_timestamp", TimestampType, false)
      .add("cluster_id", StringType, false)
      .add("cluster_name", StringType, false)

    assert(schema == expectedSchema)
  }

}
