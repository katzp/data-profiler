package com.katzp.data.profiler

import com.katzp.data.profiler.spark.SparkTestingWrapper
import org.apache.spark.sql.functions.col
import org.scalatest.flatspec.AnyFlatSpec

class ProfilerSpec extends AnyFlatSpec with SparkTestingWrapper with TSampleData {
  val df = spark.createDataFrame(sample1)
  val profiles = Profiler.runDefaults(df, spark)

  "profiler" should "return a non empty dataset" in {
    assert(!profiles.isEmpty)
  }

  it should "see all columns complete" in {
    val completeness = profiles
      .filter(_.columnName != "__recordCount")
      .select("completeness")
      .distinct()

    assert(completeness.count == 1)
    assert(completeness.collect()(0).getDouble(0) == 1.0)
  }

  it should "return numerical stats for flag column" in {
    val numericalValuesFilled = profiles
      .filter(col("columnName") === "flag")
      .select("min", "max", "mean", "std", "sum")
      .collect()
      .forall(row =>
        !(row.getDouble(0).isNaN && row.getDouble(1).isNaN && row.getDouble(2).isNaN && row
          .getDouble(3)
          .isNaN && row.getDouble(4).isNaN)
      )
    assert(numericalValuesFilled)
  }

  it should "have correct record count" in {
    val count = profiles
      .filter(_.columnName == "__recordCount")
      .select("tableSize")
      .collect()(0)
      .getLong(0)
    assert(count == 5)
  }
}
