package com.katzp.data.profiler.spark

import org.apache.spark.sql.SparkSession

trait SparkWrapper {
  lazy val spark: SparkSession = SparkSession.builder().appName("data-profiler").getOrCreate()
}

trait SparkTestingWrapper {
  lazy val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("data-profiler-test")
      .config("spark.databricks.clusterUsageTags.clusterId", "123")
      .config("spark.databricks.clusterUsageTags.clusterName", "test")
      .getOrCreate()
}
