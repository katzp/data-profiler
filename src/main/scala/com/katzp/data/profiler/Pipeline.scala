package com.katzp.data.profiler

import com.katzp.data.profiler.cli.{Config, ConfigParser}
import com.katzp.data.profiler.spark.SparkWrapper
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{current_timestamp, lit}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Pipeline extends App with SparkWrapper with Utils {
  private val logger = Logger.getLogger("ProfilerPipeline")
  new ConfigParser().parse(args, Config()) match {
    case Some(config) => run(config)
    case None         =>
  }

  def run(config: Config): Unit = {
    logger.info(s"Starting run with config: ${config.toString}")
    val connectionMap = Map(
      "sfUrl" -> config.snowflakeUrl,
      "sfDatabase" -> config.db,
      "sfSchema" -> config.schema,
      "sfWarehouse" -> config.warehouse,
      "sfRole" -> config.role,
      "sfUser" -> config.user,
      "sfPassword" -> config.password
    )
    val df = spark.read
      .format("snowflake")
      .options(connectionMap)
      .option("query", buildSelectQuery(config))
      .load()

    df.transform(transformSource(spark, config))
      .write
      .format("snowflake")
      .mode(SaveMode.Append)
      .options(connectionMap)
      .option("dbtable", s"${config.db}.${config.schema}.DATA_PROFILER")
      .save()

    logger.info("Dataframe saved to Snowflake")
  }

  private[profiler] def transformSource(spark: SparkSession, config: Config)(df: DataFrame): DataFrame = {
    val profilesDataset = Profiler.runDefaults(df, spark)
    profilesDataset
      .withColumn("qualified_table", lit(config.qualifiedTableName))
      .withColumn("tag", lit(config.tag.getOrElse("__default")))
      .withColumn("profile_timestamp", current_timestamp())
      .withColumn("cluster_id", lit(spark.conf.get("spark.databricks.clusterUsageTags.clusterId", "")))
      .withColumn("cluster_name", lit(spark.conf.get("spark.databricks.clusterUsageTags.clusterName", "")))
  }
}
