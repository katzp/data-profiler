package com.katzp.data.profiler

import com.katzp.data.profiler.cli.Config

trait Utils {
  def buildSelectQuery(config: Config): String = {
    val selectColumns = if (config.columns.isEmpty) "*" else config.columns.get.mkString(", ")
    val filter = if (config.whereClause.isEmpty) "" else s" WHERE ${config.whereClause.get}"
    s"SELECT $selectColumns FROM ${config.qualifiedTableName}$filter"
  }
}
