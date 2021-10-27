package com.katzp.data.profiler.cli

import scopt.OptionParser

case class Config(
    qualifiedTableName: String = "",
    snowflakeUrl: String = "",
    db: String = "",
    schema: String = "",
    user: String = "",
    password: String = "",
    role: String = "",
    warehouse: String = "",
    tag: Option[String] = None,
    columns: Option[Seq[String]] = None,
    whereClause: Option[String] = None
) {
  override def toString: String =
    s"""
       |Table: $qualifiedTableName
       |DB: $db
       |Tag: ${tag.getOrElse("NONE")}
       |Cols: ${columns.getOrElse("NONE")}
       |WhereClause: ${whereClause.getOrElse("NONE")}
       |""".stripMargin
}

class ConfigParser extends OptionParser[Config]("data-profiler") {
  opt[String]("table")
    .text("Snowflake table name")
    .required()
    .validate(x =>
      if (x.matches(".*\\..*\\..*")) success
      else failure("Must be in format [database].[schema].[table]")
    )
    .action((p, config) => config.copy(qualifiedTableName = p))

  opt[String]("snowflake-url")
    .text("Snowflake account url")
    .required()
    .action((p, config) => config.copy(snowflakeUrl = p))

  opt[String]("database")
    .text("Output database")
    .required()
    .action((p, config) => config.copy(db = p))

  opt[String]("schema")
    .text("Output schema")
    .required()
    .action((p, config) => config.copy(schema = p))

  opt[String]("role")
    .text("Snowflake role")
    .required()
    .action((p, config) => config.copy(role = p))

  opt[String]("warehouse")
    .text("Snowflake warehouse")
    .required()
    .action((p, config) => config.copy(warehouse = p))

  opt[String]("user")
    .text("Snowflake username")
    .required()
    .action((p, config) => config.copy(user = p))

  opt[String]("password")
    .text("Snowflake password")
    .required()
    .action((p, config) => config.copy(password = p))

  opt[String]("tag")
    .text("Tag to help identify this profile in reporting")
    .optional()
    .action((p, config) => config.copy(tag = Some(p)))

  opt[Seq[String]]("columns")
    .text("Specific list of columns to profile")
    .optional()
    .action((p, config) => config.copy(columns = Some(p)))

  opt[String]("where")
    .text("Where clause to filter prior to profiling. For example DATE = (CURRENT_DATE() -1)")
    .optional()
    .action((p, config) => config.copy(whereClause = Some(p)))
}
