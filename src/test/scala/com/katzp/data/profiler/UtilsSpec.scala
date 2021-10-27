package com.katzp.data.profiler

import com.katzp.data.profiler.cli.Config
import org.scalatest.flatspec.AnyFlatSpec

class UtilsSpec extends AnyFlatSpec with Utils {

  "build query" should "build runnable SQL queries from configs" in {
    val config1 = Config("public.clickstream.sessions")
    val config2 = Config("public.clickstream.sessions", columns = Some(Seq("session_id")))
    val config3 = Config("public.clickstream.sessions", whereClause = Some("date = '2021-01-01'"))

    val expected1 = "SELECT * FROM public.clickstream.sessions"
    val expected2 = "SELECT session_id FROM public.clickstream.sessions"
    val expected3 = "SELECT * FROM public.clickstream.sessions WHERE date = '2021-01-01'"

    assert(buildSelectQuery(config1) == expected1)
    assert(buildSelectQuery(config2) == expected2)
    assert(buildSelectQuery(config3) == expected3)
  }
}
