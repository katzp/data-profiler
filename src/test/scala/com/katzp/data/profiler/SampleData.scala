package com.katzp.data.profiler

trait TSampleData {
  private[profiler] val sample1 = Seq(
    SampleData(1234, "apple", 0),
    SampleData(1235, "banana", 1),
    SampleData(1236, "banana", 0),
    SampleData(1237, "banana", 0),
    SampleData(1238, "apple", 1)
  )
}
case class SampleData(id: Long, name: String, flag: Int)
