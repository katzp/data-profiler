name := "data-profiler"
version := "0.4"
scalaVersion := "2.12.10"
val sparkVersion = "3.0.0"
Test / parallelExecution := false
libraryDependencies ++= Seq(
  "com.amazon.deequ" % "deequ" % "1.2.2-spark-3.0" % Provided,
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "com.github.scopt" %% "scopt" % "4.0.1" % Provided,
  "org.apache.httpcomponents" % "httpclient" % "4.5.6" % Provided,
  "org.apache.httpcomponents" % "httpcore" % "4.4.12" % Provided,
  "org.scalatest" %% "scalatest" % "3.2.9" % Test
)