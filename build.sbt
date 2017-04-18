name := "quotes_struct_st"

version := "1.0"

scalaVersion := "2.11.10"

libraryDependencies ++= {
  val sparkV = "2.1.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkV % "provided",
    "org.apache.spark" %% "spark-streaming" % sparkV % "provided",
    "org.apache.spark" %% "spark-sql" % sparkV % "provided",
    "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkV,
    "io.spray" %% "spray-json" % "1.3.3",
    "org.slf4j" % "slf4j-api" % "1.7.25",
    "com.github.scopt" %% "scopt" % "3.5.0"
  )
}

assemblyJarName in assembly := "quotes.jar"


assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
  case PathList("javax.inject", xs@_*) => MergeStrategy.first
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.first
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.last
  case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.first
  case "application.conf" => MergeStrategy.concat
  case "reference.conf" => MergeStrategy.concat
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
  case _ => MergeStrategy.first
}