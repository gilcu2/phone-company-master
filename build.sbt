import AcceptanceTest._
import IntegrationTest._
import UnitTest._

name := "disco-test-phone-company"
organization := "com.gilcu2"

UnitTestSettings ++ IntegrationTestSettings ++ AcceptanceTestSettings
lazy val TestAll: Configuration = config("test-all").extend(AcceptanceTest.AcceptanceTestConfig)
configs(IntegrationTestConfig, AcceptanceTestConfig, TestAll)

version := "0.1"

scalaVersion := "2.11.12"

val sparkV = "2.4.3"

libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % sparkV % "provided",
  "org.apache.spark" %% "spark-sql" % sparkV % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkV % "runtime",

  "com.typesafe" % "config" % "1.3.2",

  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
  "ch.qos.logback" % "logback-classic" % "1.2.3",

  "org.rogach" %% "scallop" % "3.1.2",
  "com.github.nscala-time" %% "nscala-time" % "2.20.0",

  "org.scalatest" %% "scalatest" % "3.0.5" % "test",

)

mainClass in(Compile, run) := Some("com.phone.Main")

test in assembly := {}

assemblyJarName in assembly := "addresses.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("org.slf4j.**" -> "shaded.@1").inAll
)


