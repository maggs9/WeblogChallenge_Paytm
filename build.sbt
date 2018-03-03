import sbt.Keys._
import sbt._

val name = "weblogproject"
val libVersion = "1.0.0"

lazy val WebLogProject =  Project(name, file("."))
  .settings(
    scalaVersion := "2.11.8",
    libraryDependencies ++= dependencies,
    resolvers ++= Seq(
      Resolver.url("bintray-trafficland-oss", url("https://dl.bintray.com/trafficland/oss/"))(
        Patterns(isMavenCompatible = false, Resolver.localBasePattern)
      )
   )
)

lazy val dependencies = Seq(
	"org.scalatest" %% "scalatest" % "3.0.1" % "test",
	"org.apache.spark" %% "spark-core" % "2.1.0",
	"org.apache.spark" %% "spark-sql" % "2.1.0",
	"org.apache.spark" %% "spark-mllib" % "2.1.0"
)
