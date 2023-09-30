ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.15" % "test"

lazy val root = (project in file("."))
  .settings(
    name := "SparkConcepts"
  )
