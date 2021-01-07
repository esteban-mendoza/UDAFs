name := "UDAFs"

version := "0.1"

scalaVersion := "2.12.12"

idePackagePrefix := Some("com.bbva.datiocoursework")

// Spark library dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.1",
  "org.apache.spark" %% "spark-sql" % "3.0.1"
)
