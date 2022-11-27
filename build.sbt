//ThisBuild / version := "0.1.0-SNAPSHOT"

//ThisBuild / scalaVersion := "2.13.10"

//lazy val root = (project in file("."))
//  .settings(
//    name := "FirstSparkApplication",
//    idePackagePrefix := Some("org.spark.practice")
//  )

name := "FirstSparkApplication"
version := "0.1"
scalaVersion := "2.13.10"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.2"

