//ThisBuild / version := "0.1.0-SNAPSHOT"

//ThisBuild / scalaVersion := "2.13.10"

//lazy val root = (project in file("."))
//  .settings(
//    name := "FirstSparkApplication",
//    idePackagePrefix := Some("org.spark.practice")
//  )
resolvers += "jitpack" at "https://jitpack.io"

name := "FirstSparkApplication"
version := "0.1"
scalaVersion := "2.12.1"

val sparkVersion = "3.2.2"

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.2"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.2" % "provided"

//JAR file settings
//assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
//assemblyJarName in assembly := s"${name.value}_${scalaBinaryVersion.value}-${sparkVersion.value}_${version.value}.jar"