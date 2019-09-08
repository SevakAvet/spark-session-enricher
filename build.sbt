name := "spark"

version := "0.1"

scalaVersion := "2.11.0"

val sparkVersion = "2.3.1"
val scalaTestVersion = "3.0.5"

resolvers += "Bintray Maven Repository" at "https://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "net.liftweb" %% "lift-json" % "2.6.2",
  "com.typesafe" % "config" % "1.2.1",
  "org.scalactic" %% "scalactic" % scalaTestVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % "test"

)