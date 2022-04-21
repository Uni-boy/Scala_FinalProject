name := "data"

version := "1.0"

scalaVersion := "2.13.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.3.1"
libraryDependencies += "org.mongodb.scala" % "mongo-scala-driver_2.13" % "4.1.0"