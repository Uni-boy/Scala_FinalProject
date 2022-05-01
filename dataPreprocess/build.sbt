name := "dataPreprocess"

scalaVersion := "2.12.2"

libraryDependencies += "org.apache.spark" % "spark-sql_2.12" % "3.1.3"
libraryDependencies += "org.apache.spark" % "spark-core_2.12" % "3.1.3"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.3.1"
libraryDependencies += "org.mongodb.spark" % "mongo-spark-connector_2.12" % "3.0.2"
libraryDependencies += "org.mongodb.scala" % "mongo-scala-driver_2.12" % "4.1.0"
libraryDependencies += "org.mockito" % "mockito-scala_2.12" % "1.17.5" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.3" % "test"