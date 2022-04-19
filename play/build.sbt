name := "play"

version := "1.0"

lazy val `play` = (project in file(".")).enablePlugins(PlayScala)

resolvers += "Akka Snapshot Repository" at "https://repo.akka.io/snapshots/"

scalaVersion := "2.13.8"

libraryDependencies ++= Seq(jdbc, ehcache, ws, specs2 % Test, guice)
libraryDependencies += "org.apache.spark" % "spark-core_2.13" % "3.2.0"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.13" % "3.2.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.13" % "3.2.0"
libraryDependencies += "org.mongodb" % "casbah_2.12" % "3.1.1"