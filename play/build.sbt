name := "play-swagger-reactivemongo"

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.12.12"

val reactiveMongoVer = "1.0.0-play26"

libraryDependencies ++= Seq(
  guice,
  "org.reactivemongo"      %% "play2-reactivemongo" % reactiveMongoVer,
  "io.swagger"             %% "swagger-play2"       % "1.7.1",
  "org.webjars"            %  "swagger-ui"          % "3.22.2",
  "org.scalatestplus.play" %% "scalatestplus-play"  % "5.0.0-M2" % Test,
  "org.apache.spark" %% "spark-core" % "3.1.3",
  "org.apache.spark" %% "spark-sql" % "3.1.3",
  "org.apache.hadoop" % "hadoop-client" % "3.3.1",
  "org.mongodb.spark" % "mongo-spark-connector_2.12" % "3.0.2",
  "org.mongodb.scala" % "mongo-scala-driver_2.12" % "4.1.0",
  "org.apache.spark" % "spark-mllib_2.12" % "3.1.3"
)

import play.sbt.routes.RoutesKeys

RoutesKeys.routesImport += "play.modules.reactivemongo.PathBindables._"
