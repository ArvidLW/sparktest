name := "dataT"

version := "0.1"

scalaVersion := "2.11.7"

libraryDependencies += "com.typesafe" % "config" % "1.3.0"
libraryDependencies += "com.ks.ia" % "databus_2.11" % "0.4"
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.3" % "test"
credentials += Credentials(Path.userHome / ".sbt" / ".credentials")
libraryDependencies += "log4j" % "log4j" % "1.2.17"
libraryDependencies += "neo4j-contrib" % "neo4j-spark-connector" % "2.1.0-M4"
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.1"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.1.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.1"
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
