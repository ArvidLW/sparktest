package cn.arvid.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.spark.Neo4j

/**
  * 构造neo4j和spark context
  * @param appName
  * @param neo4jUri
  * @param user
  * @param password
  * @param master
  */
class Context(val appName: String,
              val neo4jUri: String,
              val user: String,
              password: String,
              master: String = "local") {

  private val conf = new SparkConf().setAppName(appName)
    .setMaster(master)
    .set("spark.neo4j.bolt.url", neo4jUri)
    .set("spark.neo4j.bolt.user", user)
    .set("spark.neo4j.bolt.password", password)

  val sc = new SparkContext(conf)
  val neo = Neo4j(sc)
}
