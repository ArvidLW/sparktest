package cn.arvid.spark

import org.neo4j.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
//import org.neo4j.harness.{ServerControls, TestServerBuilders}

object ImportData {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("testNeo4jWithSpark")
      .setMaster("local")
      .set("spark.neo4j.bolt.url","bolt://10.2.0.139:7887")
      .set("spark.neo4j.bolt.user","neo4j")
      .set("spark.neo4j.bolt.password","ksdc")
    var sc:SparkContext = new SparkContext(conf)

    val neo = Neo4j(sc)
    //
    val rdd = neo.cypher("MATCH (n:Person) RETURN n.name").loadRowRdd
    println("count:" + rdd.count())
  }
}
