package cn.arvid.exam

import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.spark.Neo4j

object Test1 {
  private val conf = new SparkConf().setAppName("Test1")
    .setMaster("local");
  val sc = new SparkContext(conf)
  def main(args: Array[String]): Unit = {
    val bc = sc.broadcast(Array(1,2))
    val value = bc.value
    for(x<-value){
      println(x)
    }
    val accumulator = sc.doubleAccumulator
    for(i <- 1 to 3){
      accumulator.add(1)
    }
    println(accumulator.value)
    val tf = sc.textFile("lw.txt")
    tf.flatMap(line=>line.split(","))
      .map(word=>(word,1)).reduceByKey((a,b)=>a+b).foreach(x=>{println(x._1+","+x._2)})
//    val arr=sc.parallelize(Array(("A",1),("B",2),("C",3)))
//    arr.flatMap(x=>(x._1+x._2)).foreach(println)
//    arr.map(x=>(x._1+x._2)).foreach(x=>{println;accumulator.add(1)})
    println(accumulator.value)
  }
}
