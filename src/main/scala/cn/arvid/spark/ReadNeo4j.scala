package cn.arvid.spark

class ReadNeo4j {
  private val context=new Context("NeoTest","bolt://10.2.0.139:7887","neo4j","ksdc")
  val neo=context.neo;
  def query(labels: Array[String]){
    val rdd = neo.cypher("MATCH (n:Person) RETURN n.name").loadRowRdd
    println("count:" + rdd.count())
  }
  def queryFailRetry(): Unit ={

  }
}
