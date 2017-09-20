package cn.arvid.spark

class ReadNeo4j {
  private val context=new Context("NeoTest","bolt://lw:7874","neo4j","123456")
  val neo=context.neo;
  def query(labels: Array[String]){
    val rdd = neo.cypher("MATCH (n:Person) RETURN n.name").loadRowRdd
    println("count:" + rdd.count())
  }
  def queryFailRetry(): Unit ={

  }
}
