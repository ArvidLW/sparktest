package cn.arvid.neo4j

import java.util

/**
  * 主程序运行代码
  * 运行时需要加入三个或三个以上参数：
  * 1.kafkauri
  * 2.指定发送的topic
  * 3.neo4j的label
  * 注意：前两个参数要按照规定的顺序
  */
object Main {
  def main(args: Array[String]): Unit = {
    val list = new util.ArrayList[String](args.length-2)
    for(i <- 2 until args.length){
      list.add(args(i))
    }
    val ex = new ExportData(list,args(0),args(1))
    ex.send2kafka()
  }
}