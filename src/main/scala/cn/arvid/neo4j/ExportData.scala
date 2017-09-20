package cn.arvid.neo4j

import java.net.SocketException
import java.sql.{DriverManager, ResultSet}
import java.{sql, util}
import com.typesafe.config.ConfigFactory
import knownsec.KSDC.DataBus.base.DataProducer
import org.slf4j.LoggerFactory




/**
  * 从neo4j读取数据并写入到kafka中
  * @param labels 需要查询的标签的集合
  * @param kafkaURI kafka的uri
  * @param topic 需要指定的topic
  */
class ExportData(labels: util.ArrayList[String], kafkaURI: String, topic: String) {

  //加载配置文件
  val conf = ConfigFactory.load()
  private val LOGGER = LoggerFactory.getLogger(classOf[ExportData])
  //获取neo4j驱动和连接
  Class.forName("org.neo4j.jdbc.Driver")
  val conn = DriverManager.getConnection(conf.getString("neo4j.uri"), conf.getString("neo4j.username"), conf.getString("neo4j.password"))

  /**
    * 从neo4j中查询数据
    *
    * @return 返回查询的结果集的集合
    */
  def select: util.ArrayList[ResultSet] = {
    var list = new util.ArrayList[sql.ResultSet](labels.size())
    try {
      //遍历集合，获取传入的labels
      for (i <- 0 until labels.size()) {

        val label = labels.get(i)
        //执行CQL查询语句，返回依次为：关系类型，开始节点的标签，开始节点，结束节点，结束节点的标签
        val stm = conn.prepareStatement(s"MATCH (n:$label)-[rel]-(l)  RETURN type(rel),labels(startnode(rel)),startnode(rel)," +
          s"endnode(rel),labels(endnode(rel))")
        //stm.setString(1,labels)
        val rs: sql.ResultSet = stm.executeQuery()
        list.add(rs)

      }
    } catch {
      case e: Exception => {
        LOGGER.error(e.getMessage)
        // e.printStackTrace()
      }
    } finally {
      conn.close()
    }
    list
  }

  /**
    * 查询的数据直接发送到kafka
    * 提供了默认的kafkauri和topic，也可以自己指定
    * @param kafkaURI
    * @param topic
    */
  def send2kafka(kafkaURI: String = kafkaURI, topic: String = topic): Unit = {
    var flag = true
    val list: util.ArrayList[ResultSet] = select
    //测试计数器，打印在控制台，测试用
    var times = 0
    val kafka = DataProducer(kafkaURI)
    for (i <- 0 until list.size()) {
      //每个label已经迭代的数量，写入log4j
      var count = 0
      var rs = list.get(i)
      try {
        while (rs.next()) {
          var record = getMessage(rs)
          count += 1
          LOGGER.info(labels.get(i) + ":第" + count + "条")
          kafka.sendASyn(topic, record)
          // times += 1
          //  println(times)
        }
      } catch {
        case e: SocketException => {
          LOGGER.error(e.getMessage)
          var start = System.currentTimeMillis()
          while (flag) {
            try {
              //10s尝试重连
              Thread.sleep(10000)
              rs.next()
              var record = getMessage(rs)
              //如果连接成功，则将此resultset剩下的数据发送到kafka
              kafka.sendASyn(topic, record)
              while (rs.next()) {
                var record = getMessage(rs)
                kafka.sendASyn(topic, record)
              }
              flag = false
            } catch {
              case e: SocketException => {
                flag = true
              }
            } finally {
              var end = System.currentTimeMillis()
              //超时时间，如果超时，则程序结束
              var time = (end - start) / 60 * 60 * 1000
              if (time > conf.getInt("timeout.time")) {
                System.exit(0)
              }
            }
          }
        }
      }
    }
  }
  /**
    * 封装返回的数据集
    *
    * @param rs
    * @return
    */
  def getMessage(rs: ResultSet): String = {
    val record = rs.getString("labels(startnode(rel))") + "\t" + rs.getString("type(rel)") + "\t" +
      rs.getString("labels(endnode(rel))") + "\t" + rs.getString("startnode(rel)") + "\t" +
      rs.getString("endnode(rel)") + "\n"
    record
  }



  /**
    * 如果在取数据集时失败，则根据日志文件中的信息重新查询该标签
    * 需要手动调用
    * @param label  失败时正在获取数据的标签
    * @param number 需要skip的number 即日志文件中的最后一条记录数加一
    */
  def reselectAndRetry(label: String, number: Int): Unit = {
    val kafka = DataProducer(kafkaURI)
    try {
      //执行CQL查询语句，返回依次为：关系类型，开始节点的标签，开始节点，结束节点，结束节点的标签
      val stm = conn.prepareStatement(s"MATCH (n:$label)-[rel]-(l)  RETURN type(rel),labels(startnode(rel)),startnode(rel)," +
        s"endnode(rel),labels(endnode(rel)) SKIP $number")
      //stm.setString(1,labels)
      val rs: sql.ResultSet = stm.executeQuery()
      while (rs.next()) {
        var record=getMessage(rs)
       kafka.sendASyn(topic,record)
      }
    } catch {
      case e: Exception => {
        LOGGER.error(e.getMessage)
        // e.printStackTrace()
      }
    } finally {
      conn.close()
    }

  }
}



