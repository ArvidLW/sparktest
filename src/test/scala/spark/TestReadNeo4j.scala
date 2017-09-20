package spark

import cn.arvid.spark.ReadNeo4j
import org.scalatest.WordSpec

class TestReadNeo4j extends WordSpec{
  private val readNeo4j = new ReadNeo4j()
  "test query" in{
    readNeo4j.query(Array())
  }
}
