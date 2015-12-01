package kmlab.main

import kmlab.utils.{ReadDir, Send2Kafka}

/**
 * Created by WeiChen on 2015/11/28.
 */
object Main {
  def main(args: Array[String]): Unit = {
    val dirName = args(0)
    val fileList = ReadDir.readDir(dirName)
    val n = fileList.length
    println (s"Find $n files.")
    val brokerList = "bigboost-kafka:9092"
    Send2Kafka.send(fileList, brokerList, "data.raw")
  }
}
