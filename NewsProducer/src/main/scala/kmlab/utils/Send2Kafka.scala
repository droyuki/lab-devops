package kmlab.utils

/**
  * Created by WeiChen on 2015/11/28.
  */

import java.io.File
import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

object Send2Kafka {
  def send(fileList: List[File], brokers: String, topic: String): Unit = {
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("producer.type", "async")
    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)
    fileList.foreach { file =>
      val source = scala.io.Source.fromFile(file)
      val str = source.getLines().mkString
      val data = new KeyedMessage[String, String](topic, str)
      println("send")
      producer.send(data)
      println("...")
    }
    producer.close()
  }
}