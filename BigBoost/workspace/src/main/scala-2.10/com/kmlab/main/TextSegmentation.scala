package com.kmlab.main

import com.kmlab.utils.KafkaProducerUtil
import kafka.producer.KeyedMessage
import kafka.serializer.StringDecoder
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.{IDF, HashingTF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import ANSJ._
import PipeRDD._
/**
 * Created by WeiChen on 2015/10/7.
 */
object TextSegmentation extends CreateSparkContext {
  def main(args: Array[String]) {
    if (args.length != 5) {
      System.err.println("Usage: TextSegmentation <checkpointDirectory> <timeframe> <kafka-brokerList> <topic,...,>  <scriptPath>")
      System.exit(1)
    }
    val Array(checkpointDirectory, timeframe, kafkaBrokerList, topicList, scriptPath) = args

    def function2CreateContext(AppName: String, checkpointDirectory: String, timeframe: String, brokerList: String, topicList: String, scriptPath: String): StreamingContext = {
      val ssc = createContext(AppName, checkpointDirectory, timeframe.toLong)
      val kafkaParams = Map("metadata.broker.list" -> brokerList)
      val topics = topicList.split(",").toSet
      val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
      stream.foreachRDD(rdd => {
        val raw = rdd.map(_._2)
        val nlp = ansj(raw,"To")
        pipeData(nlp, scriptPath)
        send2kafka(nlp, brokerList, "ansj.nlp")
      })
      ssc
    }
    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => {
        function2CreateContext("TextSegmentation", checkpointDirectory, timeframe, kafkaBrokerList, topicList, scriptPath)
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }

  def send2kafka(rdd: RDD[String], brokerList: String, topic: String): Unit = {
    rdd.foreachPartition { rddPartition =>
      val producer = KafkaProducerUtil.createProducer(Map("metadata.broker.list" -> brokerList))
      rddPartition.foreach(data =>
        producer.send(new KeyedMessage(topic, "0", data))
      )
    }
  }

  def calculateTFIDF(sc:SparkContext, article:String): RDD[Vector] ={
    val documents = sc.textFile(article).map(text =>
      ToAnalysis.parse(text).toArray.map(_.toString.split("/")).filter(_.nonEmpty).map(_(0)).toSeq
    )
    //val documents: RDD[Seq[String]] = sc.textFile(article).map(_.split(" ").toSeq)
    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(documents)
    tf.cache()
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)
    tfidf
  }
}
