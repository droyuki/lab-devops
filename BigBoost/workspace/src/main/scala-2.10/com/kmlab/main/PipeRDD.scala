package com.kmlab.main

import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by WeiChen on 2015/9/27.
  */
object PipeRDD extends CreateSparkContext {
  def main(args: Array[String]) {
    if (args.length != 5) {
      System.err.println("Usage: StreamingTest <checkpointDirectory> <timeframe> <kafka-brokerList> <topic,...,> <scriptPath>")
      System.exit(1)
    }
    val Array(checkpointDirectory, timeframe, kafkaBrokerList, topicList, scriptPath) = args

    def function2CreateContext(AppName: String, checkpointDirectory: String, timeframe: String, brokerList: String, topicList: String): StreamingContext = {
      val ssc = createContext(AppName, checkpointDirectory, timeframe.toLong)
      val kafkaParams = Map("metadata.broker.list" -> brokerList)
      val topics = topicList.split(",").toSet
      val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
      stream.foreachRDD(rdd =>
        pipeData(rdd.map(_._2), scriptPath)
      )
      ssc
    }
    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => {
        function2CreateContext("BigBoost", checkpointDirectory, timeframe, kafkaBrokerList, topicList)
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }

  def pipeData(rdd: RDD[String], scriptPath: String): RDD[String] = {
    val count = rdd.count()
    if (count != 0)
      println("[Input RDD Count]" + count)
    val dataProc = rdd.pipe(scriptPath)
    dataProc.collect().foreach(t => println("[Proc Data]" + t))
    dataProc
  }

}
