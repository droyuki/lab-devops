package com.kmlab.main

import java.util

import com.kmlab.utils.KafkaProducerUtil
import com.spreada.utils.chinese.ZHConverter
import kafka.producer.KeyedMessage
import kafka.serializer.StringDecoder
import org.ansj.app.keyword.KeyWordComputer
import org.ansj.dic.LearnTool
import org.ansj.splitWord.analysis.{NlpAnalysis, ToAnalysis}
import org.ansj.util.FilterModifWord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * Created by WeiChen on 2015/10/7.
 */
object TextSegmentation extends SparkContext {
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
        val nlp = ansj(raw)
        PipeRDD.pipeData(nlp, scriptPath)
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

  def toSimplified(rdd: RDD[String]): RDD[String] = {
    val converter = ZHConverter.getInstance(ZHConverter.SIMPLIFIED)
    rdd.map(text => converter.convert(text))
  }

  def topN(rdd: RDD[String], top: Int): RDD[String] = {
    if (rdd.count() != 0)
      println("[Input RDD Count]" + rdd.count())
    val kwc = new KeyWordComputer(top)
    rdd.map { content =>
      val temp = kwc.computeArticleTfidf(content)
      val words = for (i <- Range(0, top)) yield temp.get(i).getName
      words.mkString("\\t")
    }
  }

  def ansj(rdd: RDD[String], method: Any = "NLP"): RDD[String] = method match {
    case "NLP" =>
      if (rdd.count() != 0)
        println("[Input RDD Count]" + rdd.count())
      val procData = rdd.map { x =>
        val learnTool = new LearnTool();
        val temp = NlpAnalysis.parse(x, learnTool)
        //加入停用词
        FilterModifWord.insertStopWords(util.Arrays.asList("r", "n"))
        //加入停用词性
        FilterModifWord.insertStopNatures("w", null, "ns", "r", "u", "e")
        val filter = FilterModifWord.modifResult(temp)
        //此步骤将会只取分词，不附带词性
        val word = for (i <- Range(0, filter.size())) yield filter.get(i).getName
        word.mkString("\\t")
      }
      procData

    case "To" =>
      if (rdd.count() != 0)
        println("[Input RDD Count]" + rdd.count())
      val procData = rdd.map { x =>
        val temp = ToAnalysis.parse(x)
        //加入停用词
        FilterModifWord.insertStopWords(util.Arrays.asList("r", "n"))
        //加入停用词性
        FilterModifWord.insertStopNatures("w", null, "ns", "r", "u", "e")
        val filter = FilterModifWord.modifResult(temp)
        //此步骤将会只取分词，不附带词性
        val word = for (i <- Range(0, filter.size())) yield filter.get(i).getName
        word.mkString("\\t")
      }
      procData

    case n: Int =>
      if (rdd.count() != 0)
        println("[Input RDD Count]" + rdd.count())
      val kwc = new KeyWordComputer(n)
      val procData = rdd.map { content =>
        val temp = kwc.computeArticleTfidf(content)
        val words = for (i <- Range(0, n)) yield temp.get(i).getName
        words.mkString("\\t")
      }
      procData

    case _ =>
      if (rdd.count() != 0)
        println("[Input RDD Count]" + rdd.count())
      val procData = rdd.map { x =>
        val temp = ToAnalysis.parse(x)
        //加入停用词
        FilterModifWord.insertStopWords(util.Arrays.asList("r", "n"))
        //加入停用词性
        FilterModifWord.insertStopNatures("w", null, "ns", "r", "u", "e")
        val filter = FilterModifWord.modifResult(temp)
        //此步骤将会只取分词，不附带词性
        val word = for (i <- Range(0, filter.size())) yield filter.get(i).getName
        word.mkString("\\t")
      }
      procData
  }

  def send2kafka(rdd: RDD[String], brokerList: String, topic: String): Unit = {
    rdd.foreachPartition { rddPartition =>
      val producer = KafkaProducerUtil.createProducer(Map("metadata.broker.list" -> brokerList))
      rddPartition.foreach(data =>
        producer.send(new KeyedMessage(topic, "0", data))
      )
    }
  }

}
