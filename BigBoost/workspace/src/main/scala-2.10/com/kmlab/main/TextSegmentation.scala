package com.kmlab.main

import java.util

import com.kmlab.main.ANSJ._
import com.kmlab.utils.KafkaProducerUtil
import kafka.producer.KeyedMessage
import kafka.serializer.StringDecoder
import org.ansj.library.UserDefineLibrary
import org.ansj.splitWord.analysis.ToAnalysis
import org.ansj.util.FilterModifWord
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by WeiChen on 2015/10/7.
  */
object TextSegmentation extends CreateSparkContext {
  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println("Usage: TextSegmentation <checkpointDirectory> <timeframe> <kafka-brokerList> <topic,...,>")
      System.exit(1)
    }
    val Array(checkpointDirectory, timeframe, kafkaBrokerList, topicList) = args

    def function2CreateContext(AppName: String, checkpointDirectory: String, timeframe: String, brokerList: String, topicList: String): StreamingContext = {
      val ssc = createContext(AppName, checkpointDirectory, timeframe.toLong)
      val kafkaParams = Map("metadata.broker.list" -> brokerList)
      val topics = topicList.split(",").toSet
      val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
      stream.foreachRDD(rdd => {
        val raw = rdd.map(_._2)
        val procA = ansj(raw, "NLP")
        val proc = procA.map(tfTopN(_, 30))
          .map { list =>
            list.map { case (term, freq) =>
              //parse word class
              val wordClassMap: Map[String, String] = Map(
                "n" -> "名詞",
                "nr" -> "人名",
                "nr1" -> "漢語姓氏",
                "nr2" -> "漢語名字",
                "nrj" -> "日語人名",
                "nrf" -> "音譯人名",
                "ns" -> "地名",
                "nsf" -> "音譯地名",
                "nt" -> "機構團體名",
                "nz" -> "其它專名",
                "nl" -> "名詞性慣用語",
                "ng" -> "名詞性語素",
                "nw" -> "新詞",
                "t" -> "時間詞",
                "tg" -> "時間詞性語素",
                "s" -> "處所詞",
                "f" -> "方位詞",
                "v" -> "動詞",
                "vd" -> "副動詞",
                "vn" -> "名動詞",
                "vshi" -> "動詞「是」",
                "vyou" -> "動詞「有」",
                "vf" -> "趨向動詞",
                "vx" -> "形式動詞",
                "vi" -> "不及物動詞",
                "vl" -> "動詞性慣用語",
                "vg" -> "動詞性語素",
                "a" -> "形容詞",
                "ad" -> "副形詞",
                "an" -> "名形詞",
                "ag" -> "形容詞性語素",
                "al" -> "形容詞性慣用語",
                "b" -> "區別詞",
                "bl" -> "區別詞性慣用語",
                "z" -> "狀態詞",
                "r" -> "代詞",
                "rr" -> "人稱代詞",
                "rz" -> "指示代詞",
                "rzt" -> "時間指示代詞",
                "rzs" -> "處所指示代詞",
                "rzv" -> "謂詞性指示代詞",
                "ry" -> "疑問代詞",
                "ryt" -> "時間疑問代詞",
                "rys" -> "處所疑問代詞",
                "ryv" -> "謂詞性疑問代詞",
                "rg" -> "代詞性語素",
                "m" -> "數詞",
                "mq" -> "數量詞",
                "q" -> "量詞",
                "qv" -> "動量詞",
                "qt" -> "時量詞",
                "d" -> "副詞",
                "p" -> "介詞",
                "pba" -> "介詞「把」",
                "pbei" -> "介詞「被」",
                "c" -> "連詞",
                "cc" -> "並列連詞",
                "u" -> "助詞",
                "uzhe" -> "著",
                "ule" -> "了,嘍",
                "uguo" -> "過",
                "ude1" -> "的,底",
                "ude2" -> "地",
                "ude3" -> "得",
                "usuo" -> "所",
                "udeng" -> "等,等等,雲雲",
                "uyy" -> "一樣,一般,似的,般",
                "udh" -> "的話",
                "uls" -> "來講,來說,而言,說來",
                "uzhi" -> "之",
                "ulian" -> "連",
                "e" -> "嘆詞",
                "y" -> "語氣詞",
                "o" -> "擬聲詞",
                "h" -> "前綴",
                "k" -> "後綴",
                "x" -> "字符串",
                "xx" -> "非語素字",
                "xu" -> "網址URL",
                "w" -> "標點符號",
                "wkz" -> "左括號",
                "wky" -> "右括號",
                "wyz" -> "左引號",
                "wyy" -> "右引號",
                "wj" -> "句號",
                "ww" -> "問號",
                "wt" -> "驚嘆號",
                "wd" -> "逗號",
                "wf" -> "分號",
                "wn" -> "頓號",
                "wm" -> "冒號",
                "ws" -> "省略號",
                "wp" -> "破折號",
                "wb" -> "百分號千分號",
                "wh" -> "單位符號")
              val wordClass = term.split("/")(1)
              term.replace(wordClass, wordClass + "(" + wordClassMap.getOrElse(wordClass, "Unknown") + ")")
            }
          }.map(_.mkString("\t"))
        val newTerms = combination(procA).map{list => list.mkString("\t")}
        send2kafka(proc, brokerList, "ansj.nlp")
        send2kafka(newTerms, brokerList, "ansj.nlp")
      })
      ssc
    }
    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => {
        function2CreateContext("TextSegmentation", checkpointDirectory, timeframe, kafkaBrokerList, topicList)
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

  def combination(rdd: RDD[List[String]]): RDD[List[String]] = {
    rdd.map { list =>
      val returnList = for {i <- 0 to list.length - 2} yield {
        val term = list(i).split("/")
        val nextTerm = list(i + 1).split("/")
        val newWord = if ((term(1) == "n" && nextTerm(1) == "v")
          || (term(1) == "vf" && nextTerm(1) == "vi")
          || (term(1) == "n" && nextTerm(1) == "n")
          || (term(1) == "vn" && nextTerm(1) == "n")
          || (term(1) == "b" && nextTerm(1).matches("n"))
          || (term(1) == "a" && nextTerm(1).matches("n"))
          || (term(1) == "v" && nextTerm(1).matches("a"))) {
          term(0) + nextTerm(0)
        } else {
          "NULL"
        }
        newWord
      }
      returnList.toList.filterNot(_ == "NULL")
    }
  }

  def calculateTFIDF(sc: SparkContext, article: String): RDD[Vector] = {
    val documents = sc.textFile(article).map { text =>
      FilterModifWord.insertStopWords(util.Arrays.asList("r", "n"))
      FilterModifWord.insertStopNatures("w", null, "ns", "r", "u", "e")
      UserDefineLibrary.loadLibrary(UserDefineLibrary.FOREST, "/opt/BigBoost/Ansj/library/userLibrary.dic")
      val temp = ToAnalysis.parse(text)
      val filter = FilterModifWord.modifResult(temp)
      //只取詞不取詞性
      filter.toArray.map(_.toString.split("/")).filter(_.nonEmpty).map(_ (0)).toSeq
    }
    //val documents: RDD[Seq[String]] = sc.textFile(article).map(_.split(" ").toSeq)
    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(documents)
    tf.cache()
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)
    tfidf
  }

  def tfTopN(article: List[String], topN: Int): List[(String, Double)] = {
    val sum = article.length
    // word count
    article.groupBy(x => x).map { case (word, list) =>
      word -> list.size.toDouble / sum
    }.toList.sortBy(_._2).reverse.take(topN)
  }
}