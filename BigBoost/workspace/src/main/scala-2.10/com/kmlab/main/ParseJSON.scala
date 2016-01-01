package com.kmlab.main

import java.io._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.jsoup._

import scala.util.parsing.json.JSON


/**
 * Created by WeiChen on 2015/12/13.
 */
object ParseJSON {
  def parse(rdd: RDD[String]): RDD[List[List[String]]] = {
    val jsonMap = rdd.map(JSON.parseFull).map(_.get.asInstanceOf[scala.collection.immutable.Map[String, Any]])
    jsonMap.map(m =>
      m.get("words") match {
        case Some(list) =>
          list.asInstanceOf[List[List[String]]]
        case None => List()
      })
  }

  def parse(rdd: RDD[String], filePath: String, outputPath: String): Unit = {
    val sc = rdd.sparkContext
    val sqlContext = new SQLContext(sc)
    val hdfsPath = "hdfs://bigboost-spark:9000/testData/word.json"
    val fileDF = sqlContext.read.format("json").load(hdfsPath)
    //val flattened = fileDF.explode("words", "wordInLine") { c: mutable.WrappedArray[mutable.WrappedArray[String]] => List(c: _*) }
    //val test = fileDF.select("words").map(_.getSeq[org.apache.spark.sql.Row](0))
    fileDF.registerTempTable("JSON")
    val content = sqlContext.sql("SELECT content FROM JSON").collect().map(_.toString()).map(Jsoup.parse(_).text())
    val writer = new PrintWriter(new File(outputPath))
    content.foreach { content =>
      writer.write(content)
    }
    writer.close()
  }


  // gen pmi for each line in each file
  def genPmiFormJSON(rdd: RDD[List[List[String]]]): RDD[((String, String), Double)] = {
    println("pmi ...")
    val sc = rdd.sparkContext
    val tmp = rdd.reduce((x, y) => x ++ y).filterNot(l => l.isEmpty) //28 sec
    val ttmp = sc.parallelize(tmp, 500)
    val tmpp = ttmp.reduce((x, y) => x ++ y) //28 sec
    val fileRDD = sc.parallelize(tmpp, 500).map((_, 1))
    val allTerms = fileRDD.reduceByKey((x, y) => x + y) //23 sec
    val termCountMap = Map(allTerms.collect(): _*)
    val totalWordNum = fileRDD.count().toDouble
    sc.broadcast(termCountMap)
    sc.broadcast(totalWordNum)
    val list = rdd.map(article =>
      article.map(line =>
        line.combinations(2).filterNot(l => l(0) == l(1)).toList.map(l => ((l(0), l(1)), 1))
      )
    ).map(list =>
      list.reduce((x, y) => x ++ y)
      )
    val tupleRDD = sc.parallelize(list.reduce((x, y) => x ++ y), 600) //45 sec
    println("calculating coocurrence...")

    val pairs = tupleRDD.reduceByKey((x, y) => x + y).map(t =>
      ((t._1._1, termCountMap.getOrElse(t._1._1, 1)), (t._1._2, termCountMap.getOrElse(t._1._2, 1)), t._2)
    )

    println("calculating pmi...")
    val pmi = pairs.map { pair =>
      val x = pair._1
      val y = pair._2
      val pXandY = pair._3.toDouble / totalWordNum
      val pX = x._2.toDouble / totalWordNum
      val pY = y._2.toDouble / totalWordNum
      val pmi = Math.log(pXandY / (pX * pY))
      ((x._1, y._1), pmi)
    }

    val cooccurrence = tupleRDD.reduceByKey((x, y) => x + y)
    val cccc = cooccurrence.repartition(2000)
    val coRes = cccc.filter(t => t._2 < 5).sortBy(t => t._2)
    coRes.saveAsTextFile("hdfs://bigboost-spark:9000/coResult/")

    val ss = pmi.repartition(2000).filter(p => p._2 < 9).sortBy(t => t._2, false) //23 min !! wtf!!!
    ss.saveAsTextFile("hdfs://bigboost-spark:9000/pmiResult/")



    //ss.saveAsTextFile("/result.txt")
    ss
  }

  def sortByPmi(rdd: RDD[((String, String), Double)], topN: Int): Array[((String, String), Double)] = {
    rdd.top(topN)(new Ordering[((String, String), Double)]() {
      def compare(x: ((String, String), Double), y: ((String, String), Double)): Int =
        Ordering[Double].compare(x._2, y._2)
    })
  }
}
