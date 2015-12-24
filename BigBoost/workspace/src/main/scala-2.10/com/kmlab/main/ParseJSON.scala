package com.kmlab.main

import java.io._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.jsoup._

import scala.collection.mutable

/**
 * Created by WeiChen on 2015/12/13.
 */
object ParseJSON {
  def parse(rdd: RDD[String], filePath: String, outputPath: String): Unit = {
    val sc = rdd.sparkContext
    val sqlContext = new SQLContext(sc)
    val hdfsPath = "hdfs://bigboost-spark:9000/testData/word.json"
    val fileDF = sqlContext.read.format("json").load(filePath)
    val flattened = fileDF.explode("words", "wordInLine"){c: mutable.WrappedArray[mutable.WrappedArray[String]] => List(c: _*)}
    val rdds = flattened.select("wordInLine").map(row => row.getAs[Seq[Seq[String]]]("wordInLine"))

    //val test = fileDF.select("words").map(_.getSeq[org.apache.spark.sql.Row](0))

    //fileDF.registerTempTable("JSON")
    val content = sqlContext.sql("SELECT content FROM JSON").collect().map(_.toString()).map(Jsoup.parse(_).text())
    val writer = new PrintWriter(new File(outputPath))
    content.foreach { content =>
      writer.write(content)
    }
    writer.close()
  }
}
