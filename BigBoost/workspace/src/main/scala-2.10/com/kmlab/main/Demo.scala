package com.kmlab.main

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by WeiChen on 2015/12/24.
 */
object Demo {
  def main(args: Array[String]): Unit = {
    //parseArgs(args)
    val conf = new SparkConf().setAppName("Demo")
    val sc = new SparkContext(conf)
    println("PMI Analysis")
    val input = sc.textFile("hdfs://bigboost-spark:9000/testData/word.json")
    val rddd = input.repartition(1000)
    val fileRDD = ParseJSON.parse(rddd)
    val pmi = ParseJSON.genPmiFormJSON(fileRDD)
    pmi.take(100).foreach(t => println("(" + t._1._1 + ", " + t._1._2 + ", " + t._2))
    //pmi.saveAsTextFile("hdfs://bigboost-spark:9000/testData/result.json")
//    pmi.collect().foreach { t =>
//      val str = t._1._1 + ", " + t._1._2 + ", " + t._2.toString
//      println(str)
//    }
    sc.stop()
  }
}
