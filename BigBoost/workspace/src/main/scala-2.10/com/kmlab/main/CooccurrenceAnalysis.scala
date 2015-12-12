package com.kmlab.main

import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.mllib.rdd.RDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object CooccurrenceAnalysis {
  private var dfsDirPath: String = ""

  private def printUsage(): Unit = {
    val usage: String = "Co-occurrence Analysis\n" +
      "\n" +
      "Usage: localFile dfsDir\n" +
      "\n" +
      "dfsDir - (string) DFS directory for read/write tests\n"
    println(usage)
  }

  private def parseArgs(args: Array[String]): Unit = {
    if (args.length != 1) {
      printUsage()
      System.exit(1)
    }
    dfsDirPath = args(0)
  }

  def main(args: Array[String]): Unit = {
    parseArgs(args)
    val conf = new SparkConf().setAppName("Co-occurrence Analysis")
    val sc = new SparkContext(conf)
    println("Reading file from HDFS")
    val dfsFilename = dfsDirPath + "/news_word.txt"
    val fileRDD = sc.textFile(dfsFilename)
    val pmi = genPmi(fileRDD)
    val sortedPMI = sortByPmi(pmi,100)
    val result = cooccurrence(fileRDD)
    val sortedRes = sortByCount(cooccurrence(fileRDD), 100)
    val top100 = sortByCount(result, 100)
    val output = dfsDirPath + "/model/"
    val fortest = "hdfs://bigboost-spark:9000/testData/model/"
    genWord2Vec(fileRDD, fortest)
    val m = loadWord2Vec(sc,fortest)
    m.findSynonyms("成長",3)
    sc.stop()
  }

  def loadWord2Vec(sc:SparkContext,path2Model:String) = Word2VecModel.load(sc, path2Model)

  def genWord2Vec(fileRDD:RDD[String] ,output:String): Unit ={
    val input = fileRDD.map(_.split(" "))
    val word2vec = new Word2Vec()
    val model = word2vec.fit(input.map(_.toSeq))
    model.save(fileRDD.sparkContext, output)
  }

  def genPmi(rdd: RDD[String]): RDD[((String, String), Double)] = {
    val sc = rdd.sparkContext
    val fileRDD = sc.parallelize(rdd.map(_.split(" ")).reduce((x, y) => x ++ y)).map((_, 1))
    val allTerms = fileRDD.reduceByKey((x, y) => x + y)
    val termCountMap = Map(allTerms.collect(): _*)
    val totalWordNum = fileRDD.count().toDouble

    val rddFunc = new RDDFunctions(fileRDD)
    val pairs = rddFunc.sliding(2).map(arr =>
      (((arr(0)._1, termCountMap.getOrElse(arr(0)._1, 1)), (arr(1)._1, termCountMap.getOrElse(arr(1)._1, 1))), 1)
    ).reduceByKey((x, y) => x + y)

    sc.broadcast(termCountMap)
    sc.broadcast(totalWordNum)

    //pair: ( ((String, Int),(String, Int)) , Int)
    val pmi = pairs.map { pair =>
      val x = pair._1._1
      val y = pair._1._2
      val pXandY = pair._2.toDouble / totalWordNum
      val pX = x._2.toDouble / totalWordNum
      val pY = y._2.toDouble / totalWordNum
      val pmi = Math.log(pXandY / (pX * pY))
      ((x._1, y._1), pmi)
    }
    pmi
  }

  def cooccurrence(rdd: RDD[String]): RDD[((String, String), Int)] = {
    val sc = rdd.sparkContext
    val fileRDD = sc.makeRDD(rdd.map(_.split(" ")).reduce((x, y) => x ++ y))
    val rddFunc = new RDDFunctions(fileRDD)
    val res = rddFunc.sliding(2).map(arr => (arr(0), arr(1))).map(tuple => (tuple, 1))
    res.reduceByKey((x, y) => x + y)
  }

  def sortByCount(rdd: RDD[((String, String), Int)], topN: Int): Array[((String, String), Int)] = {
    rdd.top(topN)(new Ordering[((String, String), Int)]() {
      def compare(x: ((String, String), Int), y: ((String, String), Int)): Int =
        Ordering[Int].compare(x._2, y._2)
    })
  }

  def sortByPmi(rdd: RDD[((String, String), Double)], topN: Int): Array[((String, String), Double)] = {
    rdd.top(topN)(new Ordering[((String, String), Double)]() {
      def compare(x: ((String, String), Double), y: ((String, String), Double)): Int =
        Ordering[Double].compare(x._2, y._2)
    })
  }

}