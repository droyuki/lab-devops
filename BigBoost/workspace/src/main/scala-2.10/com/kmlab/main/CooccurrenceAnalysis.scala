package com.kmlab.main

import java.io.File

import org.apache.spark.mllib.feature.{Word2VecModel, Word2Vec}
import org.apache.spark.mllib.rdd.RDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object CooccurrenceAnalysis {
  private var localFilePath: File = new File(".")
  private var dfsDirPath: String = ""
  private val NPARAMS = 2


  private def printUsage(): Unit = {
    val usage: String = "DFS Read-Write Test\n" +
      "\n" +
      "Usage: localFile dfsDir\n" +
      "\n" +
      "localFile - (string) local file to use in test\n" +
      "dfsDir - (string) DFS directory for read/write tests\n"

    println(usage)
  }

  private def parseArgs(args: Array[String]): Unit = {
    if (args.length != NPARAMS) {
      printUsage()
      System.exit(1)
    }
    var i = 0
    localFilePath = new File(args(i))
    if (!localFilePath.exists) {
      System.err.println("Given path (" + args(i) + ") does not exist.\n")
      printUsage()
      System.exit(1)
    }
    if (!localFilePath.isFile) {
      System.err.println("Given path (" + args(i) + ") is not a file.\n")
      printUsage()
      System.exit(1)
    }
    i += 1
    dfsDirPath = args(i)
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

/*
  def someShit(sc:SparkContext): Unit ={
    val sqlContext = new SQLContext(sc)
    println("Reading file from HDFS")
    val dfsFilename = dfsDirPath + "/news_word.txt"
    val fileRDD = sc.textFile(dfsFilename)
    // val fileRDD = sc.textFile("hdfs://bigboost-spark:9000/testData/news_word.txt")
    val input = fileRDD.map(_.split(" "))
    val word2vec = new Word2Vec()
    val model = word2vec.fit(input.map(_.toSeq))

    val allTerms = sc.makeRDD(input.reduce((x, y) => x ++ y))
    val distinctSet = sc.makeRDD(fileRDD.map(_.split(" ").distinct).reduce((x, y) => x ++ y))
    val distinctJoinSet = distinctSet.cartesian(distinctSet).map(data => (data._1, data._2, 0))

    val indexedAllTerms = allTerms.zipWithIndex().map { case (v, k) => (k, v) }
    val indexedDistinctSet = distinctSet.zipWithIndex().map { case (v, k) => (k, v) }
    val indexedDistinctJoinSet = distinctJoinSet.zipWithIndex().map { case (v, k) => (k, v) }
    import sqlContext.implicits._
    val df = distinctJoinSet.toDF("word1", "word2", "count")


    val indexedVectorInModel = distinctSet.flatMap { term =>
      try {
        Some(model.transform(term))
      } catch {
        case e: IllegalStateException =>
          None
      }
    }.zipWithIndex().map { case (v, k) => (k, v) }

    def genConnection() = {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection("jdbc:mysql://bigboost-mysql:3306/DATA", "root", "")
    }
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "")
    df.write.jdbc("jdbc:mysql://bigboost-mysql:3306/DATA", "table1", prop)
    val jdbcRDD = new JdbcRDD(sc, genConnection,
      "SELECT content FROM mysqltest WHERE ID >= ? AND ID <= ?", 1, 100, 3,
      resultSet => resultSet.getString(1)
    ).cache()

    print(jdbcRDD.filter(_.contains("success")).count())


    //dataFrame.registerTempTable("cooccurrence")
    val indexKey = allTerms.zipWithIndex().map { case (k, v) => (v, k) }.cache()
    val boundary = indexKey.count()
    indexKey.foreach { case (index, word) =>
      if (index < boundary) {
        val pair = (word, indexKey.lookup(index + 1).toString())
        println(pair._1 + ", " + pair._2)

      }
    }
  }

*/



